# encoding: UTF-8
require 'zlib'
module BatchJob
  class MultiRecord < Single
    #
    # User definable attributes
    #
    # The following attributes are set when the job is created

    # Whether to store results in a separate collection, or to discard any results
    # returned when records were processed
    key :collect_output,         Boolean, default: false

    # Compress all working data
    # The parameters are not affected in any way, just the data stored in the
    # records and results collections will compressed
    key :compress,                Boolean, default: false

    # Encrypt all working data
    # The parameters are not affected in any way, just the data stored in the
    # records and results collections will be encrypted
    key :encrypt,                 Boolean, default: false

    # When compressing or encrypting multiple lines within a single record the
    # array needs to be converted back to a string. The delimiter used to join
    # and then split apart the array is:
    key :compress_delimiter,      String, default: '|@|'

    # Number of records to include in each block that is processed
    key :block_size,              Integer, default: 100

    #
    # Values that jobs can update during processing
    #

    # Number of records in this job
    key :record_count,            Integer, default: 0

    # Number of blocks that failed to process due to an un-handled exception
    key :failed_blocks,           Integer, default: 0

    after_destroy :cleanup_records

    validates_presence_of :record_count

    # State Machine events and transitions
    #
    # Usual processing:
    #   :queued -> :loading -> :processing -> :finishing -> :completed
    #

    # Use a separate Mongo connection for the Records and Results
    # Allows the records and results to be stored in a separate Mongo database
    # from the Jobs themselves.
    #
    # It is recommended to set the work_connection to a local Mongo Server that
    # is not replicated to another data center to prevent flooding the network
    # with replication of data records and results.
    # The jobs themselves can/should be replicated across data centers so that
    # they are never lost.
    def self.work_connection=(work_connection)
      @@work_connection = work_connection
    end

    # Returns the Mongo connection for the Records and Results
    def self.work_connection
      @@work_connection || self.connection
    end

    # Returns [true|false] whether to collect the results from running this batch
    def collect_output?
      collect_output == true
    end

    # Calls the supplied block for each record available for processing
    # Multiple threads can call this method at the same time
    # to increase concurrency.
    #
    # By default it will keep processing until no more records are left for processing.
    # After each block of records has been processed without raising an exception
    # it is removed from the records queue
    #
    # The result is from the block is written to the output collection if
    # collect_output? is true
    #
    # Returns the number of records processed
    #
    # If an exception was thrown the entire block of records is marked with
    # the exception that occurred and removed from general by increasing its
    # retry count.
    #
    # If the mongo_ha gem has been loaded, then the connection to mongo is
    # automatically re-established and the job will resume anytime a
    # Mongo connection failure occurs.
    #
    # Thread-safe, can be called by multiple threads at the same time
    #
    # Parameters
    #   server_name [String]
    #     A unqiue name for this server instance
    #     Should only have one server name per machine
    #     On startup all pending jobs with this 'server_name' will be retried
    #     TODO Make this a process-wide setting and include PID ( host_name:PID )
    #
    #   on_exception [Proc]
    #     Block of code to execute if an unhandled exception was raised during
    #     the processing of a block
    #
    #   block_number [Integer|Array<Integer>]
    #     Only work on that specific block number(s)
    #
    #   block_count [Integer]
    #     Only process block_count blocks before returning
    #     Default: Keep processing until there are no more blocks available
    #
    def work(server_name, options={}, &proc)
      raise 'Job must be started before calling #work' unless running?
      on_exception    = options.delete(:on_exception)
      block_number    = options.delete(:block_number)
      block_count     = options.delete(:block_count)
      options.each { |option| warn "Ignoring unknown BatchJob::MultiRecord#work option: #{option.inspect}" }

      selector = {
        query:  { 'server' => { '$exists' => false }, 'failed' => { '$exists' => false } },
        update: { '$set' => { server: server_name}, '$currentDate' => { 'started_at' => true }},
        sort:   '_id'
        # full_response: true   returns the entire response object from the server including ‘ok’ and ‘lastErrorObject’.
      }

      # Apply block_number override if applicable
      if block_number
        if block_number.is_a?(Integer)
          selector[:query]['_id'] = block_number
        else
          selector[:query]['_id'] = { '$in' => block_number }
        end
      end

      processed_block_count = 0
      processed_record_count = 0
      # find_and_modify is already in a retry block
      while message = input_collection.find_and_modify(selector)
        begin
          input_block, header = parse_message(message)
          block_id = header['_id']
          output_block = logger.benchmark_info("#work Processed Block #{block_id}", log_exception: :full, on_exception_level: :error) do
            input_block.collect { |record| proc.call(record, header) }
          end
          output_collection.insert(build_message(output_block, '_id' => block_id)) if collect_output?
          # On successful completion remove the record from the job queue
          input_collection.remove('_id' => block_id)
          processed_block_count += 1
          processed_record_count += input_block.size
          break if block_count && (processed_block_count >= block_count)
        rescue Mongo::OperationFailure, Mongo::ConnectionFailure => exc
          # Ignore duplicates since it means the job was restarted
          unless exc.message.include?('E11000')
            logger.fatal "Stopping work due to unhandled Mongo Exception", exc
            raise(exc)
          end
        rescue Exception => exc
          # Increment the failed_blocks by 1
          increment(failed_blocks: 1)
          self.failed_blocks += 1

          # Set failure information and increment retry count
          input_collection.update(
            {'_id' => header['_id']},
            {
              '$unset' => {'server' => true},
              '$set' => {
                'exception' => {
                  'class'       => exc.class.to_s,
                  'message'     => exc.message,
                  'backtrace'   => exc.backtrace || [],
                  'server'      => server_name
                },
                'failure_count' => header['failure_count'].to_i + 1,
                'failed'        => true
              }
            }
          )
          on_exception.call(exc) if on_exception
        end
      end
      # Check if processing is complete
      if record_count && (blocks_queued == 0)
        # Check if another thread / worker already completed the job
        reload
        complete! unless completed?
      end
      processed_record_count
    end

    # Make all failed blocks for this job available for processing again
    # Parameters:
    #   block_numbers [Array<Integer>]
    #     Numbers of the blocks to retry
    #     Default: Retry all blocks for this job
    def retry_failed_blocks(block_numbers=nil)
      selector = {'failed' => { '$exists' => true }}
      # Apply block_number override if applicable
      if block_numbers
        case block_numbers.size
        when 0
          return 0
        when 1
          selector['_id'] = block_numbers.first
        else
          selector['_id'] = { '$in' => block_numbers }
        end
      end

      result = input_collection.update(selector, {'$unset' => { 'failed' => true, 'exception' => true, 'started_at' => true }}, { multi: true })
      count = result['nModified']
      decrement(failed_blocks: count)
      self.failed_blocks -= count
      # In case this job instance does not have the latest count on the server
      self.failed_blocks = 0 if failed_blocks < 0
      count
    end

    # Add a block of records for processing
    # The number of records in the block should match `:block_size`
    #
    # Parameters
    #   `block` [ Array<Hash | Array | String | Integer | Float | Symbol | Regexp | Time> ]
    #     All elements in `array` must be serializable to BSON
    #     For example the following types are not supported: Date
    # Note:
    #   Not thread-safe. Only call from one thread at a time
    def input_block(block)
      input_collection.insert(build_message(block, '_id' => record_count + 1))
      logger.debug { "#input_block Added #{block.size} record(s)" }
      # Only increment record_count once the job has been saved
      self.record_count += block.size
    end

    # Load each record returned by the supplied Block until it returns nil
    #
    # The records are automatically grouped into blocks based on :block_size
    #
    # Returns [Range<Integer>] range of the record_ids that were added
    #
    # Note:
    #   Not thread-safe. Only call from one thread at a time per job instance
    def input_records(&proc)
      before_count = record_count
      block = []
      loop do
        record = proc.call
        break if record.nil?
        block << record
        if block.size % block_size == 0
          input_block(block)
          block = []
        end
      end
      input_block(block) if block.size > 0
      logger.debug { "#input_records Added #{record_count - before_count} record(s)" }
      record_count > before_count ? (before_count + 1 .. record_count) : (before_count .. record_count)
    end

    # Load records for processing from the supplied stream
    # All data read from the stream is converted into UTF-8
    # before being persisted.
    #
    # The entire stream is read until it returns nil
    #
    # Parameters
    #   io [IO]
    #     IO Stream that responds to: :read
    #
    #   options:
    #     delimeter[Symbol|String]
    #       Record delimeter to use to break the stream up
    #         nil
    #           Automatically detect line endings and break up by line
    #         String:
    #           Any string to break the stream up by
    #           The records when saved will not include this delimiter
    #       Default: nil
    #
    #     buffer_size [Integer]
    #       Maximum size of the buffer into which to read the stream into for
    #       processing.
    #       Must be large enough to hold the entire first line and its delimiter(s)
    #       Default: 65536 ( 64K )
    #
    # Notes:
    # * Only use this method for UTF-8 data, for binary data use #input_block or #input_records
    # * Not thread-safe. Only call from one thread at a time per job instance
    # * All data is converted by this method to UTF-8 since that is how strings
    #   are stored in MongoDB
    #
    # Example:
    #   # Load plain text records from a file
    #   File.open(file_name, 'r') do |file|
    #     # Copy input details as parameters to the job
    #     job.parameters['source'] = { name: file_name, type: type, time: file.mtime, size: file.size }
    #     job.input_stream(file)
    #   end
    #
    # Example:
    #   # Load from a Zip file:
    #   BatchJob::Reader::Zip.input_file('myfile.zip') do |io, source|
    #     # Copy input details as parameters to the job
    #      job.parameters['source'] = source.merge!(type: :zip)
    #      job.input_stream(io)
    #    end
    def input_stream(io, options={})
      options = options.dup
      delimiter   = options.delete(:delimiter)
      buffer_size = options.delete(:buffer_size) || 65536
      options.each { |option| warn "Ignoring unknown BatchJob::MultiRecord#add_records option: #{option.inspect}" }

      delimiter.force_encoding(UTF8_ENCODING) if delimiter

      batch_count  = 0
      end_index    = nil
      block        = []
      before_count = record_count
      buffer       = ''
      loop do
        partial = ''
        # TODO Add optional data cleansing to strip out for example non-printable
        # characters before converting to UTF-8
        chunk = io.read(buffer_size)
        unless chunk
          logger.trace { "#input_stream End of stream reached" }
          break
        end
        logger.trace { "#input_stream Read #{chunk.size} bytes" }
        buffer << chunk.force_encoding(UTF8_ENCODING)
        if delimiter.nil?
          # Auto detect text line delimiter
          if buffer =~ /\r\n?|\n/
            delimiter = $&
          elsif buffer.size <= buffer_size
            # Handle one line files that are smaller than the buffer size
            delimiter = "\n"
          else
            # TODO Add custom Exception
            raise "Malformed data. Could not find \\r\\n or \\n within the buffer_size of #{buffer_size}. Read #{buffer.size} bytes from stream"
          end
        end

        # Collect 'block_size' lines and write to mongo as a single record
        buffer.each_line(delimiter) do |line|
          if line.end_with?(delimiter)
            # Strip off delimiter when placing in record array
            block << line[0..(end_index ||= (delimiter.size + 1) * -1)]
            batch_count += 1
            if batch_count >= block_size
              # Write to Mongo
              input_block(block)
              batch_count = 0
              block.clear
            end
          else
            # The last line in the buffer could be incomplete
            logger.trace('#add_text_stream partial data') { line }
            partial = line
          end
        end
        buffer = partial
      end

      # Add last line since it may not have been terminated with the delimiter
      block << buffer if buffer.size > 0

      # Write partial record to Mongo
      input_block(block) if block.size > 0

      logger.debug { "#input_stream Added #{self.record_count - before_count} record(s)" }
      (before_count .. self.record_count)
    end

    # Returns the results of all processing into the supplied stream
    # The results are returned in the order they were originally loaded.
    #   io [IO]
    #     An IO stream to which to write all the results to
    #     Must respond to #write
    #
    #   delimiter [String]
    #     Add the specified delimiter after every record when writing it
    #     to the output stream
    #     Default: OS Specific. Linux: "\n"
    #
    # Notes:
    # * Remember to close the stream after calling #output_stream since
    #   #output_stream does not close the stream after all results have
    #   been written
    def output_stream(io, delimiter=$/)
      output_collection.find({}, sort: '_id', timeout: false) do |cursor|
        cursor.each do |message|
          block, _ = parse_message(message)
          io.write(block.join(delimiter) + delimiter)
        end
      end
      io
    end

    # Iterate over each record
    def each_record(&block)
      input_collection.find({}, sort: '_id', timeout: false) do |cursor|
        cursor.each { |message| block.call(*parse_message(message)) }
      end
    end

    # Iterate over each result
    def each_result(&block)
      output_collection.find({}, sort: '_id', timeout: false) do |cursor|
        cursor.each { |message| block.call(*parse_message(message)) }
      end
    end

    # Returns the Mongo Collection for the records queue name
    def input_collection
      @input_collection ||= self.class.work_connection.db["#{self.class.collection_name}_input_#{id.to_s}"]
    end

    # Returns the Mongo Collection for the records queue name
    def output_collection
      @output_collection ||= self.class.work_connection.db["#{self.class.collection_name}_output_#{id.to_s}"]
    end

    # Returns [Integer] percent of records completed so far
    # Returns nil if the total record count has not yet been set
    def percent_complete
      return 100 if completed?
      return 0 unless record_count > 0
      ((output_collection.count.to_f / record_count) * 100).round
    end

    # Returns [true|false] whether the entire job has been completely processed
    # Useful for determining if the job is complete when in active state
    def processing_complete?
      active? && (record_count.to_i > 0) && (input_collection.count == 0) && (output_collection.count == record_count)
    end

    # Returns [Integer] the number of blocks queued for processing
    def blocks_queued
      input_collection.count
    end

    # Returns [Integer] the number of blocks already processed
    def blocks_processed
      output_collection.count
    end

    # Returns [Hash] status of this job
    def status(time_zone='EST')
      h = super(time_zone)
      case
      when running? || paused?
        processed = blocks_processed
        h[:blocks_queued]    = blocks_queued
        h[:blocks_processed] = processed
        h[:total_records]    = record_count
        h[:records_per_hour] = ((processed * block_size / (Time.now - started_at)) * 60 * 60).round
        h[:percent_complete] = record_count == 0 ? 0 : (((processed.to_f * block_size) / record_count) * 100).to_i
      when completed?
        h[:records_per_hour] = ((record_count / h[:seconds]) * 60 * 60).round
        h[:status]           = "Completed processing #{record_count} record(s) at a rate of #{"%.2f" % h[:records_per_hour]} records per hour at #{completed_at.in_time_zone(time_zone)}"
        h[:total_records]    = record_count
      when queued?
        h[:blocks_queued]    = blocks_queued
      end
      h
    end

    private

    # Returns [Array<String>] The decompressed / un-encrypted data string if applicable
    # All strings within the Array will encoded to UTF-8 for consistency across
    # plain, compressed and encrypted
    def parse_message(message)
      block = message.delete('block')
      if encrypt || compress
        str = if encrypt
          SymmetricEncryption.cipher.binary_decrypt(block.to_s)
        else compress
          Zlib::Inflate.inflate(block.to_s).force_encoding(UTF8_ENCODING)
        end
        # Convert the de-compressed and/or un-encrypted string back into an array
        block = str.split(compress_delimiter)
      end
      [ block, message ]
    end

    # Builds the message to be stored including the supplied block
    # Compresses / Encrypts the block according to the job setting
    def build_message(block, header={})
      data = if encrypt || compress
        # Convert block of records in a single string
        str = block.join(compress_delimiter)
        if encrypt
          # Encrypt to binary without applying an encoding such as Base64
          # Use a random_iv with each encryption for better security
          BSON::Binary.new(SymmetricEncryption.cipher.binary_encrypt(str, true, compress))
        else compress
          BSON::Binary.new(Zlib::Deflate.deflate(str))
        end
      else
        # Without compression or encryption, store the array as is
        block
      end
      header['block'] = data
      header
    end

    # Drop the records collection
    def cleanup_records
      input_collection.drop
      output_collection.drop
    end

  end
end