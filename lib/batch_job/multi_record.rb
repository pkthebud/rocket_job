# encoding: UTF-8
require 'zlib'
module BatchJob
  class MultiRecord < Simple
    # Number of records in this job
    #   Useful when updating the UI progress bar
    key :record_count,            Integer, default: 0

    # Whether to store results in a separate collection, or to discard any results
    # returned when records were processed
    key :collect_results,         Boolean, default: false

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
    def collect_results?
      collect_results == true
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
    # collect_results? is true
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
    #   include_retries [true|false]
    #     Whether to include blocks of records that we tried previously and
    #     failed
    #     Default: false
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
    def work(server_name, options={}, &block)
      include_retries = options.delete(:include_retries)
      include_retries = false if include_retries.nil?
      on_exception    = options.delete(:on_exception)
      block_number    = options.delete(:block_number)
      block_count     = options.delete(:block_count)
      options.each { |option| warn "Ignoring unknown BatchJob::MultiRecord#work option: #{option.inspect}" }

      selector = {
        query:  { 'server' => { '$exists' => false }, retry_count: { '$exists' => include_retries } },
        update: { server: server_name, started_at: Time.now },
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

      block_count = 0
      record_count = 0
      # find_and_modify is already in a retry block
      while block = input_collection.find_and_modify(selector)
        begin
          block, header = parse_message(block)
          output_block = block.collect { |record| block.call(record, header) }
          output_collection.insert(build_message(output_block, '_id' => block['_id'])) if collect_results?
          # On successful completion remove the record from the job queue
          input_collection.remove('_id' => block['_id'])
          block_count += 1
          break if block_count && (block_count >= block_count)
        rescue Mongo::OperationFailure, Mongo::ConnectionFailure => exc
          # Ignore duplicates since it means the job was restarted
          unless exc.message.include?('E11000')
            logger.fatal "Stopping work due to unhandled Mongo Exception", exc
            raise(exc)
          end
        rescue Exception => exc
          logger.error "Failed to process job", exc
          # Set failure information and increment retry count
          input_collection.update(
            { '_id' => block['_id'] },
            'server_name' => nil,
            'exception' => {
              'class'       => exc.class.to_s,
              'message'     => exc.message,
              'backtrace'   => exc.backtrace || [],
              'server_name' => server_name
            },
            'retry_count' => ( block['retry_count'] || 0 ) + 1
          )
          on_exception.call(exc) if on_exception
        end
      end
      record_count
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
    def <<(block)
      input_collection.insert(build_message(block, '_id' => record_count + 1))
      logger.trace('#<< Add record') { data }
      # Only increment record_count once the job has been saved
      self.record_count += 1
    end

    # Load each record returned by the supplied Block until it returns nil
    #
    # The records are automatically grouped into blocks based on :block_size
    #
    # Returns [Range<Integer>] range of the record_ids that were added
    #
    # Note:
    #   Not thread-safe. Only call from one thread at a time per job instance
    def load_records(&proc)
      before_count = record_count
      block = []
      loop do
        record = proc.call
        break if record.nil?
        block << record
        if block.size % block_size == 0
          self << block
          block = []
        end
      end
      self << block if block.size > 0
      record_count > before_count ? (before_count + 1 .. record_count) : (before_count .. record_count)
    end

    # Loads the specified file as blocks of records for processing
    def load_file(file_name, type=:text)
      case type.to_sym
      when :text
        begin
          file = File.new(file_name, 'r')
          parameters['source'] = { name: file_name, type: type, time: file.mtime, size: file.size }
          load_stream(file)
        ensure
          file.close if file
        end
      when :zip
        BatchJob::Reader::Zip.load_file(file_name) do |io_stream, source|
          parameters['source'] = source.merge!(type: type)
          load_stream(io_stream)
        end
      else
        # TODO Custom exception
        raise "Unknown file type: #{type.inspect}"
      end
    end

    # Load records for processing from the supplied stream
    # All data read from the stream is converted into UTF-8
    # before being persisted.
    #
    # The entire stream is read until it returns nil
    #
    # Parameters
    #   io_stream [IO]
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
    # * Only use this method for UTF-8 data, for binary data use #<< or #add_records
    # * Not thread-safe. Only call from one thread at a time per job instance
    # * All data is converted by this method to UTF-8 since that is how strings
    #   are stored in MongoDB
    def load_stream(io_stream, options={})
      options = options.dup
      delimiter   = options.delete(:delimiter)
      buffer_size = options.delete(:buffer_size) || 65536
      options.each { |option| warn "Ignoring unknown BatchJob::MultiRecord#add_records option: #{option.inspect}" }

      delimiter.force_encoding(UTF8_ENCODING) if delimiter

      batch_count = 0
      end_index   = nil
      record      = []
      count       = record_count
      buffer      = ''
      loop do
        partial = ''
        # TODO Add optional data cleansing to strip out for example non-printable
        # characters before converting to UTF-8
        block = io_stream.read(buffer_size)
        logger.trace('#add_text_stream read from input stream:') { block.inspect }
        break unless block
        buffer << block.force_encoding(UTF8_ENCODING)
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
            record << line[0..(end_index ||= (delimiter.size + 1) * -1)]
            batch_count += 1
            if batch_count >= block_size
              # Write to Mongo
              self << record
              batch_count = 0
              record.clear
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
      record << buffer if buffer.size > 0

      # Write partial record to Mongo
      self << record if record.size > 0

      logger.debug { "#add_text_stream Added #{count - self.record_count} records" }
      (count .. self.record_count)
    end

    # Returns the results of all processing into the supplied stream
    # The results are returned in the order they were originally loaded.
    #   stream [IO]
    #     An IO stream to which to write all the results to
    #     Must respond to :write
    #
    #   delimiter [String]
    #     Add the specified delimiter after every record when writing it
    #     to the output stream
    #     Default: OS Specific. Linux: "\n"
    #
    # Notes:
    # * Remember to close the stream after calling #write_results since
    #   #write_results does not close the stream after all results have
    #   been written
    def unload(stream, delimiter=$/)
      output_collection.find({}, sort: '_id', timeout: false) do |cursor|
        cursor.each do |message|
          block, _ = parse_message(message)
          stream.write(block.join(delimiter) + delimiter)
        end
      end
      stream
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
      @input_collection ||= self.class.work_connection.db["batch_job_records_#{id.to_s}"]
    end

    # Returns the Mongo Collection for the records queue name
    def output_collection
      @output_collection ||= self.class.work_connection.db["batch_job_results_#{id.to_s}"]
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

    # Returns [Hash] status of this job
    def status(time_zone='EST')
      h = super(time_zone)
      case
      when running? || paused?
        h[:queued]           = input_collection.count,
          h[:processed]        = output_collection.count
        h[:record_count]     = record_count
        h[:rate_per_hour]    = ((output_collection.count / (Time.now - started_at)) * 60 * 60).round
        h[:percent_complete] = ((output_collection.count.to_f / record_count) * 100).to_i
      when completed?
        h[:rate_per_hour]    = ((record_count / h[:seconds]) * 60 * 60).round
        h[:status]           = "Completed processing #{record_count} record(s) at a rate of #{"%.2f" % h[:rate_per_hour]} records per hour at #{completed_at.in_time_zone(time_zone)}"
        h[:processed]        = record_count
        h[:record_count]     = record_count
      end
      h
    end

    private

    UTF8_ENCODING = Encoding.find("UTF-8").freeze

    # Returns [Array<String>] The decompressed / un-encrypted data string if applicable
    # All strings within the Array will encoded to UTF-8 for consistency across
    # plain, compressed and encrypted
    def parse_message(message)
      block = message.delete('data')
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
      header['data'] = data
      header
    end

    # Drop the records collection
    def cleanup_records
      input_collection.drop
      output_collection.drop
    end

  end
end
