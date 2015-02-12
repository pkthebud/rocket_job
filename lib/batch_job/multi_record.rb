# encoding: UTF-8
require 'zlib'
module BatchJob
  class MultiRecord < Single
    #
    # User definable attributes
    #
    # The following attributes are set when the job is created

    # Compress all working data
    # The arguments are not affected in any way, just the data stored in the
    # records and results collections will compressed
    key :compress,                Boolean, default: false

    # Encrypt all working data
    # The arguments are not affected in any way, just the data stored in the
    # records and results collections will be encrypted
    key :encrypt,                 Boolean, default: false

    # When compressing or encrypting multiple lines within a single record the
    # array needs to be converted back to a string. The delimiter used to join
    # and then split apart the array is:
    key :compress_delimiter,      String, default: '|@|'

    # Number of records to include in each slice that is processed
    key :slice_size,              Integer, default: 100

    #
    # Values that jobs can update during processing
    #

    # Number of records in this job
    key :record_count,            Integer, default: 0

    # Number of slices that failed to process due to an un-handled exception
    key :failed_slices,           Integer, default: 0

    # Used internally to allow other workers to also work on this job
    # when it is in :running state
    key :parallel,                Boolean, default: false

    after_destroy :cleanup!

    validates_presence_of :record_count, :compress_delimiter,
      :slice_size, :record_count, :failed_slices
    # :compress, :encrypt, :parallel

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

    # Calls the supplied slice for each record available for processing
    # Multiple threads can call this method at the same time
    # to increase concurrency.
    #
    # By default it will keep processing until no more records are left for processing.
    # After each slice of records has been processed without raising an exception
    # it is removed from the records queue
    #
    # The result is from the slice is written to the output collection if
    # collect_output? is true
    #
    # Returns the number of records processed
    #
    # If an exception was thrown the entire slice of records is marked with
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
    #   on_exception [Proc]
    #     Block of code to execute if an unhandled exception was raised during
    #     the processing of a slice
    #
    #   slice_number [Integer|Array<Integer>]
    #     Only work on that specific slice number(s)
    #
    #   slice_count [Integer]
    #     Only process slice_count slices before returning
    #     Default: Keep processing until there are no more slices available
    #
    #   processing_seconds [Integer]
    #     Number of seconds to process work for when multiple records are being
    #     processed.
    #     Default: 0 => No time limit
    #
    def work(options={}, &block)
      raise 'Job must be started before calling #work' unless running?
      slice_number       = options.delete(:slice_number)
      slice_count        = options.delete(:slice_count)
      processing_seconds = options.delete(:processing_seconds) || 0
      options.each { |option| raise ArgumentError.new("Unknown BatchJob::MultiRecord#work option: #{option.inspect}") }

      selector = {
        query:  { 'server' => { '$exists' => false }, 'failed' => { '$exists' => false } },
        update: { '$set' => { server: Server.name }, '$currentDate' => { 'started_at' => true }},
        sort:   '_id'
      }

      logger.tagged(self.id.to_s) do
        # Apply slice_number override if applicable
        if slice_number
          if slice_number.is_a?(Integer)
            selector[:query]['_id'] = slice_number
          else
            selector[:query]['_id'] = { '$in' => slice_number }
          end
        end

        start_time = Time.now if processing_seconds > 0
        processed_slice_count  = 0
        processed_record_count = 0
        worker                 = self.klass.constantize.new
        worker.batch_job       = self
        if block.nil? && self.parallel.blank?
          self.parallel = true
          # before_perform
          call_method(worker, :before)
          # Save `parallel` to allow other workers to start processing this job
          # and to save any other changes made in the before_perform
          save!
        end

        # find_and_modify is already in a retry slice
        while message = input_collection.find_and_modify(selector)
          input_slice, header = parse_message(message)
          process_slice(worker, input_slice, header, &block)
          processed_slice_count += 1
          processed_record_count += input_slice.size
          # If number of blocks to process has been exceeded
          break if slice_count && (processed_slice_count >= slice_count)
          # If processing time is exceeded
          break if processing_seconds > 0 && ((Time.now - start_time) >= processing_seconds)
          break if Server.shutting_down?
        end
        # Check if processing is complete
        if block.nil? && self.record_count && (self.slices_queued == 0)
          # Check if another thread / worker already completed the job
          reload
          if aborted?
            cleanup!
          elsif running?
            # after_perform
            call_method(worker, :after)
            complete!
          end
        end
        processed_record_count
      end
    end

    # Make all failed slices for this job available for processing again
    # Parameters:
    #   slice_numbers [Array<Integer>]
    #     Numbers of the slices to retry
    #     Default: Retry all slices for this job
    def retry_failed_slices(slice_numbers=nil)
      selector = {'failed' => { '$exists' => true }}
      # Apply slice_number override if applicable
      if slice_numbers
        case slice_numbers.size
        when 0
          return 0
        when 1
          selector['_id'] = slice_numbers.first
        else
          selector['_id'] = { '$in' => slice_numbers }
        end
      end

      result = input_collection.update(selector, {'$unset' => { 'failed' => true, 'exception' => true, 'started_at' => true }}, { multi: true })
      count = result['nModified']
      decrement(failed_slices: count)
      self.failed_slices -= count
      # In case this job instance does not have the latest count on the server
      self.failed_slices = 0 if failed_slices < 0
      count
    end

    # Add a slice of records for processing
    # The number of records in the slice should match `:slice_size`
    #
    # Parameters
    #   `slice` [ Array<Hash | Array | String | Integer | Float | Symbol | Regexp | Time> ]
    #     All elements in `array` must be serializable to BSON
    #     For example the following types are not supported: Date
    # Note:
    #   Not thread-safe. Only call from one thread at a time
    def input_slice(slice)
      input_collection.insert(build_message(slice, '_id' => record_count + 1))
      logger.debug { "#input_slice Added #{slice.size} record(s)" }
      # Only increment record_count once the job has been saved
      self.record_count += slice.size
    end

    # Load each record returned by the supplied Block until it returns nil
    #
    # The records are automatically grouped into slices based on :slice_size
    #
    # Returns [Range<Integer>] range of the record_ids that were added
    #
    # Note:
    #   Not thread-safe. Only call from one thread at a time per job instance
    def input_records(&block)
      before_count = record_count
      slice = []
      loop do
        record = block.call
        break if record.nil?
        slice << record
        if slice.size % slice_size == 0
          input_slice(slice)
          slice = []
        end
      end
      input_slice(slice) if slice.size > 0
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
    # * Only use this method for UTF-8 data, for binary data use #input_slice or #input_records
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
      options     = options.dup
      delimiter   = options.delete(:delimiter)
      buffer_size = options.delete(:buffer_size) || 65536
      options.each { |option| raise ArgumentError.new("Unknown BatchJob::MultiRecord#add_records option: #{option.inspect}") }

      delimiter.force_encoding(UTF8_ENCODING) if delimiter

      batch_count  = 0
      end_index    = nil
      slice        = []
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

        # Collect 'slice_size' lines and write to mongo as a single record
        buffer.each_line(delimiter) do |line|
          if line.end_with?(delimiter)
            # Strip off delimiter when placing in record array
            slice << line[0..(end_index ||= (delimiter.size + 1) * -1)]
            batch_count += 1
            if batch_count >= slice_size
              # Write to Mongo
              input_slice(slice)
              batch_count = 0
              slice.clear
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
      slice << buffer if buffer.size > 0

      # Write partial record to Mongo
      input_slice(slice) if slice.size > 0

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
          slice, _ = parse_message(message)
          io.write(slice.join(delimiter) + delimiter)
        end
      end
      io
    end

    # Iterate over each input slice
    def each_input_slice(&block)
      input_collection.find({}, sort: '_id', timeout: false) do |cursor|
        cursor.each { |message| block.call(*parse_message(message)) }
      end
    end

    # Iterate over each output slice
    def each_output_slice(&block)
      output_collection.find({}, sort: '_id', timeout: false) do |cursor|
        cursor.each { |message| block.call(*parse_message(message)) }
      end
    end

    # Iterate over each failed record, if any
    def each_failed_record(&block)
      input_collection.find({'failed' => { '$exists' => true }}, sort: '_id', timeout: false) do |cursor|
        cursor.each do |message|
          slice, header = *parse_message(message)
          exception = header['exception']
          if exception && (record_number = exception['record_number'])
            block.call(slice[record_number - 1], header)
          end
        end
      end
    end

    # Returns the Mongo Collection for the records queue name
    def input_collection
      @input_collection ||= self.class.work_connection.db["batch_job.inputs.#{id.to_s}"]
    end

    # Returns the Mongo Collection for the records queue name
    def output_collection
      @output_collection ||= self.class.work_connection.db["batch_job.outputs.#{id.to_s}"]
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

    # Returns [Integer] the number of slices queued for processing
    def slices_queued
      input_collection.count
    end

    # Returns [Integer] the number of slices already processed
    def slices_processed
      output_collection.count
    end

    # Returns [Hash] status of this job
    def status(time_zone='EST')
      h = super(time_zone)
      case
      when running? || paused?
        processed = slices_processed
        h[:slices_queued]    = slices_queued
        h[:slices_processed] = processed
        h[:total_records]    = record_count
        h[:records_per_hour] = ((processed * slice_size / (Time.now - started_at)) * 60 * 60).round
        h[:percent_complete] = record_count == 0 ? 0 : (((processed.to_f * slice_size) / record_count) * 100).to_i
      when completed?
        h[:records_per_hour] = ((record_count / h[:seconds]) * 60 * 60).round
        h[:status]           = "Completed processing #{record_count} record(s) at a rate of #{"%.2f" % h[:records_per_hour]} records per hour at #{completed_at.in_time_zone(time_zone)}"
        h[:total_records]    = record_count
      when queued?
        h[:slices_queued]    = slices_queued
      end
      h
    end

    # Drop the input and output collections
    def cleanup!
      input_collection.drop
      output_collection.drop
    end

    protected

    # Process a single message from Mongo
    # A message consists of a header and the slice of records to process
    # If the message is successfully processed it will be removed from the input collection
    def process_slice(worker, input_slice, header, &block)
      slice_id            = header['_id']
      record_number       = 0
      logger.tagged(slice_id) do
        output_slice = logger.benchmark_info(
          "#{worker.class.name}##{self.method}, slice:#{slice_id}",
          metric:             "batch_job/#{worker.class.name.underscore}/#{self.method}",
          log_exception:      :full,
          on_exception_level: :error,
          silence:            self.log_level
        ) do
          input_slice.collect do |record|
            record_number += 1
            # TODO Skip previously processed records if this is a retry
            if block
              block.call(*self.arguments, record, header)
            else
              # perform
              worker.send(self.method, *self.arguments, record, header)
            end
          end
        end

        # Ignore duplicates on insert into output_collection since it successfully completed previously
        begin
          output_collection.insert(build_message(output_slice, '_id' => slice_id)) if self.collect_output?
        rescue Mongo::OperationFailure, Mongo::ConnectionFailure => exc
          # Ignore duplicates since it means the job was restarted
          unless exc.message.include?('E11000')
            logger.fatal "Stopping work due to unhandled Mongo Exception", exc
            raise(exc)
          end
        end

        # On successful completion remove the record from the job queue
        input_collection.remove('_id' => slice_id)
      end
    rescue Exception => exc
      worker.on_exception(exc) if worker && worker.respond_to?(:on_exception)
      set_exception(header, exc, record_number)
    end

    # Returns [Array<String>, <Hash>] The decompressed / un-encrypted data string if applicable
    # All strings within the Array will encoded to UTF-8 for consistency across
    # plain, compressed and encrypted
    def parse_message(message)
      slice = message.delete('slice')
      if encrypt || compress
        str = if encrypt
          SymmetricEncryption.cipher.binary_decrypt(slice.to_s)
        else compress
          Zlib::Inflate.inflate(slice.to_s).force_encoding(UTF8_ENCODING)
        end
        # Convert the de-compressed and/or un-encrypted string back into an array
        slice = str.split(compress_delimiter)
      end
      [ slice, message ]
    end

    # Builds the message to be stored including the supplied slice
    # Compresses / Encrypts the slice according to the job setting
    def build_message(slice, header={})
      data = if encrypt || compress
        # Convert slice of records in a single string
        str = slice.join(compress_delimiter)
        if encrypt
          # Encrypt to binary without applying an encoding such as Base64
          # Use a random_iv with each encryption for better security
          BSON::Binary.new(SymmetricEncryption.cipher.binary_encrypt(str, true, compress))
        else compress
          BSON::Binary.new(Zlib::Deflate.deflate(str))
        end
      else
        # Without compression or encryption, store the array as is
        slice
      end
      header['slice'] = data
      header
    end

    # Set exception information for a specific slice
    def set_exception(header, exc, record_number)
      # Increment the failed_slices by 1
      increment(failed_slices: 1)
      self.failed_slices += 1

      # Set failure information and increment retry count
      input_collection.update(
        { '_id' => header['_id'] },
        {
          '$unset' => { 'server' => true },
          '$set' => {
            'exception' => {
              'class'         => exc.class.to_s,
              'message'       => exc.message,
              'backtrace'     => exc.backtrace || [],
              'server'        => Server.name,
              'record_number' => record_number
            },
            'failure_count' => header['failure_count'].to_i + 1,
            'failed'        => true
          }
        }
      )
    end

  end
end
