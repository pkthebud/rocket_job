# encoding: UTF-8
require 'zlib'
module BatchJob
  #
  # Multi-record jobs
  #
  # When jobs consists of multiple records that will be held in a separate
  # collection for processing
  class MultiRecordJob < Job
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

    # Returns each record available in the order it was added
    # until no more records are left for processing.
    # After each item has been processed without raisin an exception it is deleted
    # from the records queue
    # The result if not nil is written to the results collection
    #
    # If an exception was thrown, ...
    #
    # If a Mongo connection failure occurs the connection is automatically re-established
    # and the job will be retried ( possibly by another process )
    #
    # Thread-safe, can be called by multiple threads at the same time
    #
    # Parameters
    #   server_name
    #     A unqiue name for this server instance
    #     Should only have one server name per machine
    #     On startup all pending jobs with this 'server_name' will be retried
    def process_records(server_name, include_retries=false, &block)
      # find_and_modify is already in a retry block
      while record = records_collection.find_and_modify(
          query:  { 'server' => { :$exists => false }, retry_count: { :$exists => include_retries } },
          update: { server: server_name, started_at: Time.now },
          sort:   '_id'
          # full_response: true   returns the entire response object from the server including ‘ok’ and ‘lastErrorObject’.
        )
        begin
          result = block.call(record.delete('data'), record)
          results_collection.insert('_id' => record['_id'], 'data' => result) unless result.nil? && collect_results?
          # On successful completion delete the record from the job queue
          records_collection.remove('_id' => record['_id'])
        rescue Mongo::OperationFailure, Mongo::ConnectionFailure => exc
          logger.fatal "Going down due to unhandled Mongo Exception", exc
          raise
        rescue Exception => exc
          logger.error "Failed to process job", exc
          # Set failure information and increment retry count
          records_collection.update(
            { '_id' => record['_id'] },
            'server_name' => nil,
            'exception' => {
              'class'       => exc.class.to_s,
              'message'     => exc.message,
              'backtrace'   => exc.backtrace || [],
              'server_name' => server_name
            },
            'retry_count' => ( record['retry_count'] || 0 ) + 1
          )
        end
      end
    end

    # Returns the record id for the added record
    # Parameters
    #   `data` [ Hash | Array | String | Integer | Float | Symbol | Regexp | Time ]
    #     All elements in `data` must be serializable to BSON
    #     For example the following types are not supported: Date
    # Note:
    #   Not thread-safe. Only call from one thread at a time
    def <<(data)
      records_collection.insert('_id' => record_count + 1, 'data' => data)
      # Only increment record_count once the job has been saved
      self.record_count += 1
    end

    # Add many records for processing at the same time.
    # Returns [Range<Integer>] range of the record_ids that were added
    #
    # Note:
    #   Not thread-safe. Only call from one thread at a time per job instance
    def add_records(records)
      count = record_count
      bulk  = records_collection.initialize_ordered_bulk_op
      records.each { |data| bulk.insert('_id' => (count += 1), 'data' => data) }
      bulk.execute

      result = (self.record_count + 1 .. count)
      self.record_count = count
      result
    end

    # Add records for processing by reading the supplied text stream
    #
    # Parameters
    #   io_stream [IO]
    #     IO Stream that responds to: :read, :eof?
    #
    #   options:
    #     block_size
    #       Number of lines to include in each record
    #       Default: 10
    #
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
    def add_text_stream(io_stream, options={})
      options = options.dup
      block_size  = options.delete(:block_size) || 10
      delimiter   = options.delete(:delimiter)
      buffer_size = options.delete(:buffer_size) || 65536
      options.each { |option| warn "Ignoring unknown BatchJob::MultiRecordJob#add_records option: #{option.inspect}" }

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
        buffer << io_stream.read(buffer_size).force_encoding(UTF8_ENCODING)
        if delimiter.nil?
          # Auto detect text line delimiter
          if buffer =~ /"\r\n?|\n"/
            delimiter = $&
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
          else
            # The last line in the buffer could be incomplete
            partial = line
          end
          batch_count += 1
          if batch_count >= block_size
            # Write to Mongo
            records_collection.insert('_id' => (count += 1), 'data' => record)
            # TODO Compression and encryption - Delimiter?
            # TODO Use Mongo bulk insert API, based on size
            batch_count = 0
            record.clear
          end
        end
        buffer = partial
        break if io_stream.eof?
      end

      # Add last line since it may not have been terminated with the delimiter
      record << partial if partial.size > 0

      # Write partial record to Mongo
      records_collection.insert('_id' => (count += 1), 'data' => record) if record.size > 0

      result = (self.record_count + 1 .. count)
      self.record_count = count
      result
    end

    # Iterate over each record
    def each_record(&block)
      records_collection.find({}, sort: '_id', timeout: false) do |cursor|
        cursor.each { |record| block.call(extract_data(record), record) }
      end
    end

    # Iterate over each result
    def each_result(&block)
      results_collection.find({}, sort: '_id', timeout: false) do |cursor|
        cursor.each { |record| block.call(extract_data(record), record) }
      end
    end

    # Write the results to the supplied stream
    #   stream [IO]
    #     An IO stream to which to write all the results to
    #     Must respond to :write
    #
    # Notes:
    # * Remember to close the stream after calling #write_results since
    #   #write_results does not close the stream after all results have
    #   been written
    def write_results(stream)
      results_collection.find({}, sort: '_id', timeout: false) do |cursor|
        cursor.each { |record| stream.write(extract_data(record, false)) }
      end
    end

    # Returns the Mongo Collection for the records queue name
    def records_collection
      @records_collection ||= self.class.work_connection.db["batch_job_records_#{id.to_s}"]
    end

    # Returns the Mongo Collection for the records queue name
    def results_collection
      @results_collection ||= self.class.work_connection.db["batch_job_results_#{id.to_s}"]
    end

    # Returns [Integer] percent of records completed so far
    # Returns nil if the total record count has not yet been set
    def percent_complete
      return 100 if completed?
      return 0 unless record_count > 0
      ((results_collection.count.to_f / record_count) * 100).round
    end

    # Returns [true|false] whether the entire job has been completely processed
    # Useful for determining if the job is complete when in active state
    def processing_complete?
      active? && (record_count.to_i > 0) && (records_collection.count == 0) && (results_collection.count == record_count)
    end

    # Returns [Hash] status of this job
    def status(time_zone='EST')
      h = super(time_zone)
      case
      when running? || paused?
        h[:queued]           = records_collection.count,
          h[:processed]        = results_collection.count
        h[:record_count]     = record_count
        h[:rate_per_hour]    = ((results_collection.count / (Time.now - started_at)) * 60 * 60).round
        h[:percent_complete] = ((results_collection.count.to_f / record_count) * 100).to_i
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
    def extract_data(record, destructive=true)
      data = destructive ? record.delete('data') : record['data']
      if encrypt || compress
        str = if encrypt
          SymmetricEncryption.binary_decrypt(data.to_s)
        else compress
          Zlib::Inflate.inflate(data.to_s).force_encoding(UTF8_ENCODING)
        end
        # Convert the de-compressed and/or un-encrypted string back into an array
        data = str.split(compress_delimiter)
      end
      data
    end

    # Compresses / Encrypts the data according to the job setting
    def compress_data(array)
      if encrypt || compress
        str = array.join(compress_delimiter)
        if encrypt
          # Encrypt to binary without applying an encoding such as Base64
          # Use a random_iv with each encryption for better security
          BSON::Binary.new(SymmetricEncryption.cipher.binary_encrypt(str, true, compress))
        else compress
          BSON::Binary.new(Zlib::Deflate.deflate(str))
        end
      else
        # Without compression or encryption, store the array as is in Mongo
        array
      end
    end

    # Drop the records collection
    def cleanup_records
      records_collection.drop
      results_collection.drop
    end

  end
end
