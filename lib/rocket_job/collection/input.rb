# encoding: UTF-8
require 'rocket_job/collection/base'
module RocketJob
  module Collection
    class Input < Base
      # Input Collection for this job
      # Parameters
      #   job [RocketJob::Job]
      #     The job with which this input collection is associated
      #
      #   name [String]
      #     The named input source when multiple inputs are being processed
      #     Default: None ( Uses the single default input collection for this job )
      def initialize(job, name=nil)
        collection_name = "rocket_job.inputs.#{job.id.to_s}"
        collection_name << ".#{name}" if name
        super(job, collection_name)
        # Index for find_and_modify
        collection.ensure_index('failed' => Mongo::ASCENDING, 'server_name' => Mongo::ASCENDING, '_id' => Mongo::ASCENDING)
      end

      # Load records for processing from the supplied filename or stream into this job.
      # All data read from the file/stream is converted into UTF-8
      # before being persisted.
      #
      # Returns [Integer] the number of records loaded into the job
      #
      # Parameters
      #   file_name_or_io [String | IO]
      #     Full path and file name to stream into the job,
      #     Or, an IO Stream that responds to: :read
      #
      #   options:
      #     format [Symbol]
      #       :text
      #         Text file
      #       :gzip
      #         GZip file
      #       :zip
      #         Zip file
      #       :auto
      #         Auto-detect. If file_name ends with '.zip' then zip is assumed
      #         Note: Only applicable when a file_name of type String is being supplied
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
      # Note:
      #   - When zip format, the Zip file/stream must contain only one file, the first file found will be
      #     loaded into the job
      #   - If an io stream is supplied, it is read until it returns nil
      #
      # Notes:
      # * Only use this method for UTF-8 data, for binary data use #input_slice or #input_records
      # * Not thread-safe. Only call from one thread at a time per job instance
      # * All data is converted by this method to UTF-8 since that is how strings
      #   are stored in MongoDB
      #
      # Example:
      #   # Load plain text records from a file
      #   job.input.upload('hello.csv')
      #
      # Example:
      #   # Load from a Zip file:
      #   job.input.upload('hello.zip')
      def upload(file_name_or_io, options={})
        is_file_name = file_name_or_io.is_a?(String)
        options      = options.dup
        format       = options.delete(:format) || :auto

        if format == :auto
          raise ArgumentError.new("RocketJob Cannot use format :auto when uploading a stream") unless is_file_name
          extension = File.extname(file_name_or_io).downcase
          format    = extension == '.zip' ? :zip : :text
          raise 'Excel Spreadsheets are NOT supported' if ['.xls', '.xlsx'].include?(extension)
        end

        # Common reader to read data from the supplied stream into this collection
        reader = -> io do
          upload_stream(io, options)
        end

        # Upload the file into Mongo
        case format
        when :zip
          if is_file_name
            RocketJob::Reader::Zip.read_file(file_name_or_io) {|io, data|  upload_stream(io, options) }
          else
            RocketJob::Reader::Zip.read_stream(file_name_or_io) {|io, data|  upload_stream(io, options) }
          end
        when :gzip
          if is_file_name
            Zlib::GzipReader.open(file_name_or_io, &reader)
          else
            begin
              io = Zlib::GzipReader.new(file_name_or_io)
              reader.call(io)
            ensure
              io.close if io
            end
          end
        when :text
          if is_file_name
            File.open(file_name_or_io, 'rt', &reader)
          else
            reader.call(file_name_or_io)
          end
        else
          raise ArgumentError.new("Invalid RocketJob upload format: #{format.inspect}")
        end
      end

      # Upload the supplied slices for processing by workers
      #
      # Returns [Integer] the number of records uploaded
      #
      # Parameters
      #   `slice` [ Array<Hash | Array | String | Integer | Float | Symbol | Regexp | Time> ]
      #     All elements in `array` must be serializable to BSON
      #     For example the following types are not supported: Date
      #
      # Note: The caller should honor `:slice_size`, the entire slice is loaded as-is.
      def upload_slice(slice)
        collection.insert(build_message(slice))
        return slice.size
      end

      # Upload each record returned by the supplied Block until it returns nil
      # The records are automatically grouped into slices based on :slice_size
      #
      # Returns [Integer] the number of records uploaded
      #
      # Note:
      #   The Block must return types that can be serialized to BSON.
      #   Valid Types: Hash | Array | String | Integer | Float | Symbol | Regexp | Time
      #   Invalid: Date, etc.
      def upload_records(&block)
        record_count = 0
        slice = []
        loop do
          record = block.call
          break if record.nil?
          slice << record
          if slice.size % slice_size == 0
            record_count += upload_slice(slice)
            slice = []
          end
        end
        record_count += upload_slice(slice) if slice.size > 0
        record_count
      end

      # Returns [Integer] the number of slices currently being processed
      def active_slices
        collection.count(query: {'failed' => { '$exists' => false }, 'server_name' => { '$exists' => true } })
      end

      # Returns [Integer] the number of slices that have failed so far for this job
      # Call #requeue to re-queue the failed jobs for re-processing
      def failed_slices
        collection.count(query: {'failed' => { '$exists' => true } })
      end

      # Returns [Integer] the number of slices queued for processing excluding
      # failed slices
      def queued_slices
        collection.count(query: { 'failed' => { '$exists' => false }, 'server_name' => { '$exists' => false } })
      end

      # Removes the specified slice from the input collection
      def remove_slice(slice_id)
        collection.remove('_id' => slice_id)
      end

      # Iterate over each failed record, if any
      # Since each slice can only contain 1 failed record, only the failed
      # record is returned along with the header containing the exception
      # details
      def each_failed_record(&block)
        each_slice({'failed' => { '$exists' => true }}) do |slice, header|
          exception = header['exception']
          if exception && (record_number = exception['record_number'])
            block.call(slice[record_number - 1], header)
          end
        end
      end

      # Returns [Integer] the number of records processed
      # Invokes the supplied block passing in the slice and the header
      # for every slice found
      def process_slices(server, &block)
        start_time = Time.now
        count      = 0
        selector = {
          query:  { 'server_name' => { '$exists' => false }, 'failed' => { '$exists' => false } },
          update: { '$set' => { server: server.name, 'started_at' => Time.now } },
          sort:   '_id'
        }
        while message = collection.find_and_modify(selector)
          input_slice, header = parse_message(message)
          block.call(input_slice, header)
          count += input_slice.size
          break if !server.running?
          # Allow new jobs with a higher priority to interrupt this job worker
          break if server.re_check_seconds > 0 && ((Time.now - start_time) >= server.re_check_seconds)
        end
        count
      end

      # Requeue all failed slices
      #
      # Returns [Integer] the number of slices re-queued for processing
      #
      # Parameters:
      #   slice_numbers [Array<Integer>]
      #     Numbers of the slices to retry
      #     Default: Retry all slices for this job
      def requeue_failed_slices(slice_ids=nil)
        selector = {'failed' => { '$exists' => true }}
        # Apply slice_number override if applicable
        if slice_ids
          case slice_ids.size
          when 0
            return 0
          when 1
            selector['_id'] = slice_ids.first
          else
            selector['_id'] = { '$in' => slice_ids }
          end
        end

        result = collection.update(selector, {'$unset' => { 'server_name' => true, 'failed' => true, 'exception' => true, 'started_at' => true }}, { multi: true })
        result #['nModified'] || 0
      end

      # Requeue all slices for a server that is no longer available
      def requeue_incomplete_slices(server_name)
        collection.update({ 'server_name' => server_name }, { '$unset' => { 'server_name' => true, 'started_at' => true } })
      end

      # Set exception information for a specific slice
      def set_slice_exception(header, exc, record_number)
        # Set failure information and increment retry count
        collection.update(
          { '_id' => header['_id'] },
          {
            '$unset' => { 'server_name' => true },
            '$set' => {
              'exception' => {
                'class'         => exc.class.to_s,
                'message'       => exc.message,
                'backtrace'     => exc.backtrace || [],
                'server_name'   => header['server_name'],
                'record_number' => record_number
              },
              'failure_count' => header['failure_count'].to_i + 1,
              'failed'        => true
            }
          }
        )
      end

      ##########################################################################
      protected

      # Load records for processing from the supplied stream
      # All data read from the stream is converted into UTF-8
      # before being persisted.
      def upload_stream(io, options={})
        options             = options.dup
        delimiter           = options.delete(:delimiter)
        buffer_size         = options.delete(:buffer_size) || 65536
        strip_non_printable = options.delete(:strip_non_printable)
        strip_non_printable = true if strip_non_printable.nil?
        options.each { |option| raise ArgumentError.new("Unknown RocketJob::SlicedJob#add_records option: #{option.inspect}") }

        delimiter.force_encoding(UTF8_ENCODING) if delimiter

        batch_count  = 0
        end_index    = nil
        slice        = []
        record_count = 0
        buffer       = ''
        loop do
          partial = ''
          chunk = io.read(buffer_size)
          unless chunk
            logger.trace { "#upload_stream End of stream reached" }
            break
          end
          # Strip out non-printable characters before converting to UTF-8
          #      LC_ALL=UTF-8 tr -cd '[:print:]\n'
          #    Or, string = line.scan(/[[:print:]]/).join
          chunk = chunk.scan(/[[:print:]]|\r|\n/).join if strip_non_printable
          logger.trace { "#upload_stream Read #{chunk.size} bytes" }
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

          # Collect 'slice_size' lines and upload to mongo as a single record
          buffer.each_line(delimiter) do |line|
            if line.end_with?(delimiter)
              # Strip off delimiter when placing in record array
              slice << line[0..(end_index ||= (delimiter.size + 1) * -1)]
              batch_count += 1
              if batch_count >= slice_size
                # Write to Mongo
                record_count += upload_slice(slice)
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
        record_count += upload_slice(slice) if slice.size > 0

        record_count
      end

    end
  end
end