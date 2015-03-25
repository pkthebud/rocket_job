# encoding: UTF-8
require 'tempfile'
require 'rocket_job/collection/base'

module RocketJob
  module Collection
    class Output < Base
      attr_reader :collection

      # Output Collection for this job
      # Parameters
      #   job [RocketJob::Job]
      #     The job with which this output collection is associated
      #
      #   name [String]
      #     The named output storage when multiple outputs are being generated
      #     Default: None ( Uses the single default output collection for this job )
      def initialize(job, name=nil)
        collection_name = "rocket_job.outputs.#{job.id.to_s}"
        collection_name << ".#{name}" if name
        super(job, collection_name)
      end

      # Read output data and write it into the supplied filename or stream
      # The records are returned in '_id' order. Usually this is the order in
      # which the records were originally loaded.
      #
      # Returns [Integer] the number of records returned from the collection
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
      #     delimiter [String]
      #       Add the specified delimiter after every record when writing it
      #       to the output stream
      #       Default: OS Specific. Linux: "\n"
      #
      #     zip_filename [String]
      #       When a the output is being compressed into a zip file, the name
      #       of the file inside the ZIP container needs a filename.
      #       Default: 'file'
      #
      # Note:
      #   - Cannot stream into a Zip file since it has to rewind to the beginning
      #     to update the header information after adding a file. Use :gzip
      #   - If an io stream is supplied, it is read until it returns nil
      #
      # Notes:
      # * Remember to close the stream after calling #output_stream since
      #   #output_stream does not close the stream after all results have
      #   been written
      #
      # Example:
      #   # Load plain text records from a file
      #   job.output.download('hello.csv')
      #
      # Example:
      #   # Save to a Zip file:
      #   job.output.download('hello.zip', zip_filename: 'my_file.csv')
      #
      # Example:
      #   # Save to a stream:
      #   job.output.download(io, format: :gzip)
      #   # Internal filename defaults to: "#{job.klass_name.underscore}_#{job.id}"
      def download(file_name_or_io, options={})
        raise "Cannot download incomplete job: #{job.id}. Currently in state: #{job.state}" unless job.completed?

        is_file_name = file_name_or_io.is_a?(String)
        options      = options.dup
        format       = options.delete(:format) || :auto
        delimiter    = options.delete(:delimiter) || $/
        buffer_size  = options.delete(:buffer_size) || 65535
        record_count = 0

        if format == :auto
          raise ArgumentError.new("RocketJob Cannot use format :auto when downloading into a stream") unless is_file_name
          format = if file_name_or_io.ends_with?('.zip')
            :zip
          elsif file_name_or_io.ends_with?('.gzip') || file_name_or_io.ends_with?('.gz')
            :gzip
          else
            :text
          end
        end

        # Common writer to write data from this collection to the supplied stream
        writer = -> io do
          each_slice do |slice, _|
            # TODO Currently only supports text streams. Add support for binary data
            io.write(slice.join(delimiter) + delimiter)
            record_count += slice.size
          end
        end

        # Download the file from Mongo
        case format
        when :zip
          zip_filename = options.delete(:zip_filename) || 'file'
          if is_file_name
            RocketJob::Writer::Zip.write_file(file_name_or_io, zip_filename, &writer)
          else
            begin
              t = Tempfile.new('rocket_job')
              # Since ZIP cannot be streamed, download to a local file before streaming
              RocketJob::Writer::Zip.write_file(t.to_path, zip_filename, &writer)
              File.open(t.to_path, 'rb') do |file|
                while block = file.read(buffer_size)
                  file_name_or_io.write(block)
                end
              end
            ensure
              t.delete if t
            end
          end
        when :gzip
          if is_file_name
            Zlib::GzipWriter.open(file_name_or_io, &writer)
          else
            begin
              io = Zlib::GzipWriter.new(file_name_or_io)
              writer.call(io)
            ensure
              io.close if io
            end
          end
        when :text
          if is_file_name
            File.open(file_name_or_io, 'wt', &writer)
          else
            writer.call(file_name_or_io)
          end
        else
          raise ArgumentError.new("Invalid RocketJob download format: #{format.inspect}")
        end
        record_count
      end

      # Write the specified slice to the output collection with the supplied id
      # For one to one jobs where the output file record count matches the input
      # file record count, it is recommended to pass the _id supplied in the input
      # slice
      def upload_slice(slice, header={})
        begin
          collection.insert(build_message(slice, header))
        rescue Mongo::OperationFailure, Mongo::ConnectionFailure => exc
          # Ignore duplicates since it means the job was restarted
          raise(exc) unless exc.message.include?('E11000')
          logger.info "Skipped already processed slice# #{id}"
        end
      end
    end
  end
end
