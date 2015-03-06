# encoding: UTF-8
require 'rocket_job/collection/base'
module RocketJob
  module Collection
    class Output < Base
      include SemanticLogger::Loggable

      attr_reader :collection

      # Output Collection for this job
      # Parameters
      #   job [RocketJob::Job]
      #     The job with which this output collection is associated
      #
      #   counter [Integer]
      #     When supplied, this counter can be used to create multiple output
      #     collections for a single job.
      #     Default: None
      #
      def initialize(job, counter=nil)
        name = "rocket_job.outputs.#{job.id.to_s}"
        name << ".#{counter}" if counter
        super(job, name)
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
      #   - When zip format, the Zip file/stream must contain only one file, the first file found will be
      #     loaded into the job
      #   - If an io stream is supplied, it is read until it returns nil
      #
      # Notes:
      # * Remember to close the stream after calling #output_stream since
      #   #output_stream does not close the stream after all results have
      #   been written
      #
      # Example:
      #   # Load plain text records from a file
      #   job.upload('hello.csv')
      #
      # Example:
      #   # Load from a Zip file:
      #   job.upload('hello.zip')
      def read(file_name_or_io, options={})
        is_file_name = file_name_or_io.is_a?(String)
        options      = options.dup
        format       = options.delete(:format) || :text
        delimiter    = options.delete(:delimiter) || $/
        record_count = 0

        if format == :auto
          raise ArgumentError.new("RocketJob Cannot use format :auto when uploading a stream") unless is_file_name
          format = file_name_or_io.ends_with?('.zip') ? :zip : :text
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
            RocketJob::Writer::Zip.output_file(file_name_or_io, zip_filename, &writer)
          else
            RocketJob::Writer::Zip.output_stream(file_name_or_io, zip_filename, &writer)
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
      def write_slice(slice, id=nil)
        begin
          collection.insert(build_message(slice, '_id' => id))
        rescue Mongo::OperationFailure, Mongo::ConnectionFailure => exc
          # Ignore duplicates since it means the job was restarted
          raise(exc) unless exc.message.include?('E11000')
          logger.info "Skipped already processed slice# #{id}"
        end
      end
    end
  end
end
