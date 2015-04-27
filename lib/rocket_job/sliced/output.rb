# encoding: UTF-8
require 'tempfile'

module RocketJob
  module Sliced
    class Output < Slices
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
      #     format: [Symbol]
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
      #     delimiter: [String]
      #       Add the specified delimiter after every record when writing it
      #       to the output stream
      #       Default: OS Specific. Linux: "\n"
      #
      #     encrypt: [true|false|Hash]
      #       Encrypt text file using SymmetricEncryption
      #       When a Hash, it is passed as the options to SymmetricEncryption::Writer
      #       For Example, to compress the data in the file:
      #         { compress: true }
      #       Default: false
      #
      #     zip_filename: [String]
      #       When a the output is being compressed into a zip file, the name
      #       of the file inside the ZIP container needs a filename.
      #       Default: 'file'
      #
      # Note:
      #   - Currently the encrypt option is only available with format: :text
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
      #
      # Example:
      #   # Download text file in an encrypted file
      #   # Compress its contents before encrypting
      #   job.output.download('hello.csv', encrypt: { compress: true })
      def download(file_name_or_io, options={})
        is_file_name     = file_name_or_io.is_a?(String)
        options          = options.dup
        format           = options.delete(:format) || :auto
        delimiter        = options.delete(:delimiter) || $/
        buffer_size      = options.delete(:buffer_size) || 65535
        encrypt          = options.delete(:encrypt) || false
        record_count     = 0

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
          each do |slice|
            io.write(slice.join(delimiter) + delimiter)
            record_count += slice.size
          end
        end

        # Download the file from Mongo
        case format
        when :zip
          raise ArgumentError, "Option encrypt: #{encrypt.inspect} is not available with format: :zip"
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
          raise ArgumentError, "Option encrypt: #{encrypt.inspect} is not available with format: :gzip"
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
          if encrypt
            SymmetricEncryption::Writer.open(file_name_or_io,
              encrypt == true ? {} : encrypt,
              &writer
            )
          else
            if is_file_name
              File.open(file_name_or_io, 'wt', &writer)
            else
              writer.call(file_name_or_io)
            end
          end
        else
          raise ArgumentError.new("Invalid RocketJob download format: #{format.inspect}")
        end
        record_count
      end

    end
  end
end
