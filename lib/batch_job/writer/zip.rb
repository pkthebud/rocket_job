module BatchJob
  module Reader
    module Zip
      if defined?(JRuby)
        # Java has built-in support for Zip files

        # Convert Java Stream into a Ruby Stream
        class JavaStreamAsRubyStream
          def initialize(java_stream)
            @java_stream = java_stream
            @bytes       = nil
          end

          def read(size)
            # Auto-grow byte array if needed
            @bytes = Java::byte[size].new if !@bytes || (@bytes.size < size)
            n = @java_stream.read(@bytes)
            n > 0 ? String.from_java_bytes(@bytes)[0..n-1] : nil
          end
        end

        def self.file(job, file_name)
          fin = Java::JavaIo::FileInputStream.new(file_name)
          zin = Java::JavaUtilZip::ZipInputStream.new(fin)
          entry = zin.get_next_entry
          job.parameters['csv_filename'] = entry.name
          job.load_stream(JavaStreamAsRubyStream.new(zin))
        ensure
          zin.close if zin
        end
      else
        # MRI needs Ruby Zip, since it only has native support for GZip
        begin
          require 'zip'
        rescue LoadError => exc
          puts "Please install gem rubyzip so that BatchJob can read Zip files in Ruby MRI"
          raise(exc)
        end

        # Read from a Zip file and stream into Job
        #  Sets job parameter 'csv_filename' to the name of the first file found in the zip
        def self.file(job, file_name)
          ::Zip::File.open(file_name) do |zip_file|
            raise 'The zip archive did not have any files in it.' if zip_file.count == 0
            raise 'The zip archive has more than one file in it.' if zip_file.count != 1
            entry = zip_file.first
            job.parameters['csv_filename'] = entry.name
            entry.get_input_stream { |io_stream| job.load_stream(io_stream) }
          end
        end

      end
    end
  end
end