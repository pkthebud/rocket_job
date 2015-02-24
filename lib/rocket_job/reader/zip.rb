module RocketJob
  module Reader
    module Zip
      if defined?(JRuby)
        # Java has built-in support for Zip files
        # https://github.com/jruby/jruby/wiki/CallingJavaFromJRuby

        # Read a Zip file passing the input stream from the first file
        # found in the zip file to the supplied block
        #
        # Example:
        #   RocketJob::Reader::Zip.open_file(file_name) do |io_stream, source|
        #     # Display header info
        #     puts source.inspect
        #
        #     # Read 256 bytes at a time
        #     while data = io_stream.read(256)
        #       puts data
        #     end
        #   end
        #
        # Note: The stream currently only supports #read
        def self.input_file(file_name, &block)
          fin = Java::JavaIo::FileInputStream.new(file_name)
          zin = Java::JavaUtilZip::ZipInputStream.new(fin)
          entry = zin.get_next_entry
          block.call(zin.to_io,
            { name: entry.name, compressed_size: entry.compressed_size, time: entry.time, size: entry.size, comment: entry.comment })
        ensure
          zin.close if zin
          fin.close if fin
        end

        # Read a stream containing a Zip data, passing the input stream from
        # the first file found in the zip file to the supplied block
        #
        # Example:
        #   File.open('myfile.zip') do |io|
        #     RocketJob::Reader::Zip.input_stream(io) do |io_stream, source|
        #       # Display header info
        #       puts source.inspect
        #
        #       # Read 256 bytes at a time
        #       while data = io_stream.read(256)
        #         puts data
        #       end
        #     end
        #   end
        #
        # Note: The stream currently only supports #read
        def self.input_stream(input_stream, &block)
          zin = Java::JavaUtilZip::ZipInputStream.new(input_stream.to_inputstream)
          entry = zin.get_next_entry
          block.call(zin.to_io,
            { name: entry.name, compressed_size: entry.compressed_size, time: entry.time, size: entry.size, comment: entry.comment })
        ensure
          zin.close if zin
        end
      else
        # MRI needs Ruby Zip, since it only has native support for GZip
        begin
          require 'zip'
        rescue LoadError => exc
          puts "Please install gem rubyzip so that RocketJob can read Zip files in Ruby MRI"
          raise(exc)
        end

        # Read from a Zip file and stream into Job
        #  Sets job parameter 'csv_filename' to the name of the first file found in the zip
        def self.input_file(file_name, &block)
          ::Zip::File.open(file_name) do |zip_file|
            raise 'The zip archive did not have any files in it.' if zip_file.count == 0
            raise 'The zip archive has more than one file in it.' if zip_file.count != 1
            entry = zip_file.first
            entry.get_input_stream do |io_stream|
              block.call(io_stream,
                { name: entry.name, compressed_size: entry.compressed_size, time: entry.time, size: entry.size, comment: entry.comment })
              end
          end
        end

        def self.input_stream(io, &block)
          zin = ::Zip::InputStream.new(io)
          entry = zin.get_next_entry
          block.call(zin,
            { name: entry.name, compressed_size: entry.compressed_size, time: entry.time, size: entry.size, comment: entry.comment })
        ensure
          zin.close if zin
        end

      end
    end
  end
end