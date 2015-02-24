module RocketJob
  module Writer
    module Zip
      if defined?(JRuby)
        # Java has built-in support for Zip files

        # Write a single file in Zip format to the supplied output_stream
        #
        # Parameters
        #   output_stream [IO]
        #     Stream to write compressed data to.
        #     Must respond to #write
        #
        #   file_name [String]
        #     Name of the file within the Zip Stream
        #
        # The stream supplied to the block only responds to #write
        #
        # Example:
        #   RocketJob::Writer::Zip.output_stream(output_stream, file_name) do |io_stream|
        #     io_stream.write("hello world\n")
        #     io_stream.write("and more\n")
        #   end
        def self.output_stream(output_stream, file_name, &block)
          zout = Java::JavaUtilZip::ZipOutputStream.new(output_stream.to_outputstream)
          zout.put_next_entry(Java::JavaUtilZip::ZipEntry.new(file_name))
          io = zout.to_io
          block.call(io)
        ensure
          io.close if io
        end

        # Write a single file in Zip format to the supplied output file name
        #
        # Parameters
        #   zip_file_name [String]
        #     Full path and filename for the output zip file
        #
        #   file_name [String]
        #     Name of the file within the Zip Stream
        #
        # The stream supplied to the block only responds to #write
        #
        # Example:
        #   RocketJob::Writer::Zip.open_file('myfile.zip', 'hello.txt') do |io_stream|
        #     io_stream.write("hello world\n")
        #     io_stream.write("and more\n")
        #   end
        def self.output_file(zip_file_name, file_name, &block)
          out = Java::JavaIo::FileOutputStream.new(zip_file_name)
          zout = Java::JavaUtilZip::ZipOutputStream.new(out)
          zout.put_next_entry(Java::JavaUtilZip::ZipEntry.new(file_name))
          io = zout.to_io
          block.call(io)
        ensure
          io.close if io
          out.close if out
        end

      else

        # MRI needs Ruby Zip, since it only has native support for GZip
        begin
          require 'zip'
        rescue LoadError => exc
          puts "Please install gem rubyzip so that RocketJob can read Zip files in Ruby MRI"
          raise(exc)
        end

        def self.output_stream(output_stream, file_name,  &block)
          zos = ::Zip::OutputStream.new(file_name)
          # Hack to replace stream, otherwise it tries to do a #reopen followed by a #rewind
          zos.instance_variable_set(:@output_stream, output_stream)
          zos.put_next_entry(file_name)
          block.call(zos)
        ensure
          zos.close_buffer if zos
        end

        def self.output_file(zip_file_name, file_name, &block)
          zos = ::Zip::OutputStream.new(zip_file_name)
          zos.put_next_entry(file_name)
          block.call(zos)
        ensure
          zos.close_buffer if zos
        end

      end
    end
  end
end