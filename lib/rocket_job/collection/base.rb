require 'zlib'
require 'symmetric-encryption'
module RocketJob
  module Collection
    class Base

      attr_accessor :encrypt, :compress, :slice_size, :compress_delimiter
      attr_reader :collection

      def initialize(job, name)
        @encrypt            = job.encrypt
        @compress           = job.compress
        @slice_size         = job.slice_size
        @compress_delimiter = job.compress_delimiter
        @collection         = Config.mongo_work_connection.db[name]
      end

      # Returns [Integer] the number of slices already processed
      def total_slices
        collection.count
      end

      # Iterate over each slice in this collection
      def each_slice(filter={}, &block)
        collection.find(filter, sort: '_id', timeout: false) do |cursor|
          cursor.each { |message| block.call(*parse_message(message)) }
        end
      end

      # Iterate over each record in this collection
      def each_record(filter={},&block)
        each_slice(filter) do |slice, _|
          slice.each{ |record| block.call(record) }
        end
      end

      # Drop the input collection
      def cleanup!
        collection.drop
      end

      # Builds the message to be stored including the supplied slice
      # Compresses / Encrypts the slice according to the job setting
      def build_message(slice, header={})
        data = if encrypt || compress
          #
          # TODO Handle non-string records too.
          #
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

    end
  end
end