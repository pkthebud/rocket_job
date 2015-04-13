require 'zlib'
require 'symmetric-encryption'
module RocketJob
  module Collection
    class Slices
      include Enumerable

      attr_accessor :encrypt, :compress, :slice_size, :compress_delimiter
      attr_reader :collection

      # Parameters
      #   name: [String]
      #     Name of the collection to create
      #   slice_size: [Integer]
      #     Number of records to store in each slice
      #     Default: 100
      #   compress: [Boolean]
      #     Whether to compress slice records held in this collection
      #     Default: false
      #   encrypt: [Boolean]
      #     Whether to encrypt slice records held in this collection
      #     Default: false
      #   compress_delimiter: [String]
      #     When compressing or encrypting data, this is the delimiter to use
      #     when serializing multiple records
      #     Default: '|@|'
      def initialize(params)
        params              = params.dup
        name                = params.delete(:name)
        raise 'Missing mandatory parameter :name' unless name
        @encrypt            = params.delete(:encrypt) || false
        @compress           = params.delete(:compress) || false
        @slice_size         = params.delete(:slice_size) || 100
        @compress_delimiter = params.delete(:compress_delimiter) || '|@|'
        @collection         = Config.mongo_work_connection.db[name]
        params.each { |param| raise(ArgumentError, "Invalid parameter: #{param}") }
      end

      # Returns [Integer] the number of slices remaining to be processed
      def count
        collection.count
      end

      # Iterate over each slice in this collection
      def each(filter={}, &block)
        collection.find(filter, sort: '_id', timeout: false) do |cursor|
          cursor.each { |doc| block.call(self.class.from_bson(doc, encrypt, compress)) }
        end
      end

      # Returns [Slice] the first slice in the collection
      # and the header for that slice
      # Returns nil if the collection does not contain any slices
      def first
        doc = collection.find_one(sort: [['_id', Mongo::ASCENDING]])
        self.class.from_bson(doc, encrypt, compress) if doc
      end

      # Returns [Slice] the last slice in the collection
      # and the header for that slice
      # Returns nil if the collection does not contain any slices
      def last
        doc = collection.find_one(sort: [['_id', Mongo::DESCENDING]])
        self.class.from_bson(doc, encrypt, compress) if doc
      end

      # Insert a new slice into the collection
      def insert(slice)
        collection.insert(slice.as_bson(encrypt, compress))
      end
      alias_method :<<, :insert

      # Update an existing slice in the collection
      def update(slice)
        collection.update({'_id' => slice.id}, slice.as_bson(encrypt, compress))
      end

      # Drop this collection since it is no longer needed
      def cleanup!
        collection.drop
      end

      # Removes the specified slice from the input collection
      def remove_slice(slice)
        collection.remove('_id' => slice.id)
      end

    end
  end
end