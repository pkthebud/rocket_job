module RocketJob
  module Sliced
    class Slices
      include Enumerable

      attr_accessor :encrypt, :compress, :slice_size
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
      def initialize(params)
        params              = params.dup
        name                = params.delete(:name)
        raise 'Missing mandatory parameter :name' unless name
        @encrypt            = params.delete(:encrypt) || false
        @compress           = params.delete(:compress) || false
        @slice_size         = params.delete(:slice_size) || 100
        @collection         = Config.mongo_work_connection.db[name]
        params.each { |param| raise(ArgumentError, "Invalid parameter: #{param}") }
      end

      # Returns [Integer] the number of slices remaining to be processed
      def count
        collection.count
      end
      alias_method :size, :count
      alias_method :length, :count

      # Iterate over each slice in this collection
      def each(filter={}, &block)
        collection.find(filter, sort: '_id', timeout: false) do |cursor|
          cursor.each { |doc| block.call(Slice.from_bson(doc)) }
        end
      end

      # Returns [Slice] the first slice in the collection
      # and the header for that slice
      # Returns nil if the collection does not contain any slices
      def first
        doc = collection.find_one({}, sort: [['_id', Mongo::ASCENDING]])
        Slice.from_bson(doc) if doc
      end

      # Returns [Slice] the last slice in the collection
      # and the header for that slice
      # Returns nil if the collection does not contain any slices
      def last
        doc = collection.find_one({}, sort: [['_id', Mongo::DESCENDING]])
        Slice.from_bson(doc) if doc
      end

      # Insert a new slice into the collection
      #
      # Returns [Integer] the number of records uploaded
      #
      # Parameters
      #   slice [RocketJob::Sliced::Slice]
      #     The slice to write to the slices collection
      #     If slice is an Array, it will be converted to a Slice before inserting
      #     into the slices collection
      #
      # Note:
      #   `slice_size` is not enforced.
      #   However many records are present in the slice will be written as a
      #   slingle slice to the slices collection
      def insert(slice)
        slice = Slice.new(records: slice) unless slice.is_a?(Slice)
        collection.insert(slice.to_bson(encrypt: encrypt, compress: compress))
        slice
      end
      alias_method :<<, :insert

      # Update an existing slice in the collection
      def update(slice)
        collection.update({'_id' => slice.id}, slice.to_bson(encrypt: encrypt, compress: compress))
      end

      # Removes the specified slice from the input collection
      def remove(slice)
        collection.remove('_id' => slice.id)
      end

      # Drop this collection since it is no longer needed
      def destroy
        collection.drop
      end

    end
  end
end