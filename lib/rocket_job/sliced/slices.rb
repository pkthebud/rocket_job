module RocketJob
  module Sliced
    class Slices
      include Enumerable
      include SemanticLogger::Loggable

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
      # Returns nil if the collection does not contain any slices
      def first
        doc = collection.find_one({}, sort: [['_id', Mongo::ASCENDING]])
        Slice.from_bson(doc) if doc
      end

      # Returns [Slice] the last slice in the collection
      # Returns nil if the collection does not contain any slices
      def last
        doc = collection.find_one({}, sort: [['_id', Mongo::DESCENDING]])
        Slice.from_bson(doc) if doc
      end

      # Returns [Slice] with the matching id from the collection
      # Returns nil if the slice was not found
      #
      # Note:
      #  If the id is a valid BSON::ObjectId string it will be converted to a
      #  BSON::ObjectId before performing the lookup
      def find(id)
        id = BSON::ObjectId.from_string(id) if !id.is_a?(BSON::ObjectId) && BSON::ObjectId.legal?(id)
        doc = collection.find_one(id)
        Slice.from_bson(doc) if doc
      end

      # Insert a new slice into the collection
      #
      # Returns [Integer] the number of records uploaded
      #
      # Parameters
      #   slice [RocketJob::Sliced::Slice | Array]
      #     The slice to write to the slices collection
      #     If slice is an Array, it will be converted to a Slice before inserting
      #     into the slices collection
      #
      #   input_slice [RocketJob::Sliced::Slice]
      #     The input slice to which this slice corresponds
      #     The id of the input slice is copied across
      #     If the insert results in a duplicate record it is ignored, to support
      #     restarting of jobs that failed in the middle of processing.
      #     A warning is logged that the slice has already been processed.
      #
      # Note:
      #   `slice_size` is not enforced.
      #   However many records are present in the slice will be written as a
      #   slingle slice to the slices collection
      def insert(slice, input_slice=nil)
        slice = Slice.new(records: slice) unless slice.is_a?(Slice)
        if input_slice
          # Retain input_slice id in the new output slice
          slice.id = input_slice.id
          begin
            collection.insert(slice.to_bson(encrypt: encrypt, compress: compress))
          rescue Mongo::OperationFailure, Mongo::ConnectionFailure => exc
            # Ignore duplicates since it means the job was restarted
            raise(exc) unless exc.message.include?('E11000')
            logger.warn "Skipped already processed slice# #{slice.id}"
          end
        else
          collection.insert(slice.to_bson(encrypt: encrypt, compress: compress))
        end
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
      def drop
        collection.drop
      end

      # Clear out the slices in this collection
      def clear
        collection.remove({})
      end

    end
  end
end