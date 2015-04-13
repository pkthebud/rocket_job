# encoding: UTF-8
module RocketJob
  module Sliced
    # A slice consists of several records and header information
    #
    # Encrypt and Compress are considered Slices level attributes
    # and are held there.
    # To persist this model in any way see the methods in Slices
    # that will persist or retrieve this model.
    class Slice
      include MongoMapper::Document

      # State Machine events and transitions
      #
      # Each slice is processed separately:
      #   :queued -> :running -> :completed
      #                       -> :failed     -> :running  ( manual )
      #
      # Slices are processed by ascending _id sort order
      #
      # Note:
      #   Currently all slices are destroyed on completion, so no slices
      #   are available in the completed state
      aasm column: :state do
        # Job has been created and is queued for processing ( Initial state )
        state :queued, initial: true

        # Job is running
        state :running

        # Job has completed processing ( End state )
        state :completed

        # Job failed to process and needs to be manually re-tried or aborted
        state :failed

        event :start do
          transitions from: :queued, to: :running
        end

        event :complete do
          transitions from: :running, to: :completed
        end

        event :fail do
          transitions from: :running, to: :failed
        end
      end

      #
      # Read-only attributes
      #

      # Current state, as set by AASM
      key :state,                   Symbol, default: :queued

      # Number of times that this job has failed to process
      key :failure_count,           Integer

      # This name of the server that this job is being processed by, or was processed by
      key :server_name,             String

      # Whether this job is currently in a failed state and needs to be re-tried if applicable
      key :failed,                  Boolean

      # Store the last exception for this job
      key :exception,               Hash

      # The records for this slice if encrypted
      key :encrypted_records,       String

      # The records for this slice if compressed
      key :compressed_records,      String

      # The records that make up this slice
      def records
        @records ||= begin

        end
      end

      # Returns [Slice] The decompressed / un-encrypted data string if applicable
      def self.from_bson(doc, encrypt, compress)
        return unless doc

        # In-flight Backward compatibility
        if old_slice = doc.delete('slice')
          doc['records'] = old_slice
        end

        slice = self.class.new
        slice.load_from_database(doc)
        if encrypt || compress
          str = if encrypt
            SymmetricEncryption.cipher.binary_decrypt(slice.records.to_s)
          else compress
            Zlib::Inflate.inflate(slice.records.to_s).force_encoding(UTF8_ENCODING)
          end
          # Convert the de-compressed and/or un-encrypted string back into an array
          slice.records = str.split(compress_delimiter)
        end
        slice
      end

      # Returns [Integer] the number of records in this slice
      def size
        records.size
      end

      # Set exception information for this slice
      def set_exception(exc, record_number)
        self.exception = {
          'class'         => exc.class.to_s,
          'message'       => exc.message,
          'backtrace'     => exc.backtrace || [],
          'server_name'   => server_name,
          'record_number' => record_number
        }
        self.failure_count = failure_count.to_i + 1
        self.failed        = true
        self.server_name   = nil
      end

      # Returns [Hash] the slice as a Hash for storage purposes
      # Compresses / Encrypts the slice according to the job setting
      def as_bson(encrypt, compress)
        attrs = attributes.dup
        if encrypt || compress
          #
          # TODO Handle non-string records too.
          #
          # Convert slice of records in a single string
          str  = slice.records.join(compress_delimiter)
          data = if encrypt
            # Encrypt to binary without applying an encoding such as Base64
            # Use a random_iv with each encryption for better security
            BSON::Binary.new(SymmetricEncryption.cipher.binary_encrypt(str, true, compress))
          else compress
            BSON::Binary.new(Zlib::Deflate.deflate(str))
          end
          attrs['records'] = data
        end
        attrs
      end

      #############################################################################
      private

      # Prevent this model from being persisted directly, since it has a dynamic collection
      def save(*_)
      end

      def update(*_)
      end

      def insert(*_)
      end

    end
  end
end