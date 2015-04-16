# encoding: UTF-8
require 'aasm'
require 'zlib'
require 'symmetric-encryption'
module RocketJob
  module Sliced
    # A slice is an Array of Records, along with meta-data that is used
    # or set during processing of the individual records
    #
    # Encrypt and Compress are considered Slices level attributes
    # and are held there.
    # To persist this model in any way see the methods in Slices
    # that will persist or retrieve this model.
    class Slice < Array
      include MongoMapper::Document
      include AASM

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

        event :fail  do
          transitions from: :running, to: :failed
        end

        event :retry  do
          transitions from: :failed, to: :running
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

      # The last exception for this slice if any
      one :exception,               class_name: 'RocketJob::JobException'

      # Alternative way to replace the records within this slice
      def records=(records)
        clear
        concat(records)
      end

      # Alternative way to obtain the list of records within this slice
      alias_method :records, :to_a

      # Fail this slice, along with the exception that caused the failure
      def failure(exc=nil, record_number=nil)
        if exc
          self.exception          = JobException.from_exception(exc)
          exception.server_name   = server_name
          exception.record_number = record_number
        end
        self.failure_count = failure_count.to_i + 1
        self.server_name   = nil
        fail
      end

      # Returns [Hash] the slice as a Hash for storage purposes
      # Compresses / Encrypts the slice according to the job setting
      def to_bson(options={})
        options            = options.dup
        encrypt            = options.delete(:encrypt)  || false
        compress           = options.delete(:compress) || false
        options.each{ |o| raise ArgumentError, "Invalid option: #{o.inspect}" }

        attrs = attributes.dup
        attrs['records'] = if encrypt || compress
          # Convert slice of records in a single string
          str  = BSON.serialize('r' => to_a).to_s
          data = if encrypt
            # Encrypt to binary without applying an encoding such as Base64
            # Use a random_iv with each encryption for better security
            SymmetricEncryption.cipher.binary_encrypt(str, true, compress)
          else compress
            Zlib::Deflate.deflate(str)
          end
          BSON::Binary.new(data)
        else
          to_a
        end
        attrs
      end

      # Returns [Slice] The decompressed / un-encrypted data string if applicable
      def self.from_bson(doc)
        # Extract the records from doc
        records = doc.delete('records') || doc.delete('slice')

        slice = new
        slice.send(:load_from_database, doc)

        # Records not compressed or encrypted?
        if records.is_a?(Array)
          slice.concat(records)
        else
          # Convert BSON::Binary to a string
          str = records.to_s
          if SymmetricEncryption::Cipher.has_header?(str)
            str = SymmetricEncryption.cipher.binary_decrypt(str)
          else
            str = Zlib::Inflate.inflate(records.to_s)
          end
          slice.concat(BSON.deserialize(str)['r'])
        end
        slice
      end

      def inspect
        "#<#{self.class.name} #{attributes.collect{|k,v| "#{k.inspect}=>#{v.inspect}"}.join(' ')}> #{to_a.inspect}"
      end

      #############################################################################
      private

      # Prevent this model from being persisted directly, since it has a dynamic collection
      def save(*_)
      end

      def save!(*_)
      end

      def update(*_)
      end

      def insert(*_)
      end

    end
  end
end