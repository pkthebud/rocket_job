# encoding: UTF-8
module RocketJob
  class SlicedJob < Job
    # Prevent data in MongoDB from re-defining the model behavior
    #self.static_keys = true

    #
    # User definable attributes
    #
    # The following attributes are set when the job is created

    # Compress all working data
    # The arguments are not affected in any way, just the data stored in the
    # records and results collections will compressed
    key :compress,                Boolean, default: false

    # Encrypt all working data
    # The arguments are not affected in any way, just the data stored in the
    # records and results collections will be encrypted
    key :encrypt,                 Boolean, default: false

    # Number of records to include in each slice that is processed
    # Note:
    #   slice_size is only used by SlicedJob#upload_records & Input#upload_records
    #   When slices are supplied directly, their size is not modified to match this number
    key :slice_size,              Integer, default: 100

    # Whether to retain nil results.
    #
    # Only applicable if `collect_output` is `true`
    # Set to `false` to prevent collecting output from the perform
    # method when it returns `nil`.
    key :collect_nil_output,      Boolean, default: true

    # Maximum number of workers actively processing slices for this job.
    #
    # It attempts to ensure that the number of workers do not exceed this number.
    # This is not a hard limit and it is possible for the number of workers to
    # slightly exceed this value at times. It can also occur that the number of
    # slices running can drop below this number for a short period.
    #
    # This value can be modified while a job is running. The change will be picked
    # up at the start of processing slices, or after processing a slice and
    # `re_check_seconds` has been exceeded.
    #
    # Default: nil - No limits in place
    key :max_active_workers,      Integer

    #
    # Values that jobs can also update during processing
    #

    # Number of records in this job
    key :record_count,            Integer, default: 0

    #
    # Read-only attributes
    #

    # Breaks the :running state up into multiple sub-states:
    #   :running -> :before -> :processing -> :after -> :complete
    key :sub_state,               Symbol

    after_destroy :cleanup!

    validates_presence_of :record_count, :slice_size

    # Returns [true|false] whether to collect nil results from running this batch
    def collect_nil_output?
      collect_output? ? (collect_nil_output == true) : false
    end

    # Returns [RocketJob::Sliced::Input] input collection for holding input slices
    #
    # Parameters:
    #   name [String]
    #     The named input source when multiple inputs are being processed
    #     Default: None ( Uses the single default input collection for this job )
    def input(name=nil)
      collection_name = "rocket_job.inputs.#{id}"
      collection_name << ".#{name}" if name
      (@inputs ||= {})[name] = RocketJob::Sliced::Input.new(
        name:       collection_name,
        encrypt:    encrypt,
        compress:   compress,
        slice_size: slice_size
      )
    end

    # Returns [RocketJob::Sliced::Output] output collection for holding output slices
    # Returns nil if no output is being collected
    #
    # Parameters:
    #   name [String]
    #     The named output storage when multiple outputs are being generated
    #     Default: None ( Uses the single default output collection for this job )
    def output(name=nil)
      collection_name = "rocket_job.outputs.#{id}"
      collection_name << ".#{name}" if name
      (@outputs ||= {})[name] = RocketJob::Sliced::Output.new(
        name:       collection_name,
        encrypt:    encrypt,
        compress:   compress,
        slice_size: slice_size
      )
    end

    # Upload the supplied file_name or stream
    #
    # Updates the record_count after adding the records
    #
    # See RocketJob::Sliced::Input#upload for complete parameters
    #
    # Returns [Integer] the number of records uploaded
    #
    # Note:
    #   Not thread-safe. Only call from one thread at a time
    def upload(file_name_or_io, options={})
      count = input.upload(file_name_or_io, options)
      self.record_count += count
      count
    end

    # Upload the supplied slices for processing by workers
    #
    # Updates the record_count after adding the records
    #
    # Returns [Integer] the number of records uploaded
    #
    # Parameters
    #   `slice` [ Array<Hash | Array | String | Integer | Float | Symbol | Regexp | Time> ]
    #     All elements in `array` must be serializable to BSON
    #     For example the following types are not supported: Date
    #
    # Note:
    #   The caller should honor `:slice_size`, the entire slice is loaded as-is.
    #
    # Note:
    #   Not thread-safe. Only call from one thread at a time
    def upload_slice(slice)
      input.insert(slice)
      count = slice.size
      self.record_count += count
      count
    end

    # Upload each record returned by the supplied Block until it returns nil
    # The records are automatically grouped into slices based on :slice_size
    #
    # Updates the record_count after adding the records
    #
    # Returns [Integer] the number of records uploaded
    #
    # Note:
    #   The Block must return types that can be serialized to BSON.
    #   Valid Types: Hash | Array | String | Integer | Float | Symbol | Regexp | Time
    #   Invalid: Date, etc.
    #
    # Note:
    #   Not thread-safe. Only call from one thread at a time
    def upload_records(&block)
      count = input.upload_records(&block)
      self.record_count += count
      count
    end

    # Download the output data into the supplied file_name or stream
    #
    # See RocketJob::Sliced::Output#download for complete parameters
    #
    # Returns [Integer] the number of records downloaded
    #
    # Note:
    #   Not thread-safe. Only call from one thread at a time
    def download(file_name_or_io, options={})
      raise "Cannot download incomplete job: #{id}. Currently in state: #{state}-#{sub_state}" if processing?
      output.download(file_name_or_io, options)
    end

    # Processes records in each available slice for this job. Slices are processed
    # one at a time to allow for concurrent calls to this method to increase
    # throughput. Processing will continue until there are no more jobs available
    # for this job.
    #
    # Returns [Integer] the number of records processed
    # Returns [??] when no slices are available for processing on this job
    #
    # Slices are destroyed after their records are successfully processed
    # TODO Make this an option
    #
    # Results are stored in the output collection if `collect_output?`
    # `nil` results from workers are kept if `collect_nil_output`
    #
    # If an exception was thrown the entire slice of records is marked as failed.
    #
    # If the mongo_ha gem has been loaded, then the connection to mongo is
    # automatically re-established and the job will resume anytime a
    # Mongo connection failure occurs.
    #
    # Thread-safe, can be called by multiple threads at the same time
    def work(server)
      raise 'Job must be started before calling #work' unless running?
      count = 0
      begin
        worker = new_worker
        # If this is the first worker to pickup this job
        if before_processing?
          # before_perform
          worker.rocket_job_call(perform_method, arguments, event: :before, log_level: log_level)
          processing!
        elsif after_processing?
          # previous after_perform failed
          worker.rocket_job_call(perform_method, arguments, event: :after, log_level: log_level)
          complete!
          return 0
        end
        start_time = Time.now
        loop do
          # TODO Protect with a Mutex
          return count if !server.running?
          return count if max_active_workers && (active_count >= max_active_workers)
          slice = input.next_slice(server.name)
          break unless slice
          count += process_slice(worker, slice)

          # If the slice has failed and there are no other queued slices, fail the job
          if slice.failed? && (input.queued_count == 0)
            fail!
            return count
          end

          # Allow new jobs with a higher priority to interrupt this job worker
          return count if (Time.now - start_time) >= Config.instance.re_check_seconds
        end
        # Don't check if not everything has finished processing
        check_completion(worker)
      rescue Exception => exc
        set_exception(server.name, exc)
        raise exc if RocketJob::Config.inline_mode
      end
      count
    end

    # Prior to a job being made available for processing it can be processed one
    # slice at a time.
    #
    # For example, to extract the header row which would be in the first slice.
    #
    # Returns [Integer] the number of records processed in the slice
    #
    # Note: The slice will be removed from processing when this method completes
    def work_first_slice(worker, &block)
      raise 'Job must be running and in :before sub_state when calling #before_work' unless before_processing?
      if slice = input.first
        process_slice(worker, slice, &block)
      else
        0
      end
    end

    # Returns [Integer] percent of records completed so far
    # Returns nil if the total record count has not yet been set
    def percent_complete
      return 100 if completed?
      return 0 unless record_count.to_i > 0
      ((output.count.to_f / record_count) * 100).round
    end

    # Returns [Hash] status of this job
    def status(time_zone='EST')
      # TODO Add sub-state
      h = super(time_zone)
      h[:record_count] = record_count
      case
      when running? || paused? || failed?
        h[:active_slices]    = input.active_count
        h[:failed_slices]    = input.failed_count
        h[:queued_slices]    = input.queued_count
        h[:output_slices]    = output.count if collect_output?
        input_slices         = h[:running_slices] + h[:failed_slices] + h[:queued_slices]
        # Approximate number of input records
        input_records        = input_slices.to_f * slice_size
        h[:percent_complete] = ((1.0 - (input_records.to_f / record_count)) * 100).to_i if record_count > 0
        # TODO seconds has been replaced with duration
        #h[:records_per_hour] = (((record_count - input_records) / h[:seconds]) * 60 * 60).round if record_count > 0
        #h[:remaining_minutes] = h[:percent_complete] > 0 ? ((((h[:seconds].to_f / h[:percent_complete]) * 100) - h[:seconds]) / 60).to_i : nil
      when completed?
        h[:records_per_hour] = ((record_count / h[:seconds]) * 60 * 60).round
        count = output.count if collect_output?
        h[:output_slices]    = count if count
      when queued?
        h[:queued_slices]    = input.count
      end
      h
    end

    # Drop the input and output collections
    def cleanup!
      input.drop
      output.drop
    end

    # Is this job still being processed
    def processing?
      running? && (sub_state == :processing)
    end

    # Returns the default output filename for this job
    # which is made up of the worker class name and the job id
    def default_file_name
      "#{klass_name.underscore}_#{id}"
    end

    ############################################################################
    protected

    def before_processing?
      running? && (sub_state == :before)
    end

    def after_processing?
      running? && (sub_state == :after)
    end

    # Mark job as available for processing by other workers
    def processing!
      self.sub_state = :processing
      save!
    end

    # Add sub_state to aasm events
    def before_start
      super
      self.sub_state = :before unless self.sub_state
    end

    def before_complete
      super
      self.sub_state = nil
    end

    def before_abort
      super
      cleanup!
    end

    # Checks for completion and runs after_perform if defined
    def check_completion(worker)
      return unless record_count && (input.count == 0)
      # Run after_perform, only if it has not already been run by another worker
      # and prevent other workers from also completing it
      if result = collection.update({ '_id' => id, 'state' => :running, 'sub_state' => :processing }, { '$set' => { 'sub_state' => :after }})
        if (result['nModified'] || result['n']).to_i > 0
          # Also update the in-memory value
          self.sub_state = :after
          # after_perform
          worker.rocket_job_call(perform_method, arguments, event: :after, log_level: log_level)
          complete!
        end
      else
        reload
        cleanup! if aborted?
      end
    end

    # Process a single slice from Mongo
    # Once the slice has been successfully processed it will be removed from the input collection
    # Returns [Integer] the number of records successfully processed
    def process_slice(worker, slice, &block)
      record_number = 0
      logger.tagged("Slice #{slice.id}") do
        logger.info "Start #{klass}##{perform_method}"
        output_records = logger.benchmark_info(
          "Completed #{slice.size} records",
          metric:             "rocket_job/#{klass.underscore}/#{perform_method}",
          log_exception:      :full,
          on_exception_level: :error,
          silence:            log_level
        ) do
          worker.rocket_job_slice = slice
          results = slice.collect do |record|
            record_number += 1
            logger.tagged("Rec #{record_number}") do
              if block
                block.call(*arguments, record, slice)
              else
                worker.send(perform_method, *arguments, record)
              end
            end
          end
          worker.rocket_job_slice = nil
          results
        end

        if collect_output?
          if collect_nil_output?
            # Ignore duplicates on insert into output.collection since it successfully completed previously
            output.insert(output_records, slice)
          else
            output_records.compact!
            output.insert(output_records, slice) if output_records.size > 0
          end
        end

        # On successful completion remove the slice from the input queue
        # TODO Option to set it to completed instead of destroying it
        input.remove(slice)
      end
      record_number
    rescue Exception => exc
      slice.failure(exc, record_number)
      input.update(slice)
      raise exc if RocketJob::Config.inline_mode
      record_number > 0 ? record_number - 1 : 0
    end

    protected

    def before_retry
      super
      input.requeue_failed
    end

  end
end
