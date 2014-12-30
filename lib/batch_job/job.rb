require 'aasm'
# Re-use just the Resque Status UI
require 'resque/plugins/status/hash'
module BatchJob
  # Batch Job identifies each batch job submission
  #
  # - Make it an expired collection with purging of jobs completed_at older than 14 days
  #
  # Batch Job is a global "priority based queue" (wikipedia link).
  # All jobs are placed in a single global queue and the job with the highest priority
  # is always processed first.
  #
  # This differs from the traditional approach of separate
  # queues for jobs which quickly becomes cumbersome when their are for example
  # over a hundred different types of workers.
  #
  # The global priority based queue ensures that the servers are utilized to their
  # capacity without requiring constant manual intervention.
  #
  # Batch Job is designed to handle upwards of hundreds of millions of concurrent
  # "jobs" that are often encountered in high volume batch processing environments.
  # It is designed from the ground up to support large batch file processing.
  # For example a single file that contains millions of records to be processed
  # as quickly as possible without impacting other batch jobs with a higher priority.
  #
  class Job
    include MongoMapper::Document
    include AASM

    # Stored centrally with data replicated between data centers
    set_database_name   "shared_#{Rails.env}" unless Rails.env.development? || Rails.env.test?

    # Unique tracking number to identify this batch job (primary key)
    key :tracking_number,         String, default: -> { Clarity.random_tracking_number }
    key :description,             String

    # When the job was created
    key :created_at,              Time, default: -> { Time.now }
    # When processing started on this job
    key :started_at,              Time
    # When the job completed processing
    key :completed_at,            Time
    # Support running this job in the future
    #  Also set when a job fails and needs to be re-tried in the future
    key :run_at,                  Time

    # Number of times that this job has been retried
    key :retry_count,             Integer, Default: 0

    # Once a job is complete it should expire after set timeframe
    # TODO Or can Mongo use :completed_at + some timeframe?
    key :expires_at,              Time

    # When specified a job will be re-scheduled to run at it's next scheduled interval
    # Format is the same as cron
    key :schedule,                String

    # Priority of this job as it relates to other jobs [0..99]
    #   0: Lowest Priority
    #  99: Highest Priority
    #  50: Default Priority
    key :priority,                Integer, default: 50

    # Current state, as set by AASM
    key :state,                   Symbol, default: :queued

    # If present an email will be sent to these addresses when the job completes or is aborted
    # For multi-record jobs an email is also sent when the job starts
    key :email_addresses,         Array

    # Job should be marked as repeatable when it can be run multiple times
    # without changing the system state or modifying database contents.
    # Setting to false will result in an additional lookup on the results collection
    # before processing the record to ensure it was not previously processed.
    # This is necessary for retrying a job.
    key :repeatable,              Boolean, default: true

    # Number of records in this job
    #   Useful when updating the UI progress bar
    key :record_count,            Integer, default: 1

    # Any user supplied job parameters held in a Hash
    # All keys must be UTF-8 strings. The values can be any valid BSON type:
    #   Integer
    #   Float
    #   Time    (UTC)
    #   String  (UTF-8)
    #   Array
    #   Hash
    #   True
    #   False
    #   Symbol
    #   nil
    #   Regular Expression
    #
    # Note: Date is not supported, convert it to a UTC time
    key :parameters,              Hash,    default: {}

    # This job is assigned_to, or was processed by this server
    key :assigned_to,             String

    after_create  :create_status
    after_destroy :destroy_status

    validates_presence_of :tracking_number, :state, :priority, :record_count

    # State Machine events and transitions
    #
    # For Single Record jobs, usual processing:
    #   :queued -> :running -> :completed
    #                       -> :paused     -> :running  ( manual )
    #                       -> :retry      -> :running  ( future date )
    #
    # Any state other than :completed can transition manually to :aborted
    #
    # Work queue is priority based and then FIFO thereafter
    # means that records from existing multi-record jobs will be completed before
    # new jobs are started with the same priority.
    # Unless, the loader is not fast enough and the
    # records queue is empty. In this case the next multi-record job will
    # start loading too.
    #
    # Where: state: [:queued, :running], run_at: $lte: Time.now
    # Sort:  priority, created_at
    #
    # Index: state, run_at
    aasm column: :state do
      # Job has been created and is queued for processing ( Initial state )
      state :queued, initial: true

      # Job is running
      state :running

      # Job has completed processing ( End state )
      state :completed

      # Job is temporarily paused and no further processing will be completed
      # until this job has been resumed
      state :paused

      # Job was aborted and cannot be resumed ( End state )
      state :aborted

      event :start do
        after do
          self.started_at = Time.now
          # TODO change to find_and_modify() to prevent contention
          set(state: state, started_at: started_at)
          UserMailer.batch_job_started(self).deliver if email_addresses.present?
          # Update UI Job status
          set_status('message' => "Started loading records at #{started_at.in_time_zone('EST')}", 'num' => 0, 'total' => record_count, 'status' => 'working')
        end
        transitions from: :queued, to: :running
      end

      event :complete do
        after do
          self.completed_at = Time.now
          set(state: state, completed_at: completed_at)
          UserMailer.batch_job_completed(self).deliver if email_addresses.present?
          # Update UI Job status
          set_status('message' => "Processed #{record_count} record(s) in #{"%.2f" % (Time.now - started_at)} seconds at #{completed_at.in_time_zone('EST')}", 'num' => record_count, 'total' => record_count, 'status' => 'completed')
        end
        transitions from: :running, to: :completed
      end

      event :pause do
        after do
          set(state: state)
          UserMailer.batch_job_paused(self).deliver if email_addresses.present?
          # publish_pause
          set_status('message' => "Paused at #{Time.now.in_time_zone('EST')}", 'num' => results_collection.count, 'total' => record_count, 'status' => 'failed')
        end
        transitions from: :running, to: :paused
      end

      event :abort do
        after do
          set(state: state)
          UserMailer.batch_job_aborted(self).deliver if email_addresses.present?
          # publish_abort
          set_status('message' => "Aborted at #{Time.now.in_time_zone('EST')}", 'num' => results_collection.count, 'total' => record_count, 'status' => 'killed')
        end
        transitions from: :running, to: :aborted
        transitions from: :queued, to: :aborted
      end
    end

    # Create capped collection and indexes
    def self.create_indexes
      # Create indexes
      ensure_index [[:tracking_number, 1]], background: true, unique: true
      ensure_index [[:state, 1], [:priority, 1]], background: true
    end

    # Use the working storage connection
    def self.connection
      SharedStorage.connection
    end

    # Returns [Integer] percent of records completed so far
    # Returns nil if the total record count has not yet been set
    def percent_complete
      completed? ? 100 : 0
    end

    # Returns Hash of the current status of this job
    def status
      {
        state:                state,
        percent_complete:     percent_complete,
        started_at:           started_at.in_time_zone('EST'),
        completed_at:         completed? ? completed_at.in_time_zone('EST') : nil,
        seconds:              completed? ? (completed_at - started_at) : (Time.now - started_at),
        record_count:         record_count,
      }
    end

    # Updates the processed count in the status UI if the record_count has been set
    def update_processed_count
      return unless record_count.to_i > 0
      Resque::Plugins::Status::Hash.set(tracking_number, 'name' => description, 'message' => "Started at #{started_at.in_time_zone('EST')}", 'num' => results_collection.count, 'total' => record_count, 'status' => 'working')
    end

    # Add support for MongoMapper
    def aasm_read_state
      state
    end

    # may be overwritten by persistence mixins
    def aasm_write_state(new_state)
      self.state = new_state
      save!
    end

    # may be overwritten by persistence mixins
    def aasm_write_state_without_persistence(new_state)
      self.state = new_state
    end

    # Same basic formula for calculating retry interval as delayed_job and Sidekiq
    # TODO Consider lowering the priority automatically for a retry?
    def seconds_to_delay(count)
      (count ** 4) + 15 + (rand(30)*(count+1))
    end

    # Update UI Status
    def set_status(h)
      h.merge('name' => description)
      Resque::Plugins::Status::Hash.set(tracking_number, h)
    end

    private

    # Delete Status from UI if job is destroyed
    def destroy_status
      Resque::Plugins::Status::Hash.remove(tracking_number)
    end

    # Create UI Job status
    def create_status
      Resque::Plugins::Status::Hash.create(tracking_number, 'name' => description, 'message' => "Queued as of #{Time.now.in_time_zone('EST')}", 'status' => 'queued')
    end
  end
end
