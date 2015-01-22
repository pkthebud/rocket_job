# encoding: UTF-8
require 'aasm'
# Temporarily Re-use just the Resque Status UI
begin
  require 'resque/plugins/status/hash'
rescue LoadError
end

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
    include SemanticLogger::Loggable

    # Stored centrally with data replicated between data centers
    #set_database_name   "shared_#{Rails.env}" unless Rails.env.development? || Rails.env.test?

    #
    # User defined attributes
    #

    # Description for this job instance
    key :description,             String

    # Priority of this job as it relates to other jobs [1..100]
    #   1: Lowest Priority
    # 100: Highest Priority
    #  50: Default Priority
    key :priority,                Integer, default: 50

    # Support running this job in the future
    #   Also set when a job fails and needs to be re-tried in the future
    key :run_at,                  Time

    # If a job has not started by this time, destroy it
    key :expires_at,              Time

    # When specified a job will be re-scheduled to run at it's next scheduled interval
    # Format is the same as cron
    key :schedule,                String

    # If present an email will be sent to these addresses when the job completes or is aborted
    # For multi-record jobs an email is also sent when the job starts
    key :email_addresses,         Array

    # Job should be marked as repeatable when it can be run multiple times
    # without changing the system state or modifying database contents.
    # Setting to false will result in an additional lookup on the results collection
    # before processing the record to ensure it was not previously processed.
    # This is necessary for retrying a job.
    key :repeatable,              Boolean, default: true

    # When the job completes destroy it from both the database and the UI
    key :destroy_on_completion,   Boolean, default: false

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

    # Only give access through the Web UI to this group identifier
    key :group,                   String

    #
    # Read-only attributes
    #

    # Current state, as set by AASM
    key :state,                   Symbol, default: :queued

    # When the job was created
    key :created_at,              Time, default: -> { Time.now }

    # When processing started on this job
    key :started_at,              Time

    # When the job completed processing
    key :completed_at,            Time

    # Number of times that this job has been retried
    key :retry_count,             Integer, default: 0

    # This job is assigned_to, or was processed by this server
    key :assigned_to,             String

    #
    # Values that jobs can update during processing
    #

    # Allow the worker to set how far it is in the job
    # Any integer from 0 to 100
    # For Multi-record jobs do not set this value directly
    key :percent_complete,        Integer, default: 0

    after_create  :create_status
    after_destroy :destroy_status

    validates_presence_of :state, :priority, :retry_count, :created_at, :percent_complete
    validates :percent_complete, inclusion: 0..100

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
          set(state: state, started_at: started_at)
          UserMailer.batch_job_started(self).deliver if email_addresses.present?
          update_ui_status
        end
        transitions from: :queued, to: :running
      end

      event :complete do
        after do
          self.completed_at = Time.now
          set(state: state, completed_at: completed_at)
          UserMailer.batch_job_completed(self).deliver if email_addresses.present?
          update_ui_status
        end
        transitions from: :running, to: :completed
      end

      event :pause do
        after do
          self.completed_at = Time.now
          set(state: state, completed_at: completed_at)
          UserMailer.batch_job_paused(self).deliver if email_addresses.present?
          update_ui_status
        end
        transitions from: :running, to: :paused
      end

      event :resume do
        after do
          self.completed_at = nil
          set(state: state, completed_at: completed_at)
          UserMailer.batch_job_resumed(self).deliver if email_addresses.present?
          update_ui_status
        end
        transitions from: :running, to: :paused
      end

      event :abort do
        after do
          self.completed_at = Time.now
          set(state: state)
          UserMailer.batch_job_aborted(self).deliver if email_addresses.present?
          update_ui_status
        end
        transitions from: :running, to: :aborted
        transitions from: :queued, to: :aborted
      end
    end

    # Create indexes
    def self.create_indexes
      # Create indexes
      ensure_index [[:state, 1], [:priority, 1]], background: true
    end

    # Returns [Hash] status of this job
    def status(time_zone='EST')
      h = {
        state:                state,
        description:          description
      }
      h[:started_at]         = started_at.in_time_zone(time_zone) if started_at

      case
      when running? || paused?
        h[:status]           = running? ? "Started at #{started_at.in_time_zone(time_zone)}" : "Paused at #{completed_at.in_time_zone(time_zone)}"
        h[:seconds]          = Time.now - started_at
        h[:paused_at]        = completed_at.in_time_zone(time_zone) if paused?
        h[:percent_complete] = percent_complete if percent_complete
      when completed?
        h[:seconds]          = completed_at - started_at
        h[:status]           = "Completed at #{completed_at.in_time_zone(time_zone)}"
        h[:completed_at]     = completed_at.in_time_zone(time_zone)
      when queued?
        h[:wait_seconds]     = Time.now - created_at
        h[:status]           = "Queued for #{"%.2f" % h[:wait_seconds]} seconds"
      when aborted?
        h[:status]           = "Aborted at #{completed_at.in_time_zone(time_zone)}"
        h[:aborted_at]       = completed_at.in_time_zone(time_zone)
        h[:percent_complete] = percent_complete if percent_complete
      else
        h[:status]           = "Unknown job state: #{state}"
      end
      h
    end

    # Add AASM support for MongoMapper
    if AASM::VERSION.to_f <= 4.0
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
    end

    # Same basic formula for calculating retry interval as delayed_job and Sidekiq
    # TODO Consider lowering the priority automatically after every retry?
    def seconds_to_delay(count)
      (count ** 4) + 15 + (rand(30)*(count+1))
    end

    # Hijack the Resque Status UI until BatchJob gets it's own
    def update_ui_status
      resque_status = case
      when completed?
        'completed'
      when paused?
        'failed'
      when aborted?
        'killed'
      when queued?
        'queued'
      else
        'working'
      end

      h = self.status

      resque_status = {
        'message' => h[:message],
        'num'     => completed? ? 100 : (percent_complete || 0),
        'total'   => 100,
        'name'    => h[:description],
        'status'  => resque_status
      }
      Resque::Plugins::Status::Hash.set(id.to_s, resque_status) if defined?(Resque::Plugins::Status::Hash)
    end

    private

    @@work_connection = nil

    # Delete Status from UI if job is destroyed
    def destroy_status
      Resque::Plugins::Status::Hash.remove(id.to_s) if defined?(Resque::Plugins::Status::Hash)
    end

    # Create UI Job status
    def create_status
      Resque::Plugins::Status::Hash.create(id.to_s, 'name' => description, 'message' => "Queued as of #{Time.now.in_time_zone('EST')}", 'status' => 'queued') if defined?(Resque::Plugins::Status::Hash)
    end
  end
end
