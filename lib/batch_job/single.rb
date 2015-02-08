# encoding: UTF-8
require 'aasm'
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
  class Single
    include MongoMapper::Document
    include AASM
    include SemanticLogger::Loggable

    #
    # User definable attributes
    #
    # The following attributes are set when the job is created

    # Description for this job instance
    key :description,             String

    # Class that implements this jobs behavior
    key :klass,                   String

    # Method that must be invoked to complete this job
    key :method,                  String, default: 'perform'

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
    key :failure_count,           Integer, default: 0

    # This job is being processed by, or was processed by this server
    key :server,                  String

    #
    # Values that jobs can update during processing
    #

    # Allow the worker to set how far it is in the job
    # Any integer from 0 to 100
    # For Multi-record jobs do not set this value directly
    key :percent_complete,        Integer, default: 0

    # Store the last exception for this job
    key :exception,               Hash

    # Store all job types in this collection
    set_collection_name 'batch_jobs'

    validates_presence_of :state, :priority, :failure_count, :created_at, :percent_complete
    validates :percent_complete, inclusion: 0..100

    # State Machine events and transitions
    #
    # For Single Record jobs, usual processing:
    #   :queued -> :running -> :completed
    #                       -> :paused     -> :running  ( manual )
    #                       -> :failed     -> :running  ( manual )
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

      # Job failed to process and needs to be manually re-tried or aborted
      state :failed

      # Job failed to process previously and is scheduled to be retried at a
      # future date
      state :retry

      # Job was aborted and cannot be resumed ( End state )
      state :aborted

      event :start do
        after do
          self.started_at = Time.now
          set(state: state, started_at: started_at)
          UserMailer.batch_job_started(self).deliver if email_addresses.present?
        end
        transitions from: :queued, to: :running
      end

      event :complete do
        after do
          self.completed_at = Time.now
          set(state: state, completed_at: completed_at)
          UserMailer.batch_job_completed(self).deliver if email_addresses.present?
        end
        transitions from: :running, to: :completed
      end

      event :fail do
        after do
          self.completed_at = Time.now
          set(state: state, completed_at: completed_at)
          UserMailer.batch_job_failed(self).deliver if email_addresses.present?
        end
        transitions from: :running, to: :failed
      end

      event :pause do
        after do
          self.completed_at = Time.now
          set(state: state, completed_at: completed_at)
          UserMailer.batch_job_paused(self).deliver if email_addresses.present?
        end
        transitions from: :running, to: :paused
      end

      event :resume do
        after do
          self.completed_at = nil
          set(state: state, completed_at: completed_at)
          UserMailer.batch_job_resumed(self).deliver if email_addresses.present?
        end
        transitions from: :running, to: :paused
      end

      event :abort do
        after do
          self.completed_at = Time.now
          set(state: state)
          UserMailer.batch_job_aborted(self).deliver if email_addresses.present?
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

    # Calls the perform method for the class specified in this job
    # Returns the result of the perform, but nothing is done with the result.
    #
    # If an exception is thrown the job is marked as failed and the exception
    # is set in the job itself.
    #
    # If the mongo_ha gem has been loaded, then the connection to mongo is
    # automatically re-established and the job will resume anytime a
    # Mongo connection failure occurs.
    #
    # Thread-safe, can be called by multiple threads at the same time
    #
    # Parameters
    #   on_exception [Proc]
    #     Block of code to execute if an unhandled exception was raised during
    #     the processing of a slice
    def work(options={}, &block)
      raise 'Job must be started before calling #work' unless running?
      on_exception       = options.delete(:on_exception)
      # Not used. Always returns after this entire job has been processed
      processing_seconds = options.delete(:processing_seconds) || 0
      options.each { |option| raise ArgumentError.new("Unknown BatchJob::MultiRecord#work option: #{option.inspect}") }

      worker = klass.constantize.new
      worker.batch_job = job
      args = parameters['_parms']
      worker.send(method, *args)
    rescue Exception => exc
      set_exception(header, exc, record_number)
      on_exception.call(exc) if on_exception
    end

    # Returns the next job to work on in priority based order
    # Returns nil if there are currently no queued jobs, or processing batch jobs
    #   with records that require processing
    #
    # If a job is in queued state it will be started
    def self.next_job(server)
      #job = where(state: { :$in => ['queued', 'processing'] } ).order(:priority.desc).limit(1).first
      job = Job.where(state: 'queued').order(:priority.desc).limit(1).first
      job.start if job && job.pending?
      job

      find_and_modify(
        # (state = 'queued' AND assigned_to = nil) OR (state = 'processing')
        query: { state: { :$in => ['queued', 'processing'] } },
        sort: [['priority', 'asc'], ['created_at', 'asc']],
        update: { 'assigned_to' => name },
        new: true
      )

    end

    private

    # Set exception information for this job
    def set_exception(exc)
      self.server = nil
      self.failure_count += 1
      self.exception = {
        'class'         => exc.class.to_s,
        'message'       => exc.message,
        'backtrace'     => exc.backtrace || [],
        'server'        => Server.name,
      }
      fail!
    end

  end
end
