# encoding: UTF-8
require 'socket'
require 'sync_attr'
module BatchJob
  # Server
  #
  # On startup a server instance will automatically register itself
  # if not already present
  #
  # Starting a server in the foreground:
  #   - Using a Rails runner:
  #     bin/rails r 'BatchJob::Server.start'
  #
  #   - Or, using a rake task:
  #     bin/rake batch_job:server
  #
  # Starting a server in the background:
  #   - Using a Rails runner:
  #     nohup bin/rails r 'BatchJob::Server.start' 2>&1 1>output.log &
  #
  #   - Or, using a rake task:
  #     nohup bin/rake batch_job:server 2>&1 1>output.log &
  #
  # Stopping a server:
  #   - Stop the server via the Web UI
  #   - Send a regular kill signal to make it shutdown once all active work is complete
  #       kill <pid>
  #   - Or, use the following Ruby code:
  #     server = BatchJob::Server.where(name: 'server name').first
  #     server.stop!
  #
  #   Sending the kill signal locally will result in starting the shutdown process
  #   immediately. Via the UI or Ruby code the server can take up to 30 seconds
  #   (the heartbeat interval) to start shutting down.
  #
  # Restarting a server:
  #   - Restart the server via the Web UI
  #   - Or, use the following Ruby code:
  #     server = BatchJob::Server.where(name: 'server name').first
  #     server.restart!
  #
  #   It can take up to 30 seconds (the heartbeat interval) before the server re-starts
  #
  #
  class Server
    include MongoMapper::Document
    include AASM
    include SyncAttr
    include SemanticLogger::Loggable

    # Unique Name of this server instance
    #   Defaults to the `hostname` but _must_ be overriden if mutiple Server instances
    #   are started on the same host
    # The unique name is used on re-start to re-queue any jobs that were being processed
    # at the time the server or host unexpectedly terminated, if any
    key :name,               String, default: -> { "#{Socket.gethostname}:#{$$}" }

    # The maximum number of worker threads
    #   If set, it will override the default value in BatchJob::Config
    key :max_threads,        Integer, default: -> { Config.instance.max_worker_threads }

    # When this server process was started
    key :started_at,         Time

    # The heartbeat information for this server
    one :heartbeat,          class_name: 'BatchJob::Heartbeat'

    # Number of seconds job workers will be requested to return after so that
    # jobs with a higher priority can interrupt current jobs
    #
    # Note:
    #   Not all job types support stopping in the middle
    key :re_check_seconds,   Integer, default: 900

    # Current state
    #   Internal use only. Do not set this field directly
    key :state,              Symbol, default: :starting

    # Current state
    #   Internal use only. Do not set this field directly
    key :daemon,             Boolean, default: false

    validates_presence_of :state, :name, :max_threads

    # States
    #   :starting -> :running -> :paused
    #                         -> :stopping
    aasm column: :state do
      state :starting, initial: true
      state :running
      state :paused
      state :stopping

      event :started do
        transitions from: :starting, to: :running
        before do
          self.started_at = Time.now
        end
      end
      event :pause do
        transitions from: :running, to: :paused
      end
      event :stop do
        transitions from: :running, to: :stopping
        transitions from: :paused,  to: :stopping
      end
    end

    attr_reader :threads

    def threads
      @threads ||=[]
    end

    # Run the server process
    # Attributes supplied are passed to #new
    def self.run(attrs={})
      server = new(attrs)
      server.build_heartbeat
      server.save!
      create_indexes
      register_signal_handlers

      # If not a daemon also log messages to stdout
      SemanticLogger.add_appender(STDOUT,  &SemanticLogger::Appender::Base.colorized_formatter) unless server.daemon?
      server.run
    end

    # Create indexes
    def self.create_indexes
      ensure_index [[:name, 1]], background: true, unique: true
      # Also create indexes for the jobs collection
      Single.create_indexes
    end

    # Run this instance of the server
    def run
      # Apply instance specific logger to include server name with all log entries
      logger = SemanticLogger["#{self.class.name} #{name}"]

      Thread.current.name = 'BatchJob.run'
      adjust_threads
      started!
      logger.info "BatchJob Server started with #{max_threads} workers running"

      count = 0
      loop do
        # Update heartbeat so that monitoring tools know that this server is alive
        set(
          'heartbeat.updated_at'      => Time.now,
          'heartbeat.current_threads' => thread_pool_count
        )

        # Reload the server model every 10 heartbeats in case its config was changed
        # TODO make 3 configurable
        if count >= 3
          # Reload clears out instance variables too
          t = @threads
          reload
          @threads = t
          adjust_threads
          count = 0
        else
          count += 1
        end

        # Stop server if shutdown signal was raised
        stop! if @@shutdown && !stopping?

        break if stopping?

        sleep Config.instance.heartbeat_seconds
      end
      logger.debug 'Waiting for worker threads to stop'
      threads.join
      logger.debug 'Shutdown'
    rescue Exception => exc
      logger.error('BatchJob::Server is stopping due to an exception', exc)
    ensure
      # Destroy this server instance
      destroy
    end

    def thread_pool_count
      threads.count{ |i| i.alive? }
    end

    protected

    def next_worker_id
      @worker_id ||= 0
      @worker_id += 1
    end

    # Re-adjust the number of running threads to get it up to the
    # required number of threads
    def adjust_threads
      count = thread_pool_count
      # Cleanup threads that have stopped
      if count != threads.count
        logger.info "Cleaning up #{threads.count - count} threads that went away"
        threads.delete_if { |t| !t.alive? }
      end

      # Need to add more threads?
      if count < max_threads
        thread_count = max_threads - count
        logger.info "Starting #{thread_count} threads"
        thread_count.times.each do
          # Start worker thread
          threads << Thread.new(next_worker_id) do |id|
            begin
              worker(id)
            rescue Exception => exc
              logger.fatal('Cannot start worker thread', exc)
            end
          end
        end
      end
    end

    # Keep processing jobs until server stops running
    def worker(worker_id)
      Thread.current.name = "BatchJob Worker #{worker_id}"
      logger.debug 'Started'
      loop do
        if job = next_job
          logger.tagged(job.id.to_s) do
            job.work(self)
          end
        else
          # TODO Use exponential back-off algorithm
          sleep BatchJob::Config.instance.max_poll_interval
        end
        break if @@shutdown || !running?
      end
      logger.debug "Stopping. Server state: #{state.inspect}"
    rescue Exception => exc
      logger.fatal('Unhandled exception in job processing thread', exc)
    end

    # Returns the next job to work on in priority based order
    # Returns nil if there are currently no queued jobs, or processing batch jobs
    #   with records that require processing
    #
    # If a job is in queued state it will be started
    def next_job
      query = {
        '$or' => [
          # Single Jobs
          { 'state' => 'queued' },
          # MultiRecord Jobs available for additional workers
          { 'state' => 'running', 'sub_state' => :processing }
        ]
      }

      if doc = Single.find_and_modify(
          query:  query,
          sort:   [['priority', 'asc'], ['created_at', 'asc']],
          update: { '$set' => { 'server' => self.name, 'state' => 'running' } }
        )
        job = Single.load(doc)
        # Also update in-memory state and run call-backs
        job.start unless job.running?
        job
      end
    end

    @@shutdown = false

    # Register handlers for the various signals
    # Term:
    #   Perform clean shutdown
    #
    def self.register_signal_handlers
      begin
        Signal.trap "SIGTERM" do
          @@shutdown = true
          logger.warn "Shutdown signal (SIGTERM) received. Will shutdown as soon as active jobs/slices have completed."
        end

        Signal.trap "INT" do
          @@shutdown = true
          logger.warn "Shutdown signal (INT) received. Will shutdown as soon as active jobs/slices have completed."
        end
      rescue Exception
        logger.warn "SIGTERM handler not installed. Not able to shutdown gracefully"
      end
    end

  end
end

