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
    include SyncAttr
    include SemanticLogger::Loggable

    # Unique Instance name for this process
    # Default: host_name
    sync_cattr_accessor(:name) { "#{Socket.gethostname}" }

    # Returns [BatchJob::Server] the unique server instance for this process
    sync_cattr_reader(:server) do
      create_indexes
      server = where(name: name).first
      if server
        server.send(:perform_recovery)
        server.set(started_at: Time.now)
      else
        server = new(name: name, started_at: Time.now)
        server.build_heartbeat
        server.save!
      end
      register_signal_handlers
      self.state = :running
      server
    end

    # Current state for the server
    # Default: :starting
    #    sync_cattr_accessor(:state) { :starting }
    # Signal handlers don't allow the use of mutex's
    @@state = :starting
    def self.state
      @@state
    end

    def self.state=(state)
      @@state = state
    end

    # Unique Name of this server instance
    #   Defaults to the `hostname` but _must_ be overriden if mutiple Server instances
    #   are started on the same host
    # The unique name is used on re-start to re-queue any jobs that were being processed
    # at the time the server or host unexpectedly terminated, if any
    key :name,               String

    # Current state
    key :state,              Symbol, default: :available

    # The maximum number of worker threads
    #   If set, it will override the default value in BatchJob::Config
    key :max_threads,        Integer, default: 3

    # When this server process was started
    key :started_at,         Time

    # Name of the host on which the server is currently running
    # Useful for when the name of the server was set explicitly
    key :host_name,          String

    # Process ID of the server process
    key :pid,                Integer

    # The heartbeat information for this server
    one :heartbeat,          class_name: 'BatchJob::Heartbeat'

    # State Machine events and transitions
    #
    #   :available -> :paused      -> :available  ( manual )
    #              -> :stopped     -> :available  ( on restart )
    #

    # Run the server process
    # Parameters
    #   server_name
    #     The same name must be passed in every time to ensure proper
    #     recovery on re-start
    def self.run(server_name=nil, daemon=true)
      self.name = server_name if server_name
      Thread.current.name = 'BatchJob.run'

      # If not a daemon log info level messages to stdout
      SemanticLogger.add_appender(STDOUT, :info,  &SemanticLogger::Appender::Base.colorized_formatter) unless daemon

      # Start worker threads
      threads = server.max_threads.times.collect do |i|
        Thread.new(server, i) do |server, i|
          server.send(:process_jobs, i)
        end
      end

      logger.info "BatchJob Server started with #{server.max_threads} workers running"

      loop do
        sleep Config.instance.heartbeat_seconds

        # Update heartbeat so that monitoring tools know that this server is alive
        server.set('heartbeat.updated_at' => Time.now)

        # Reload the server model every 5 minutes in case its config was changed
        # server.reload

        break if shutting_down?
      end
      logger.debug 'Waiting for worker threads to stop'
      threads.join
      logger.debug 'Shutdown'
    rescue Exception => exc
      logger.error('BatchJob::Server is stopping due to an exception', exc)
    end

    # Create indexes
    def self.create_indexes
      ensure_index [[:name, 1]], background: true, unique: true
      # Also create indexes for the jobs collection
      Single.create_indexes
    end

    # Is the server shutting down?
    def self.shutting_down?
      state == :shutdown
    end

    protected

    # Check if their was a previous instance of this server running
    # If so, re-queue all of its jobs
    def perform_recovery
      # TODO re-queue previous jobs
      #Single.where()
    end

    # Keep process jobs until the shutdown semaphore is set
    def process_jobs(id)
      Thread.current.name = "BatchJob::Server.process_jobs##{id}"
      logger.debug 'Started'
      loop do
        if job = self.class.next_job
          job.work
        else
          # TODO Use exponential back-off algorithm
          sleep BatchJob::Config.instance.max_poll_interval
        end
        break if self.class.shutting_down?
      end
      logger.debug 'Stopping thread due to shutdown request'
    rescue Exception => exc
      logger.fatal('Unhandled exception in job processing thread', exc)
    end

    # Returns the next job to work on in priority based order
    # Returns nil if there are currently no queued jobs, or processing batch jobs
    #   with records that require processing
    #
    # If a job is in queued state it will be started
    def self.next_job
      query = {
        '$or' => [
          # Single Jobs
          { 'state' => 'queued' },
          # MultiRecord Jobs available for additional workers
          { 'state' => 'running', 'parallel' => true }
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

    # Register handlers for the various signals
    # Term:
    #   Perform clean shutdown
    #
    def self.register_signal_handlers
      begin
        Signal.trap "SIGTERM" do
          Server.state = :shutdown
          logger.warn "Shutdown signal (SIGTERM) received. Will shutdown as soon as active jobs/slices have completed."
        end

        Signal.trap "INT" do
          Server.state = :shutdown
          logger.warn "Shutdown signal (INT) received. Will shutdown as soon as active jobs/slices have completed."
        end
      rescue Exception
        logger.warn "SIGTERM handler not installed. Not able to shutdown gracefully"
      end
    end

  end
end

