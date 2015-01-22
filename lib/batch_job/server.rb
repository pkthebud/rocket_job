# encoding: UTF-8
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

    # Unique Name of this server instance
    #   Defaults to the `hostname` but _must_ be overriden if mutiple Server instances
    #   are started on the same host
    # The unique name is used on re-start to re-queue any jobs that were being processed
    # at the time the server or host unexpectedly terminated, if any
    key :name,               String

    # Current state
    key :state,              Symbol, default: :available

    # The maximum number of worker threads
    #   If set, it will override the default value in Batch::Config
    key :max_threads,        Integer

    # When this server process was started
    key :started_at,         Time

    # Name of the host on which the server is currently running
    key :host_name,          String

    # Process ID of the server process
    key :pid,                Integer

    # The heartbeat information for this server
    one :heartbeat,          class_name: 'Batch::Heartbeat'

    # State Machine events and transitions
    #
    #   :available -> :paused      -> :available  ( manual )
    #              -> :stopped     -> :available  ( on restart )
    #

    # Start this server
    #   The same name name must be passed in every time to ensure proper
    #   recovery on re-start
    #
    #   name:
    #     Unique name to use for this Batch Server instance
    #     Default: `hostname`
    def self.start(name=nil)
      name ||= `hostname`
      server = where(name: 'name').first
      if server
        server.perform_recovery
        server.set(started_at: Time.now)
      else
        server = create(name: 'name', started_at: Time.now)
      end
      server
    end

    # Create indexes
    def self.create_indexes
      ensure_index [[:name, 1]], background: true, unique: true
    end

    # Check if their was a previous instance if this server running
    # If so, re-queue all of its jobs
    def perform_recovery
      Job.where()
    end

    # Returns the next job to work on in priority based order
    # Returns nil if there are currently no queued jobs, or processing batch jobs
    #   with records that require processing
    #
    # If a job is in queued state it will be started
    def next_job
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

  end
end

