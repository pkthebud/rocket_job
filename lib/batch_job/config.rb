# encoding: UTF-8
module BatchJob
  # Centralized Configuration for Batch Jobs
  class Config
    include MongoMapper::Document
    include SyncAttr

    # Returns the single instance of the Batch Configuration
    # in a thread-safe way
    sync_cattr_reader(:instance) do
      begin
        first || create
      rescue Exception => exc
        # In case another process has already created the first document
        first
      end
    end

    # By enabling test_mode jobs will be called in-line
    # No server processes will be created, nor threads created
    sync_cattr_accessor(:test_mode) { false }

    # The maximum number of worker threads to create on any one server
    key :max_worker_threads,         Integer, default: 10

    # Number of seconds between heartbeats from Batch Server processes
    key :heartbeat_seconds,          Integer, default: 5

    # Maximum number of seconds between checks for new jobs
    key :max_poll_interval,          Integer, default: 5

    # Limit the number of workers per job class per server
    #    'class_name' / group => 100
    key :limits, Hash

  end
end
