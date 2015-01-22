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
      rescue Exception
        # In case another process has already created the first document
        first
      end
    end

    # The maximum number of worker threads to create on any one server
    key :max_worker_threads,         Integer, default: 10

    # Number of seconds between heartbeats from Batch Server processes
    key :server_heartbeat_seconds,   Integer, default: 5

    # Limit the number of workers per job class per server
    #    'class_name' / group => 100
    key :limits, Hash

  end
end
