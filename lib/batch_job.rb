# encoding: UTF-8
require 'mongo'
require 'mongo_ha'
require 'mongo_mapper'
require 'semantic_logger'
require 'symmetric-encryption'
require 'batch_job/version'

module BatchJob
  autoload :Config,                'batch_job/config'
  autoload :Heartbeat,             'batch_job/heartbeat'
  autoload :Single,                'batch_job/single'
  autoload :MultiRecord,           'batch_job/multi_record'
  autoload :Server,                'batch_job/server'
  autoload :Worker,                'batch_job/worker'
  module Reader
    autoload :Zip,                 'batch_job/reader/zip'
  end
  module Utility
    autoload :CSVRow,              'batch_job/utility/csv_row'
  end
  module Writer
    autoload :Zip,                 'batch_job/writer/zip'
  end

  UTF8_ENCODING = Encoding.find("UTF-8").freeze

  # Replace the MongoMapper default mongo connection for holding jobs
  def self.set_mongo_connection(connection)
    Single.connection(connection)
  end

  # Replace the MongoMapper default mongo connection for holding working data.
  # For example, blocks, records, etc.
  def self.set_mongo_work_connection(connection)
    BatchJob::MultiRecord.work_connection = connection
  end

  # Ensure that the necessary indexes exist for Batch Jobs
  # If a non-default connection is being used, be sure to call `set_mongo_connection`
  # and/or `set_mongo_work_connection` prior to calling `create_indexes`
  def self.create_indexes
    BatchJob::Single.create_indexes
  end
end
