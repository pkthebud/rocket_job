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
end
