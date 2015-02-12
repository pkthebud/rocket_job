require 'batch_job'
module Workers
  class Single
    include BatchJob::Worker
    @@result = nil

    # For holding test results
    def self.result
      @@result
    end

    def perform(first)
      @@result = first + 1
    end

    def sum(a, b)
      @@result = a + b
    end

    # Test silencing noisy logging
    def noisy_logger
      logger.info 'some very noisy logging'
    end

    # Test increasing log level for debugging purposes
    def debug_logging
      logger.trace 'enable tracing level for just the job instance'
    end

  end
end