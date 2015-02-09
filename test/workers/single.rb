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

  end
end