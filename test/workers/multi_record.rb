require 'batch_job'
module Workers
  class MultiRecord
    include BatchJob::Worker

    def perform(record, header)
      record
    end

    def reverse(record, header)
      record.reverse
    end

  end
end