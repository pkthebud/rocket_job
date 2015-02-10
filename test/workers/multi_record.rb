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

    def oh_no(record, header)
      raise 'Oh no'
    end

  end
end