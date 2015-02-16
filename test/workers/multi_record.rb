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


    def before_event(hash)
      hash['before_event'] = true
    end

    def event(hash, record, header)
      raise "Missing header" unless hash['before_event']
      record
    end

    def after_event(hash)
      hash['after_event'] = true
    end

  end
end