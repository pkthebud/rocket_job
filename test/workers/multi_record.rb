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

    def able(record, header)
      record
    end

    # Raise an exception the first time it is called
    def after_able
      if batch_job.failure_count > 0
        0
      else
        raise 'After exception'
      end
    end

    # Raise an exception the first time it is called
    def before_probable
      if batch_job.failure_count > 0
        0
      else
        raise 'Before exception'
      end
    end

    def probable(record, header)
      record
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