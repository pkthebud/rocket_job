require 'rocket_job'
module Workers
  class SlicedJob
    include RocketJob::Worker

    # Job Defaults
    rocket_job(RocketJob::SlicedJob) do |job|
      job.description         = 'Hello World'
      job.collect_output      = true
      job.repeatable          = true
      job.destroy_on_complete = false
    end

    def perform(record)
      record
    end

    def reverse(record)
      record.reverse
    end

    def oh_no(record)
      raise 'Oh no'
    end

    def able(record)
      record
    end

    # Raise an exception the first time it is called
    def after_able
      if rocket_job.failure_count > 0
        0
      else
        raise 'After exception'
      end
    end

    # Raise an exception the first time it is called
    def before_probable
      if rocket_job.failure_count > 0
        0
      else
        raise 'Before exception'
      end
    end

    def probable(record)
      record
    end

    def before_event(hash)
      hash['before_event'] = true
    end

    def event(hash, record)
      raise "Missing slice" unless hash['before_event']
      record
    end

    def after_event(hash)
      hash['after_event'] = true
    end

  end
end