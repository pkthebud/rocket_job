# encoding: UTF-8

# Mix-in to add Worker behavior to a class
#
# Optional methods that can be implemented by workers:
#   on_exception
#     Called whenever an exception is raised while processing that job
#
module BatchJob
  module Worker
    def self.included(base)
      base.extend(ClassMethods)
      base.send(:attr_accessor, :batch_job)
    end

    module ClassMethods
      def self.async_perform(*args)
        job = Single.new(
          klass: name,
          parameters: { '_parms' => args }
        )
        if BatchJob::Config.test_mode
          job.start
          job.work
        else
          job.start!
        end
      end

    end

    # Method that must be implemented to process the job
    def self.perform(*args)
      raise NotImplementedError.new('Must implement worker method #perform')
    end

  end
end
