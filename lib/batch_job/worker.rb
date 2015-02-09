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
      def async(method, *args)
        job = Single.new(
          klass: name,
          method: method.to_sym,
          parameters: { '_params' => args }
        )
        if BatchJob::Config.test_mode
          job.start
          job.work
        else
          job.save!
        end
        job
      end

      def async_perform(*args)
        async(:perform, *args)
      end

    end

    # Method that must be implemented to process the job
    def self.perform(*args)
      raise NotImplementedError.new('Must implement worker method #perform')
    end

  end
end
