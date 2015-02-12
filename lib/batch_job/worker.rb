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
      base.include(SemanticLogger::Loggable)
      base.send(:attr_accessor, :batch_job)
    end

    module ClassMethods
      def later(method, *args, &block)
        job = if block
          j = MultiRecord.new(
            klass:     name,
            method:    method.to_sym,
            arguments: args
          )
          block.call(j)
          j
        else
          Single.new(
            klass:     name,
            method:    method.to_sym,
            arguments: args
          )
        end
        if BatchJob::Config.test_mode
          job.start
          job.work
        else
          job.save!
        end
        job
      end

      def perform_later(*args, &block)
        later(:perform, *args, &block)
      end

    end

    # Method that must be implemented to process the job
    def self.perform(*args)
      raise NotImplementedError.new('Must implement worker method #perform')
    end

  end
end
