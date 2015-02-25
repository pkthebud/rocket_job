# encoding: UTF-8

# Mix-in to add Worker behavior to a class
#
# Optional methods that can be implemented by workers:
#   on_exception
#     Called whenever an exception is raised while processing that job
#
module RocketJob
  module Worker
    def self.included(base)
      base.extend(ClassMethods)
      base.class_eval do
        include SemanticLogger::Loggable
        attr_accessor :rocket_job
        @rocket_job_class    = RocketJob::Job
        @rocket_job_defaults = nil
      end
    end

    module ClassMethods
      # Call a specific method as a batch worker
      def later(method, *args, &block)
        job = rocket_job_class.new(
          klass:     name,
          method:    method.to_sym,
          arguments: args
        )
        job.instance_eval(&@rocket_job_defaults) if @rocket_job_defaults
        block.call(job) if block
        if RocketJob::Config.test_mode
          job.start
          job.work
        else
          job.save!
        end
        job
      end

      # Method to be performed later
      def perform_later(*args, &block)
        later(:perform, *args, &block)
      end

      # Define job defaults
      def rocket_job(job_class=RocketJob::Job, &block)
        puts "rocket job for class: #{job_class.name}"
        @rocket_job_class    = job_class
        @rocket_job_defaults = block
        self
      end

      # Returns the job class
      def rocket_job_class
        @rocket_job_class
      end
    end
  end
end
