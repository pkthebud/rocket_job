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
      # Call a specific method as a rocket job worker
      # The job is immediately enqueued for processing by a worker
      # once the block has returned
      def later(method, *args, &block)
        job = build(method, *args, &block)
        if RocketJob::Config.inline_mode
          server = Server.new(name: 'inline')
          job.start
          while job.running?
            job.work(server)
          end
        else
          job.save!
        end
        job
      end

      # Build a Rocket Job instance that can be used to call a specific
      # method as a rocket job worker
      #
      # Note:
      #  - #save! must be called on the return job instance if it needs to be
      #    queued for processing.
      #  - If data is uploaded into the job instance before saving, and is then
      #    discarded, call #cleanup! to clear out any partially uploaded data
      def build(method, *args, &block)
        job = rocket_job_class.new(
          klass:          name,
          perform_method: method.to_sym,
          arguments: args
        )
        @rocket_job_defaults.call(job) if @rocket_job_defaults
        block.call(job) if block
        job
      end

      # Method to be performed later
      def perform_later(*args, &block)
        later(:perform, *args, &block)
      end

      # Method to be performed later
      def perform_build(*args, &block)
        build(:perform, *args, &block)
      end

      # Define job defaults
      def rocket_job(job_class=RocketJob::Job, &block)
        @rocket_job_class    = job_class
        @rocket_job_defaults = block
        self
      end

      # Returns the job class
      def rocket_job_class
        @rocket_job_class
      end
    end

    def rocket_job_csv_parser
      # TODO Change into an instance variable once CSV handling has been re-worked
      RocketJob::Utility::CSVRow.new
    end

  end
end
