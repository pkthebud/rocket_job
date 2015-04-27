# encoding: UTF-8

# Mix-in to add Worker behavior to a class
module RocketJob
  module Worker
    def self.included(base)
      base.extend(ClassMethods)
      base.class_eval do
        include SemanticLogger::Loggable
        attr_accessor :rocket_job, :rocket_job_slice
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
          arguments:      args
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

    # Calls a method on this worker, if it is defined
    # Adds the event name to the method call if supplied
    #
    # Returns [Object] the result of calling the method
    #
    # Parameters
    #   method [Symbol]
    #     The method to call on this worker
    #
    #   arguments [Array]
    #     Arguments to pass to the method call
    #
    #   Options:
    #     event: [Symbol]
    #       Any one of: :before, :after
    #       Default: None, just calls the method itself
    #
    #     log_level: [Symbol]
    #       Log level to apply to silence logging during the call
    #       Default: nil ( no change )
    #
    def rocket_job_call(method, arguments, options={})
      options               = options.dup
      event                 = options.delete(:event)
      log_level             = options.delete(:log_level)
      options.each { |option| raise ArgumentError.new("Unknown RocketJob::Worker#rocket_job_call option: #{option.inspect}") }

      the_method = event.nil? ? method : "#{event}_#{method}".to_sym
      if respond_to?(the_method)
        method_name = "#{self.class.name}##{the_method}"
        logger.info "Start #{method_name}"
        logger.benchmark_info("Completed #{method_name}",
          metric:             "rocket_job/#{self.class.name.underscore}/#{the_method}",
          log_exception:      :full,
          on_exception_level: :error,
          silence:            log_level
        ) do
          self.send(the_method, *arguments)
        end
      end
    end

  end
end
