module RocketJob
  module Jobs
    # # Load a file for processing by the cluster of workers
    # job = RocketJob::Jobs::PerformanceJob.upload('large_file.zip')
    #
    # # Watch the status until it completes
    # loop { p job.reload.status; break if job.completed?; sleep 1 }
    #
    # # Once completed, download the output into a zip file
    # job.output.download('output.zip')
    #
    # # Destroy the job and its output data
    # job.destroy
    #
    # # Results from running a test with a Zipped 670,000 line CSV file
    # # on a Mac Air i7 dual core, with Ruby MRI and JRuby
    #
    #                    Ruby MRI V2.2.0            JRuby 1.7.9
    #   upload time:       9s                        15s
    #   perform:          19s  128,120,097/hr        12s   201,584,140/hr
    #   custom_csv:      124s   19,622,109/hr        40s    60,623,399/hr
    #   ruby_csv:        212s   11,441,162/hr        57s    42,283,743/hr
    #   download time:     6s                        8s
    #
    # # All data was compressed and encrypted
    # # Local MongoDB V2.6.7
    #
    # # The following was run between every run to clear out mongo data
    # RocketJob::Job.destroy_all
    #
    # # JRuby was run on Oracle Java V1.8.0_31
    # # using the following java options:
    # export JAVA_OPTS="-server -Xms512m -Xmx4096m -Xss2048k -Dfile.encoding=UTF-8 -XX:+UseG1GC -XX:+UseCodeCacheFlushing -XX:G1HeapRegionSize=4m -Djruby.compile.invokedynamic=false"
    #
    # # Mongo stats per second while the workers were running:
    # insert  query update delete getmore command flushes mapped  vsize    res faults
    #    576    576    576    576       0  1153|0       0  2.58g  7.62g   474m      0
    #
    class PerformanceJob
      include RocketJob::Worker

      # Define the job's default attributes
      rocket_job(RocketJob::SlicedJob) do |job|
        job.destroy_on_complete = false
        job.encrypt             = true
        job.compress            = true
        job.description         = "Performance Test"
        job.slice_size          = 100
        job.collect_output      = true
        # Higher priority in case something else is also running
        job.priority            = 5
      end

      # Load a file for processing by the cluster of workers
      def self.upload(file_name, method = :perform)
        start_time = Time.now
        rocket_job = later(method) do |job|
          job.upload(file_name)
        end
        puts "Loaded #{file_name} in #{Time.now - start_time} seconds"
        rocket_job
      end

      # Submit numbers for processing, with one number per record
      #
      # Example:
      #   # Submits 1,000,000 numbers for processing by the workers
      #   job = RocketJob::Jobs::PerformanceJob.number_test
      def self.number_test(count=1000000, method = :perform)
        start_time = Time.now
        rocket_job = later(method) do |job|
          start = 1
          job.upload_records do
            start += 1 if start <= count
          end
        end
        puts "Loaded #{count} records in #{Time.now - start_time} seconds"
        rocket_job
      end

      # No operation, just return the supplied line (record)
      def perform(line)
        line
      end

      # Parse supplied line with custom CSV and then just convert back to a CSV string
      def custom_csv(line)
        csv_parser.to_csv(csv_parser.parse(line))
      end

      # Parse supplied line with Regular CSV and then just convert back to a CSV string
      def ruby_csv(line)
        CSV.parse_line(line).to_csv(row_sep: '')
      end

      if defined?(JRuby)
        # Due a JRuby optimization issue we cannot re-use the same instance
        def csv_parser
          RocketJob::Utility::CSVRow.new
        end
      else
        def csv_parser
          @csv_parser ||= RocketJob::Utility::CSVRow.new
        end
      end
    end

  end
end
