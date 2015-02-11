module BatchJob
  module Jobs
    # # Load a file for processing by the cluster of workers
    # filename = 'large_file.zip'
    # job = BatchJob::Jobs::PerformanceJob.upload(file_name, :perform)
    #
    # # Watch the status util it completes
    # loop { p job.reload.status; break if job.completed?; sleep 5}
    #
    # # Download the results into a Zip file
    # BatchJob::Writer::Zip.output_file('myfile.zip', 'data.csv') do |file|
    #   job.output_stream(file)
    # end
    #
    # # Destroy the job and its output data
    # job.destroy
    #
    #
    # # Results from running a test with a Zipped 670,000 line CSV file
    # # on a Mac Air i7 dual core, with Ruby MRI and JRuby
    #
    #                      Ruby MRI V2.2.0            JRuby 1.7.9
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
    # BatchJob::Single.destroy_all
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
      include BatchJob::Worker
      # # Load a file for processing by the cluster of workers
      # filename = 'large_file.zip'
      # job = BatchJob::Jobs::PerformanceJob.upload(file_name, :perform)
      #
      def self.upload(file_name, method = :perform)
        start_time = Time.now
        batch_job = later(method) do |job|
          job.destroy_on_complete = false
          job.encrypt             = true
          job.compress            = true
          job.description         = "Performance Test"
          job.slice_size          = 100
          job.collect_output      = true
          # Higher priority in case something else is also running
          job.priority            = 5

          # Upload the file into Mongo
          if file_name.ends_with?('.zip')
            BatchJob::Reader::Zip.input_file(file_name) do |io, source|
              job.input_stream(io)
            end
          else
            File.open(file_name, 'rt') do |io|
              job.input_stream(io)
            end
          end
        end
        puts "Loaded #{file_name} in #{Time.now - start_time} seconds"
        batch_job
      end

      # No operation, just return the supplied line (record)
      def perform(line, header)
        line
      end

      # Parse supplied line with custom CSV and then just convert back to a CSV string
      def custom_csv(line, header)
        csv_parser = BatchJob::Utility::CSVRow.new
        csv_parser.to_csv(csv_parser.parse(line))
      end

      # Parse supplied line with Regular CSV and then just convert back to a CSV string
      def ruby_csv(line, header)
        return CSV.parse_line(line).to_csv(row_sep: '')
      end
    end

  end
end
