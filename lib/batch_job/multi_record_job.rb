module BatchJob
  #
  # Multi-record jobs
  #
  # When jobs consists of multiple records that will be held in a separate
  # collection for processing
  class MultiRecordJob < Job
    # Number of records in this job
    #   Useful when updating the UI progress bar
    key :record_count,            Integer, default: 0

    # Whether to store results in a separate collection, or to discard any results
    # returned when records were processed
    key :collect_results,         Boolean, default: false

    after_destroy :cleanup_records

    validates_presence_of :record_count

    # State Machine events and transitions
    #
    # Usual processing:
    #   :queued -> :loading -> :processing -> :finishing -> :completed
    #

    # Returns [true|false] whether to collect the results from running this batch
    def collect_results?
      collect_results == true
    end

    # Returns each record available in the order it was added
    # until no more records are left for processing.
    # After each item has been processed without raisin an exception it is deleted
    # from the records queue
    # The result if not nil is written to the results collection
    #
    # If an exception was thrown, ...
    #
    # If a Mongo connection failure occurs the connection is automatically re-established
    # and the job will be retried ( possibly by another process )
    #
    # Thread-safe, can be called by multiple threads at the same time
    #
    # Parameters
    #   server_name
    #     A unqiue name for this server instance
    #     Should only have one server name per machine
    #     On startup all pending jobs with this 'server_name' will be retried
    def process_records(server_name, include_retries=false, &block)
      # find_and_modify is already in a retry block
      while record = records_collection.find_and_modify(
          query:  { 'server' => { :$exists => false }, retry_count: { :$exists => include_retries } },
          update: { server: server_name, started_at: Time.now },
          sort:   '_id'
          # full_response: true   returns the entire response object from the server including ‘ok’ and ‘lastErrorObject’.
        )
        begin
          result = block.call(record.delete('data'), record)
          results_collection.insert('_id' => record['_id'], 'data' => result) unless result.nil? && collect_results?
          # On successful completion delete the record from the job queue
          records_collection.remove('_id' => record['_id'])
        rescue Mongo::OperationFailure, Mongo::ConnectionFailure => exc
          logger.fatal "Going down due to unhandled Mongo Exception", exc
          raise
        rescue Exception => exc
          logger.error "Failed to process job", exc
          # Set failure information and increment retry count
          records_collection.update(
            { '_id' => record['_id'] },
            'server_name' => nil,
            'exception' => {
              'class'       => exc.class.to_s,
              'message'     => exc.message,
              'backtrace'   => exc.backtrace || [],
              'server_name' => server_name
            },
            'retry_count' => ( record['retry_count'] || 0 ) + 1
          )
        end
      end
    end

    # Returns the record id for the added record
    # Parameters
    #   `data` [ Hash | Array | String | Integer | Float | Symbol | Regexp | Time ]
    #     All elements in `data` must be serializable to BSON
    #     For example the following types are not supported: Date
    # Note:
    #   Not thread-safe. Only call from one thread at a time
    def <<(data)
      records_collection.insert('_id' => record_count + 1, 'data' => data)
      # Only increment record_count once the job has been saved
      self.record_count += 1
    end

    # Add many records for processing at the same time.
    # Returns [Range<Integer>] range of the record_ids that were added
    # Note:
    #   Not thread-safe. Only call from one thread at a time
    def add_records(records)
      count = record_count
      bulk  = records_collection.initialize_ordered_bulk_op
      records.each { |data| bulk.insert('_id' => (count += 1), 'data' => data) }
      bulk.execute

      result = (self.record_count + 1 .. count)
      self.record_count = count
      result
    end

    # Iterate over each record
    def each_record(&block)
      records_collection.find({}, sort: '_id', timeout: false) do |cursor|
        cursor.each { |record| block.call(record.delete('data'), record) }
      end
    end

    # Iterate over each result
    #   destructive
    #     Delete each record after it is successfully processed by the block
    def each_result(destructive=false, &block)
      results_collection.find({}, sort: '_id', timeout: false) do |cursor|
        cursor.each do |record|
          block.call(record.delete('data'), record)
          results_collection.remove(record['_id']) if destructive
        end
      end
    end

    # Returns the Mongo Collection for the records queue name
    def records_collection
      @records_collection ||= WorkingStorage::Work.with_collection("batch_job_records_#{tracking_number}")
    end

    # Returns the Mongo Collection for the records queue name
    def results_collection
      @results_collection ||= WorkingStorage::Work.with_collection("batch_job_results_#{tracking_number}")
    end

    # Returns [Integer] percent of records completed so far
    # Returns nil if the total record count has not yet been set
    def percent_complete
      return 100 if completed?
      return 0 unless record_count > 0
      ((results_collection.count.to_f / record_count) * 100).round
    end

    # Returns [true|false] whether the entire job has been completely processed
    # Useful for determining if the job is complete when in active state
    def processing_complete?
      active? && (record_count.to_i > 0) && (records_collection.count == 0) && (results_collection.count == record_count)
    end

    # Returns [Hash] status of this job
    def status(time_zone='EST')
      h = super(time_zone)
      case
      when running? || paused?
        h[:queued]           = records_collection.count,
        h[:processed]        = results_collection.count
        h[:record_count]     = record_count
        h[:rate_per_hour]    = ((results_collection.count / (Time.now - started_at)) * 60 * 60).round
        h[:percent_complete] = ((results_collection.count.to_f / record_count) * 100).to_i
      when completed?
        h[:rate_per_hour]    = ((record_count / h[:seconds]) * 60 * 60).round
        h[:status]           = "Completed processing #{record_count} record(s) at a rate of #{"%.2f" % h[:rate_per_hour]} records per hour at #{completed_at.in_time_zone(time_zone)}"
        h[:processed]        = record_count
        h[:record_count]     = record_count
      end
    end

    private

    # Drop the records collection
    def cleanup_records
      records_collection.drop
      results_collection.drop
    end
  end
end
