require_relative 'test_helper'
require_relative 'workers/sliced_job'

# Unit Test for RocketJob::SlicedJob
class SlicedJobTest < Minitest::Test
  context RocketJob::SlicedJob do
    setup do
      RocketJob::Job.destroy_all
      @server = RocketJob::Server.new(name: 'test')
      @server.started
      @lines = [
        'this is some',
        'data',
        #        '',
        'a',
        'that we can delimit',
        'as necessary'
      ]
      @description = 'Hello World'
    end

    teardown do
      @job.destroy if @job && !@job.new_record?
    end

    context '.rocket_job' do
      should 'set defaults' do
        @job = Workers::SlicedJob.perform_later
        assert_equal @description, @job.description
        assert_equal true, @job.collect_output?
        assert_equal true, @job.repeatable
        assert_equal false, @job.destroy_on_complete
      end
    end

    context '#status' do
      should 'return status for a queued job' do
        @job = Workers::SlicedJob.perform_later
        assert_equal true, @job.queued?
        h = @job.status
        assert_equal :queued,      h[:state]
        assert_equal @description, h[:description]
      end
    end

    context '#upload_slice' do
      should 'upload slices' do
        @job = Workers::SlicedJob.perform_later
        @lines.each { |line| @job.upload_slice([line]) }

        assert_equal @lines.size, @job.record_count
        assert_equal @lines.size, @job.input.count
        assert_equal @lines.size, @job.input.queued_count
        assert_equal 0,           @job.input.active_count
        assert_equal 0,           @job.input.failed_count
      end
    end

    context '#upload_records' do
      should 'support slice size of 1' do
        lines = @lines.dup
        count = 0
        @job = Workers::SlicedJob.perform_later do |job|
          # Override default slice size
          job.slice_size = 1
          count = job.upload_records { lines.shift }
        end
        assert_equal @lines.size, count
        assert_equal @lines.size, @job.record_count
        assert_equal @lines.size, @job.input.count
        assert_equal @lines.size, @job.input.queued_count
        assert_equal 0,           @job.input.active_count
        assert_equal 0,           @job.input.failed_count
      end

      should 'support slice size of 2' do
        lines = @lines.dup
        count = 0
        @job = Workers::SlicedJob.perform_later do |job|
          # Override default slice size
          job.slice_size = 2
          count = job.upload_records { lines.shift }
        end
        slice_count = (0.5 + @lines.size.to_f / 2).to_i

        assert_equal @lines.size, count
        assert_equal @lines.size, @job.record_count
        assert_equal slice_count, @job.input.count
        assert_equal slice_count, @job.input.queued_count
        assert_equal 0,           @job.input.active_count
        assert_equal 0,           @job.input.failed_count
      end
    end

    context '#work' do
      teardown do
        @job.destroy if @job
      end

      should 'read all records' do
        @job = Workers::SlicedJob.perform_later do |job|
          assert_equal nil, job.record_count
          # slice_size has no effect since it calling #write_slice directly
          job.slice_size = 1
          @lines.each { |record| job.upload_slice([record]) }
        end
        @job.start!

        assert_equal @lines.size, @job.record_count
        assert_equal @lines.size, @job.input.count
        assert_equal @lines.size, @job.input.queued_count
        assert_equal 0,           @job.input.active_count
        assert_equal 0,           @job.input.failed_count

        count = 0
        @job.work(@server)
        failures = []
        @job.input.each_failed_record { |r, slice| failures << { slice: slice, record: r } }
        assert_equal 0, @job.input.failed_count, failures
        @job.output.each do |slice|
          slice.each do |record|
            assert_equal @lines[count], record
            count += 1
          end
        end

        assert_equal @lines.size, count
        assert_equal @lines.size, @job.record_count
        assert_equal 0,           @job.input.count
        assert_equal 0,           @job.input.queued_count
        assert_equal 0,           @job.input.active_count
        assert_equal 0,           @job.input.failed_count
        assert_equal true, @job.completed?
        assert_equal @lines.size, @job.output.count
        RocketJob::Job.find(@job.id)
      end

      should 'destroy on completion' do
        @job = Workers::SlicedJob.perform_later do |job|
          assert_equal nil, job.record_count
          job.destroy_on_complete = true
          job.slice_size = 1
          @lines.each { |row| job.upload_slice([row]) }
        end
        @job.start!

        assert_equal @lines.size, @job.record_count

        count = 0
        assert_equal false, @job.work(@server)
        failures = []
        @job.input.each_failed_record { |r, slice| failures << { slice: slice, record: r } }
        assert_equal 0, @job.input.failed_count, failures
        @job.output.each do |slice|
          slice.each do |record|
            count += 1
          end
        end
        assert_equal 0, @job.input.failed_count
        assert_equal true, @job.completed?
        assert_equal 0, @job.output.count
        assert_equal 0, @job.output.count
        assert_equal nil, RocketJob::Job.where(id: @job.id).first
      end

      should 'retry on exception' do
        @job = Workers::SlicedJob.later(:oh_no) do |job|
          job.slice_size = 1
          @lines.each { |row| job.upload_slice([row]) }
        end
        @job.start!

        assert_equal @lines.size, @job.record_count

        # New jobs should fail
        assert_equal false, @job.work(@server)
        assert_equal @lines.size, @job.input.failed_count
        assert_equal 0, @job.output.count
        assert_equal 0, @job.input.queued_count
        assert_equal 0, @job.input.active_count
        assert_equal false, @job.completed?
        # Since all slices have failed, it should automatically transition to failed
        assert_equal true, @job.failed?

        # Should not process failed jobs
        @job.retry!
        assert_equal false, @job.work(@server)
        assert_equal @lines.size, @job.input.failed_count
        assert_equal 0, @job.output.count
        assert_equal 0, @job.input.queued_count
        assert_equal 0, @job.input.active_count
        assert_equal false, @job.completed?

        # Make records available for processing again
        assert_equal @lines.size, @job.input.requeue_failed
        assert_equal 0, @job.input.failed_count, @job.input.first.inspect

        # Re-process the failed jobs
        @job.perform_method = :perform
        @job.retry!
        assert_equal false, @job.work(@server)
        failures = []
        @job.input.each_failed_record { |r, slice| failures << { slice: slice, record: r } }
        assert_equal 0, @job.input.failed_count, failures
        assert_equal @lines.size, @job.output.count
        assert_equal 0, @job.input.queued_count
        assert_equal 0, @job.input.active_count
        assert_equal nil, @job.sub_state
        assert_equal true, @job.completed?
      end

      should 'call before_event' do
        named_parameters = { 'counter' => 23 }
        @job = Workers::SlicedJob.later(:event, named_parameters) do |job|
          job.slice_size = 1
          @lines.each { |row| job.upload_slice([row]) }
        end
        @job.start!

        @job.reload
        assert_equal named_parameters, @job.arguments.first
        assert_equal @lines.size, @job.record_count

        assert_equal false, @job.work(@server)
        assert_equal true, @job.completed?, @job.inspect
        assert_equal named_parameters.merge('before_event' => true, 'after_event' => true), @job.arguments.first
        assert_equal nil, @job.sub_state

        failures = []
        @job.input.each_failed_record { |r, slice| failures << { slice: slice, record: r } }
        assert_equal 0, @job.input.failed_count, failures
        assert_equal @lines.size, @job.output.count
        assert_equal 0, @job.input.queued_count
        assert_equal 0, @job.input.active_count
        assert_equal true, @job.completed?
      end

      should 'retry after an after_perform exception' do
        @job = Workers::SlicedJob.later(:able) do |job|
          job.slice_size = 1
          @lines.each { |row| job.upload_slice([row]) }
        end
        @job.start!

        assert_equal @lines.size, @job.record_count

        # New jobs should fail
        assert_equal false, @job.work(@server)
        failures = []
        @job.input.each_failed_record { |r, slice| failures << { slice: slice, record: r } }
        assert_equal 0, @job.input.failed_count
        assert_equal @lines.size, @job.output.count
        assert_equal 0, @job.input.queued_count
        assert_equal 0, @job.input.active_count
        assert_equal true, @job.failed?
        assert_equal :after, @job.sub_state

        # Make records available for processing again
        @job.retry!

        # Re-process the failed job
        assert_equal false, @job.work(@server)
        failures = []
        @job.input.each_failed_record { |r, slice| failures << { slice: slice, record: r } }
        assert_equal 0, @job.input.failed_count, failures
        assert_equal @lines.size, @job.output.count
        assert_equal 0, @job.input.queued_count
        assert_equal 0, @job.input.active_count
        assert_equal true, @job.completed?, @job.state
        assert_equal nil, @job.sub_state
      end

      should 'retry after a before_perform exception' do
        @job = Workers::SlicedJob.later(:probable) do |job|
          job.slice_size = 1
          @lines.each { |row| job.upload_slice([row]) }
        end
        @job.start!

        assert_equal @lines.size, @job.record_count

        # New jobs should fail
        assert_equal false, @job.work(@server)
        assert_equal 0, @job.input.failed_count
        assert_equal 0, @job.output.count
        assert_equal @lines.size, @job.input.queued_count
        assert_equal 0, @job.input.active_count
        assert_equal true, @job.failed?
        assert_equal :before, @job.sub_state

        # Make records available for processing again
        @job.retry!

        # Re-process the failed job
        assert_equal false, @job.work(@server)
        failures = []
        @job.input.each_failed_record { |r, slice| failures << { slice: slice, record: r } }
        assert_equal 0, @job.input.failed_count, failures
        assert_equal @lines.size, @job.output.count
        assert_equal 0, @job.input.queued_count
        assert_equal 0, @job.input.active_count
        assert_equal nil, @job.sub_state
        assert_equal true, @job.completed?, @job.state
      end

      should "backoff when throttle exceeded" do
        @job = Workers::SlicedJob.perform_later do |job|
          assert_equal nil, job.record_count
          # slice_size has no effect since it is calling #write_slice directly
          job.slice_size = 1
          @lines.each { |record| job.upload_slice([record]) }
        end
        @job.start!

        assert_equal @lines.size, @job.record_count
        assert_equal @lines.size, @job.input.count
        assert_equal @lines.size, @job.input.queued_count
        assert_equal 0,           @job.input.active_count
        assert_equal 0,           @job.input.failed_count
        assert_equal nil,         @job.max_active_workers
        assert_equal false,       @job.throttle_exceeded?

        # Mark all slices as running
        @job.input.each { |s| s.start; @job.input.update(s) }
        assert_equal false, @job.throttle_exceeded?
        @job.max_active_workers = 2
        assert_equal true, @job.throttle_exceeded?

        assert_equal true, @job.work(@server)
        assert_equal @lines.size, @job.input.count
      end

      should "backoff when no work available" do
        @job = Workers::SlicedJob.perform_later do |job|
          assert_equal nil, job.record_count
          # slice_size has no effect since it is calling #write_slice directly
          job.slice_size = 1
          @lines.each { |record| job.upload_slice([record]) }
        end
        @job.start!

        assert_equal @lines.size, @job.record_count
        assert_equal @lines.size, @job.input.count
        assert_equal @lines.size, @job.input.queued_count
        assert_equal 0,           @job.input.active_count
        assert_equal 0,           @job.input.failed_count
        assert_equal nil,         @job.max_active_workers
        assert_equal false,       @job.throttle_exceeded?

        # Mark all slices as running
        @job.input.each { |s| s.start; @job.input.update(s) }

        assert_equal true, @job.work(@server)
        assert_equal @lines.size, @job.input.count
      end

    end

    context '#upload' do
      setup do
        @job = Workers::SlicedJob.perform_later do |job|
          job.destroy_on_complete = true
          job.slice_size = 100
        end
      end

      should 'handle empty streams' do
        str = ""
        stream = StringIO.new(str)
        @job.upload(stream, format: :text)
        assert_equal 0, @job.input.count
      end

      should 'handle a stream consisting only of the delimiter' do
        str = "\n"
        stream = StringIO.new(str)
        @job.upload(stream, format: :text)
        assert_equal 1, @job.input.count
        @job.input.each do |slice|
          slice.each do |record |
            assert_equal '', record
          end
        end
      end

      should 'handle a linux stream' do
        str = @lines.join("\n")
        stream = StringIO.new(str)
        @job.upload(stream, format: :text)
        assert_equal 1, @job.input.count
        @job.input.each do |slice|
          assert_equal @lines, slice.records
        end
      end

      should 'handle a windows stream' do
        str = @lines.join("\r\n")
        stream = StringIO.new(str)
        @job.upload(stream, format: :text)
      end

      should 'handle a one line stream with a delimiter' do
        str = "hello\r\n"
        stream = StringIO.new(str)
        @job.upload(stream, format: :text)
      end

      should 'handle a one line stream with no delimiter' do
        str = "hello"
        stream = StringIO.new(str)
        @job.upload(stream, format: :text)
        assert_equal 1, @job.input.count
        @job.input.each do |slice|
          slice.each do |record|
            assert_equal stream.string, record
          end
        end
      end

      should 'handle last line ending with a delimiter' do
        str = @lines.join("\r\n")
        str << "\r\n"
        stream = StringIO.new(str)
        @job.upload(stream, format: :text)
        assert_equal 1, @job.input.count
        @job.input.each do |slice|
          assert_equal @lines, slice.records
        end
      end

      should 'handle a slice size of 1' do
        str = @lines.join("\n")
        stream = StringIO.new(str)
        @job.slice_size = 1
        @job.upload(stream, format: :text)
        assert_equal @lines.size, @job.input.count, @job.input.collection.find.to_a
        index = 0
        @job.input.each do |slice|
          slice.each do |record |
            assert_equal @lines[index], record
            index += 1
          end
        end
      end

      should 'handle a small stream the same size as slice_size' do
        str = @lines.join("\n")
        stream = StringIO.new(str)
        @job.slice_size = @lines.size
        @job.upload(stream, format: :text)
        assert_equal 1, @job.input.count, @job.input.collection.find.to_a
        @job.input.each do |slice|
          assert_equal @lines, slice.records
        end
      end

      should 'handle a custom 1 character delimiter' do
        str = @lines.join('$')
        stream = StringIO.new(str)
        @job.slice_size = 1
        @job.upload(stream, delimiter: '$', format: :text)
        assert_equal @lines.size, @job.input.count, @job.input.collection.find.to_a
        index = 0
        @job.input.each do |slice|
          slice.each do |record |
            assert_equal @lines[index], record
            index += 1
          end
        end
      end

      should 'handle a custom multi-character delimiter' do
        delimiter = '$DELIMITER$'
        str = @lines.join(delimiter)
        stream = StringIO.new(str)
        @job.slice_size = 1
        @job.upload(stream, delimiter: delimiter, format: :text)
        assert_equal @lines.size, @job.input.count, @job.input.collection.find.to_a
        index = 0
        @job.input.each do |slice|
          slice.each do |record |
            assert_equal @lines[index], record
            index += 1
          end
        end
      end

    end

    context '#download' do
      setup do
        @job = Workers::SlicedJob.perform_later
      end

      should 'handle no results' do
        stream = StringIO.new('')
        @job.start
        @job.complete
        @job.download(stream, format: :text)
        assert_equal "", stream.string, stream.string.inspect
      end

      should 'raise exception when not completed' do
        @job.upload_slice([ @lines.first ])
        @job.upload_slice([ @lines.second ])
        @job.start!
        # Make worker only process one entry
        RocketJob::Config.instance.stub(:re_check_seconds, 0) do
          assert_equal false, @job.work(@server)
        end

        stream = StringIO.new('')
        assert_equal true, @job.running?, @job.state
        assert_raises(::RuntimeError) { @job.download(stream, format: :text) }
      end

      should 'handle 1 result' do
        @job.upload_slice([ @lines.first ])
        @job.start!
        assert_equal false, @job.work(@server), @job.inspect
        failures = []
        @job.input.each_failed_record { |r, slice| failures << { slice: slice, record: r } }
        assert_equal 0, @job.input.failed_count, failures
        assert_equal true, @job.completed?

        stream = StringIO.new('')
        @job.download(stream, format: :text)
        assert_equal @lines.first + "\n", stream.string, stream.string.inspect
      end

      should 'handle many results' do
        @job.slice_size = 1
        slices = @lines.dup
        @job.upload_records { slices.shift }
        @job.start!
        @job.work(@server)
        failures = []
        @job.input.each_failed_record { |r, slice| failures << { slice: slice, record: r } }
        assert_equal 0, @job.input.failed_count, failures
        assert_equal true, @job.completed?
        stream = StringIO.new('')
        @job.download(stream, format: :text)
        assert_equal @lines.join("\n") + "\n", stream.string, stream.string.inspect
      end
    end

    context '.config' do
      should 'support multiple databases' do
        assert_equal 'test_rocket_job', RocketJob::SlicedJob.collection.db.name
        job = RocketJob::SlicedJob.new
        assert_equal 'test_rocket_job_work', job.input.collection.db.name
        assert_equal 'test_rocket_job_work', job.output.collection.db.name
      end
    end

    context '#throttle_exceeded?' do
      setup do
        @job = Workers::SlicedJob.perform_later
        @job.slice_size = 1
        slices = @lines.dup
        @job.upload_records { slices.shift }
        @job.start!
        assert_equal false, @job.throttle_exceeded?
        @job.input.each { |s| s.start; @job.input.update(s) }
      end

      teardown do
        @job.destroy
      end

      should 'not exceed throttle when not set' do
        assert_equal nil,  @job.max_active_workers
        assert_equal false, @job.throttle_exceeded?
      end

      should 'not exceed throttle when set high' do
        @job.max_active_workers = 2000
        assert_equal false, @job.throttle_exceeded?
      end

      should 'exceed throttle' do
        @job.max_active_workers = 2
        assert_equal true, @job.throttle_exceeded?
      end
    end

  end
end