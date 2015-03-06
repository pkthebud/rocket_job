require_relative 'test_helper'
require_relative 'workers/batch_job'

# Unit Test for RocketJob::BatchJob
class BatchJobTest < Minitest::Test
  context RocketJob::BatchJob do
    setup do
      RocketJob::Job.destroy_all
      @server = RocketJob::Server.new
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
        @job = Workers::BatchJob.perform_later
        assert_equal @description, @job.description
        assert_equal true, @job.collect_output?
        assert_equal true, @job.repeatable
        assert_equal false, @job.destroy_on_complete
      end
    end

    context '#status' do
      should 'return status for a queued job' do
        @job = Workers::BatchJob.perform_later
        assert_equal true, @job.queued?
        h = @job.status
        assert_equal :queued,      h[:state]
        assert_equal @description, h[:description]
        assert h[:seconds]
        assert h[:status] =~ /Queued for \d+.\d\d seconds/
      end
    end

    context '#write_slice' do
      should 'write slices' do
        @job = Workers::BatchJob.perform_later
        @lines.each { |line| @job.upload_slice([line]) }

        assert_equal @lines.size, @job.record_count
        assert_equal @lines.size, @job.input.total_slices
        assert_equal @lines.size, @job.input.queued_slices
        assert_equal 0,           @job.input.active_slices
        assert_equal 0,           @job.input.failed_slices
      end
    end

    context '#write_records' do
      should 'support slice size of 1' do
        lines = @lines.dup
        count = 0
        @job = Workers::BatchJob.perform_later do |job|
          # Override default slice size
          job.slice_size = 1
          count = job.upload_records { lines.shift }
        end
        assert_equal @lines.size, count
        assert_equal @lines.size, @job.record_count
        assert_equal @lines.size, @job.input.total_slices
        assert_equal @lines.size, @job.input.queued_slices
        assert_equal 0,           @job.input.active_slices
        assert_equal 0,           @job.input.failed_slices
      end

      should 'support slice size of 2' do
        lines = @lines.dup
        count = 0
        @job = Workers::BatchJob.perform_later do |job|
          # Override default slice size
          job.slice_size = 2
          count = job.upload_records { lines.shift }
        end
        slice_count = (0.5 + @lines.size.to_f / 2).to_i

        assert_equal @lines.size, count
        assert_equal @lines.size, @job.record_count
        assert_equal slice_count, @job.input.total_slices
        assert_equal slice_count, @job.input.queued_slices
        assert_equal 0,           @job.input.active_slices
        assert_equal 0,           @job.input.failed_slices
      end
    end

    context '#work' do
      should 'read all records' do
        @job = Workers::BatchJob.perform_later do |job|
          assert_equal 0, job.record_count
          # slice_size has no effect since it calling #write_slice directly
          job.slice_size = 1
          @lines.each { |record| job.upload_slice([record]) }
        end
        @job.start!

        assert_equal @lines.size, @job.record_count
        assert_equal @lines.size, @job.input.total_slices
        assert_equal @lines.size, @job.input.queued_slices
        assert_equal 0,           @job.input.active_slices
        assert_equal 0,           @job.input.failed_slices

        count = 0
        @job.work(@server)
        failures = []
        @job.input.each_failed_record { |r, h| failures << { header: h, record: r } }
        assert_equal 0, @job.input.failed_slices, failures
        @job.output.each_record do |record|
          assert_equal @lines[count], record
          count += 1
        end

        assert_equal @lines.size, count
        assert_equal @lines.size, @job.record_count
        assert_equal 0,           @job.input.total_slices
        assert_equal 0,           @job.input.queued_slices
        assert_equal 0,           @job.input.active_slices
        assert_equal 0,           @job.input.failed_slices
        assert_equal true, @job.completed?
        assert_equal @lines.size, @job.output.total_slices
        RocketJob::Job.find(@job.id)
      end

      should 'destroy on completion' do
        @job = Workers::BatchJob.perform_later do |job|
          assert_equal 0, job.record_count
          job.destroy_on_complete = true
          job.slice_size = 1
          @lines.each { |row| job.upload_slice([row]) }
        end
        @job.start!

        assert_equal @lines.size, @job.record_count

        count = 0
        @job.work(@server)
        failures = []
        @job.input.each_failed_record { |r, h| failures << { header: h, record: r } }
        assert_equal 0, @job.input.failed_slices, failures
        @job.output.each_record do |record|
          count += 1
        end
        assert_equal 0, @job.input.failed_slices
        assert_equal true, @job.completed?
        assert_equal 0, @job.output.total_slices
        assert_equal 0, count
        assert_equal nil, RocketJob::Job.where(id: @job.id).first
      end

      should 'retry on exception' do
        @job = Workers::BatchJob.later(:oh_no) do |job|
          job.slice_size = 1
          @lines.each { |row| job.upload_slice([row]) }
        end
        @job.start!

        assert_equal @lines.size, @job.record_count

        # New jobs should fail
        @job.work(@server)
        assert_equal @lines.size, @job.input.failed_slices
        assert_equal 0, @job.output.total_slices
        assert_equal 0, @job.input.queued_slices
        assert_equal 0, @job.input.active_slices
        assert_equal false, @job.completed?

        # Should not process failed jobs
        @job.work(@server)
        assert_equal @lines.size, @job.input.failed_slices
        assert_equal 0, @job.output.total_slices
        assert_equal 0, @job.input.queued_slices
        assert_equal 0, @job.input.active_slices
        assert_equal false, @job.completed?

        # Make records available for processing again
        @job.input.requeue_failed_slices

        # Re-process the failed jobs
        @job.perform_method = :perform
        @job.work(@server)
        failures = []
        @job.input.each_failed_record { |r, h| failures << { header: h, record: r } }
        assert_equal 0, @job.input.failed_slices, failures
        assert_equal @lines.size, @job.output.total_slices
        assert_equal 0, @job.input.queued_slices
        assert_equal 0, @job.input.active_slices
        assert_equal nil, @job.sub_state
        assert_equal true, @job.completed?
      end

      should 'call before_event' do
        named_parameters = { 'counter' => 23 }
        @job = Workers::BatchJob.later(:event, named_parameters) do |job|
          job.slice_size = 1
          @lines.each { |row| job.upload_slice([row]) }
        end
        @job.start!

        @job.reload
        assert_equal named_parameters, @job.arguments.first
        assert_equal @lines.size, @job.record_count

        @job.work(@server)
        assert_equal named_parameters.merge('before_event' => true, 'after_event' => true), @job.arguments.first
        assert_equal nil, @job.sub_state

        failures = []
        @job.input.each_failed_record { |r, h| failures << { header: h, record: r } }
        assert_equal 0, @job.input.failed_slices, failures
        assert_equal @lines.size, @job.output.total_slices
        assert_equal 0, @job.input.queued_slices
        assert_equal 0, @job.input.active_slices
        assert_equal true, @job.completed?
      end

      should 'retry after an after_perform exception' do
        @job = Workers::BatchJob.later(:able) do |job|
          job.slice_size = 1
          @lines.each { |row| job.upload_slice([row]) }
        end
        @job.start!

        assert_equal @lines.size, @job.record_count

        # New jobs should fail
        @job.work(@server)
        failures = []
        @job.input.each_failed_record { |r, h| failures << { header: h, record: r } }
        assert_equal 0, @job.input.failed_slices
        assert_equal @lines.size, @job.output.total_slices
        assert_equal 0, @job.input.queued_slices
        assert_equal 0, @job.input.active_slices
        assert_equal true, @job.failed?
        assert_equal :after, @job.sub_state

        # Make records available for processing again
        @job.retry!

        # Re-process the failed job
        @job.work(@server)
        failures = []
        @job.input.each_failed_record { |r, h| failures << { header: h, record: r } }
        assert_equal 0, @job.input.failed_slices, failures
        assert_equal @lines.size, @job.output.total_slices
        assert_equal 0, @job.input.queued_slices
        assert_equal 0, @job.input.active_slices
        assert_equal true, @job.completed?, @job.state
        assert_equal nil, @job.sub_state
      end

      should 'retry after a before_perform exception' do
        @job = Workers::BatchJob.later(:probable) do |job|
          job.slice_size = 1
          @lines.each { |row| job.upload_slice([row]) }
        end
        @job.start!

        assert_equal @lines.size, @job.record_count

        # New jobs should fail
        @job.work(@server)
        assert_equal 0, @job.input.failed_slices
        assert_equal 0, @job.output.total_slices
        assert_equal @lines.size, @job.input.queued_slices
        assert_equal 0, @job.input.active_slices
        assert_equal true, @job.failed?
        assert_equal :before, @job.sub_state

        # Make records available for processing again
        @job.retry!

        # Re-process the failed job
        @job.work(@server)
        failures = []
        @job.input.each_failed_record { |r, h| failures << { header: h, record: r } }
        assert_equal 0, @job.input.failed_slices, failures
        assert_equal @lines.size, @job.output.total_slices
        assert_equal 0, @job.input.queued_slices
        assert_equal 0, @job.input.active_slices
        assert_equal nil, @job.sub_state
        assert_equal true, @job.completed?, @job.state
      end

    end

    context '#upload' do
      setup do
        @job = Workers::BatchJob.perform_later do |job|
          job.destroy_on_complete = true
          job.slice_size = 100
        end
      end

      should 'handle empty streams' do
        str = ""
        stream = StringIO.new(str)
        @job.upload(stream)
        assert_equal 0, @job.input.total_slices
      end

      should 'handle a stream consisting only of the delimiter' do
        str = "\n"
        stream = StringIO.new(str)
        @job.upload(stream)
        assert_equal 1, @job.input.total_slices
        @job.input.each_slice do |record, header|
          assert_equal [''], record
        end
      end

      should 'handle a linux stream' do
        str = @lines.join("\n")
        stream = StringIO.new(str)
        @job.upload(stream)
        assert_equal 1, @job.input.total_slices
        @job.input.each_slice do |record, header|
          assert_equal @lines, record
        end
      end

      should 'handle a windows stream' do
        str = @lines.join("\r\n")
        stream = StringIO.new(str)
        @job.upload(stream)
      end

      should 'handle a one line stream with a delimiter' do
        str = "hello\r\n"
        stream = StringIO.new(str)
        @job.upload(stream)
      end

      should 'handle a one line stream with no delimiter' do
        str = "hello"
        stream = StringIO.new(str)
        @job.upload(stream)
        assert_equal 1, @job.input.total_slices
        @job.input.each_slice do |record, header|
          assert_equal [str], record
        end
      end

      should 'handle last line ending with a delimiter' do
        str = @lines.join("\r\n")
        str << "\r\n"
        stream = StringIO.new(str)
        @job.upload(stream)
        assert_equal 1, @job.input.total_slices
        @job.input.each_slice do |record, header|
          assert_equal @lines, record
        end
      end

      should 'handle a slice size of 1' do
        str = @lines.join("\n")
        stream = StringIO.new(str)
        @job.slice_size = 1
        @job.upload(stream)
        assert_equal @lines.size, @job.input.total_slices, @job.input.collection.find.to_a
        index = 0
        @job.input.each_slice do |record, header|
          assert_equal [ @lines[index] ], record
          index += 1
        end
      end

      should 'handle a small stream the same size as slice_size' do
        str = @lines.join("\n")
        stream = StringIO.new(str)
        @job.slice_size = @lines.size
        @job.upload(stream)
        assert_equal 1, @job.input.total_slices, @job.input.collection.find.to_a
        @job.input.each_slice do |record, header|
          assert_equal @lines, record
        end
      end

      should 'handle a custom 1 character delimiter' do
        str = @lines.join('$')
        stream = StringIO.new(str)
        @job.slice_size = 1
        @job.upload(stream, delimiter: '$')
        assert_equal @lines.size, @job.input.total_slices, @job.input.collection.find.to_a
        index = 0
        @job.input.each_slice do |record, header|
          assert_equal [ @lines[index] ], record
          index += 1
        end
      end

      should 'handle a custom multi-character delimiter' do
        delimiter = '$DELIMITER$'
        str = @lines.join(delimiter)
        stream = StringIO.new(str)
        @job.slice_size = 1
        @job.upload(stream, delimiter: delimiter)
        assert_equal @lines.size, @job.input.total_slices, @job.input.collection.find.to_a
        index = 0
        @job.input.each_slice do |record, header|
          assert_equal [ @lines[index] ], record
          index += 1
        end
      end

      should 'compress records' do
        @job.compress = true

        str = @lines.join("\n")
        stream = StringIO.new(str)
        @job.slice_size = 1
        @job.upload(stream)
        assert_equal @lines.size, @job.input.total_slices, @job.input.collection.find.to_a
        index = 0
        @job.input.each_slice do |record, header|
          assert_equal [ @lines[index] ], record
          index += 1
        end
        # Confirm that the slice stored was actually compressed
        message = @job.input.collection.find.sort('_id').limit(1).first
        slice   = message['slice'].to_s
        assert_equal Zlib::Deflate.deflate(@lines.first), slice, Zlib::Inflate.inflate(slice)
      end

      should 'encrypt records' do
        @job.encrypt = true

        str = @lines.join("\n")
        stream = StringIO.new(str)
        @job.slice_size = 1
        @job.upload(stream)
        assert_equal @lines.size, @job.input.total_slices, @job.input.collection.find.to_a
        index = 0
        @job.input.each_slice do |record, header|
          assert_equal [ @lines[index] ], record
          index += 1
        end
        # Confirm that the slice stored was actually encrypted
        message = @job.input.collection.find.sort('_id').limit(1).first
        slice   = message['slice'].to_s
        assert_equal @lines.first, SymmetricEncryption.cipher.binary_decrypt(slice)
      end

      should 'compress and encrypt records' do
        @job.encrypt = true
        @job.compress = true

        str = @lines.join("\n")
        stream = StringIO.new(str)
        @job.slice_size = 1
        @job.upload(stream)
        assert_equal @lines.size, @job.input.total_slices, @job.input.collection.find.to_a
        index = 0
        @job.input.each_slice do |record, header|
          assert_equal [ @lines[index] ], record
          index += 1
        end
        # Confirm that the slice stored was actually compressed & encrypted
        message = @job.input.collection.find.sort('_id').limit(1).first
        slice   = message['slice'].to_s
        assert_equal @lines.first, SymmetricEncryption.cipher.binary_decrypt(slice)
      end

    end

    context '#download' do
      setup do
        @job = Workers::BatchJob.perform_later
      end

      should 'handle no results' do
        stream = StringIO.new('')
        @job.download(stream)
        assert_equal "", stream.string, stream.string.inspect
      end

      should 'handle 1 result' do
        @job.upload_slice([ @lines.first ])
        @job.start!
        count = @job.work(@server)
        assert_equal 1, count
        failures = []
        @job.input.each_failed_record { |r, h| failures << { header: h, record: r } }
        assert_equal 0, @job.input.failed_slices, failures
        assert_equal true, @job.completed?

        stream = StringIO.new('')
        @job.download(stream)
        assert_equal @lines.first + "\n", stream.string, stream.string.inspect
      end

      should 'handle many results' do
        @job.slice_size = 1
        slices = @lines.dup
        @job.upload_records { slices.shift }
        @job.start!
        @job.work(@server)
        failures = []
        @job.input.each_failed_record { |r, h| failures << { header: h, record: r } }
        assert_equal 0, @job.input.failed_slices, failures
        assert_equal true, @job.completed?
        stream = StringIO.new('')
        @job.download(stream)
        assert_equal @lines.join("\n") + "\n", stream.string, stream.string.inspect
      end

      should 'decompress results' do
        @job.compress = true
        @job.slice_size = 1
        slices = @lines.dup
        @job.upload_records { slices.shift }
        @job.start!
        @job.work(@server)
        failures = []
        @job.input.each_failed_record { |r, h| failures << { header: h, record: r } }
        assert_equal 0, @job.input.failed_slices, failures
        assert_equal true, @job.completed?, @job.state
        stream = StringIO.new('')
        @job.download(stream)
        assert_equal @lines.join("\n") + "\n", stream.string, stream.string.inspect
      end

      should 'decrypt results' do
        @job.encrypt = true
        @job.slice_size = 1
        slices = @lines.dup
        @job.upload_records { slices.shift }
        @job.start!
        @job.work(@server)
        failures = []
        @job.input.each_failed_record { |r, h| failures << { header: h, record: r } }
        assert_equal 0, @job.input.failed_slices, failures
        assert_equal true, @job.completed?, @job.state
        stream = StringIO.new('')
        @job.download(stream)
        assert_equal @lines.join("\n") + "\n", stream.string, stream.string.inspect
      end

      should 'decompress & decrypt results' do
        @job.compress = true
        @job.encrypt = true
        @job.slice_size = 1
        slices = @lines.dup
        @job.upload_records { slices.shift }
        @job.start!
        @job.work(@server)
        failures = []
        @job.input.each_failed_record { |r, h| failures << { header: h, record: r } }
        assert_equal 0, @job.input.failed_slices, failures
        assert_equal nil, @job.sub_state
        assert_equal true, @job.completed?, @job.state
        stream = StringIO.new('')
        @job.download(stream)
        assert_equal @lines.join("\n") + "\n", stream.string, stream.string.inspect
      end

    end

    context '.config' do
      should 'support multiple databases' do
        assert_equal 'test_rocket_job', RocketJob::BatchJob.collection.db.name
        job = RocketJob::BatchJob.new
        assert_equal 'test_rocket_job_work', job.input.collection.db.name
        assert_equal 'test_rocket_job_work', job.output.collection.db.name
      end
    end

  end
end