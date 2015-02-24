require_relative 'test_helper'
require_relative 'workers/batch_job'

# Unit Test for RocketJob::BatchJob
class MultiRecordJobTest < Minitest::Test
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
      @job = RocketJob::BatchJob.create(
        description:         @description,
        collect_output:      true,
        repeatable:          true,
        klass:               'Workers::BatchJob',
        destroy_on_complete: false
      )
    end

    teardown do
      @job.destroy if @job && !@job.new_record?
    end

    context '#status' do
      should 'return status for a queued job' do
        assert_equal true, @job.queued?
        h = @job.status
        assert_equal :queued,      h[:state]
        assert_equal @description, h[:description]
        assert h[:seconds]
        assert h[:status] =~ /Queued for \d+.\d\d seconds/
      end
    end

    context '#input_slice' do
      should 'write slices' do
        @lines.each { |line| @job.input_slice([line]) }

        assert_equal @lines.size, @job.record_count
        assert_equal @lines.size, @job.input_collection.count
      end
    end

    context '#input_records' do
      should 'support slice size of 1' do
        @job.slice_size = 1
        lines = @lines.dup
        result = @job.input_records { lines.shift }
        assert_equal (1..@lines.size), result

        assert_equal @lines.size, @job.record_count
        assert_equal @lines.size, @job.input_collection.count
      end

      should 'support slice size of 2' do
        @job.slice_size = 2
        lines = @lines.dup
        result = @job.input_records { lines.shift }
        assert_equal (1..@lines.size), result
        slice_count = (0.5 + @lines.size.to_f / 2).to_i

        assert_equal @lines.size, @job.record_count
        assert_equal slice_count, @job.input_collection.count
      end
    end

    context '#work' do
      should 'read all records' do
        assert_equal 0, @job.record_count
        @job.slice_size = 1
        @lines.each { |row| @job.input_slice([row]) }
        @job.start!

        assert_equal @lines.size, @job.record_count

        count = 0
        @job.work(@server)
        failures = []
        @job.each_failed_record { |r, h| failures << { header: h, record: r } }
        assert_equal 0, @job.slices_failed, failures
        @job.each_output_record do |record|
          assert_equal @lines[count], record
          count += 1
        end
        assert_equal 0, @job.slices_failed
        assert_equal true, @job.completed?
        assert_equal @lines.size, count
      end

      should 'retry on exception' do
        @job.method = :oh_no
        @job.slice_size = 1
        @lines.each { |line| @job.input_slice([line]) }
        @job.start!

        assert_equal @lines.size, @job.record_count

        # New jobs should fail
        @job.work(@server)
        assert_equal @lines.size, @job.slices_failed
        assert_equal 0, @job.slices_processed
        assert_equal 0, @job.slices_queued
        assert_equal 0, @job.slices_active
        assert_equal false, @job.completed?

        # Should not process failed jobs
        @job.work(@server)
        assert_equal @lines.size, @job.slices_failed
        assert_equal 0, @job.slices_processed
        assert_equal 0, @job.slices_queued
        assert_equal 0, @job.slices_active
        assert_equal false, @job.completed?

        # Make records available for processing again
        @job.requeue

        # Re-process the failed jobs
        @job.method = :perform
        @job.work(@server)
        failures = []
        @job.each_failed_record { |r, h| failures << { header: h, record: r } }
        assert_equal 0, @job.slices_failed, failures
        assert_equal @lines.size, @job.slices_processed
        assert_equal 0, @job.slices_queued
        assert_equal 0, @job.slices_active
        assert_equal :complete, @job.sub_state
        assert_equal true, @job.completed?
      end

      should 'call before_event' do
        @job.method = :event
        named_parameters = { 'counter' => 23 }
        @job.arguments << named_parameters
        assert_equal 0, @job.record_count
        @job.slice_size = 1
        @lines.each { |row| @job.input_slice([row]) }
        @job.start!

        @job.reload
        assert_equal named_parameters, @job.arguments.first
        assert_equal @lines.size, @job.record_count

        @job.work(@server)
        assert_equal named_parameters.merge('before_event' => true, 'after_event' => true), @job.arguments.first
        assert_equal :complete, @job.sub_state

        failures = []
        @job.each_failed_record { |r, h| failures << { header: h, record: r } }
        assert_equal 0, @job.slices_failed, failures
        assert_equal @lines.size, @job.slices_processed
        assert_equal 0, @job.slices_queued
        assert_equal 0, @job.slices_active
        assert_equal true, @job.completed?
      end

      should 'retry after an after_perform exception' do
        @job.method = :able
        @job.slice_size = 1
        @lines.each { |line| @job.input_slice([line]) }
        @job.start!

        assert_equal @lines.size, @job.record_count

        # New jobs should fail
        @job.work(@server)
        failures = []
        @job.each_failed_record { |r, h| failures << { header: h, record: r } }
        assert_equal 0, @job.slices_failed
        assert_equal @lines.size, @job.slices_processed
        assert_equal 0, @job.slices_queued
        assert_equal 0, @job.slices_active
        assert_equal true, @job.failed?
        assert_equal :after, @job.sub_state

        # Make records available for processing again
        @job.retry!

        # Re-process the failed job
        @job.work(@server)
        failures = []
        @job.each_failed_record { |r, h| failures << { header: h, record: r } }
        assert_equal 0, @job.slices_failed, failures
        assert_equal @lines.size, @job.slices_processed
        assert_equal 0, @job.slices_queued
        assert_equal 0, @job.slices_active
        assert_equal true, @job.completed?, @job.state
        assert_equal :complete, @job.sub_state
      end

      should 'retry after a before_perform exception' do
        @job.method = :probable
        @job.slice_size = 1
        @lines.each { |line| @job.input_slice([line]) }
        @job.start!

        assert_equal @lines.size, @job.record_count

        # New jobs should fail
        @job.work(@server)
        assert_equal 0, @job.slices_failed
        assert_equal 0, @job.slices_processed
        assert_equal @lines.size, @job.slices_queued
        assert_equal 0, @job.slices_active
        assert_equal true, @job.failed?
        assert_equal :before, @job.sub_state

        # Make records available for processing again
        @job.retry!

        # Re-process the failed job
        @job.work(@server)
        failures = []
        @job.each_failed_record { |r, h| failures << { header: h, record: r } }
        assert_equal 0, @job.slices_failed, failures
        assert_equal @lines.size, @job.slices_processed
        assert_equal 0, @job.slices_queued
        assert_equal 0, @job.slices_active
        assert_equal :complete, @job.sub_state
        assert_equal true, @job.completed?, @job.state
      end

    end

    context '#input_stream' do
      should 'handle empty streams' do
        str = ""
        stream = StringIO.new(str)
        @job.input_stream(stream)
        assert_equal 0, @job.input_collection.count
      end

      should 'handle a stream consisting only of the delimiter' do
        str = "\n"
        stream = StringIO.new(str)
        @job.input_stream(stream)
        assert_equal 1, @job.input_collection.count
        @job.each_input_slice do |record, header|
          assert_equal [''], record
        end
      end

      should 'handle a linux stream' do
        str = @lines.join("\n")
        stream = StringIO.new(str)
        @job.input_stream(stream)
        assert_equal 1, @job.input_collection.count
        @job.each_input_slice do |record, header|
          assert_equal @lines, record
        end
      end

      should 'handle a windows stream' do
        str = @lines.join("\r\n")
        stream = StringIO.new(str)
        @job.input_stream(stream)
      end

      should 'handle a one line stream with a delimiter' do
        str = "hello\r\n"
        stream = StringIO.new(str)
        @job.input_stream(stream)
      end

      should 'handle a one line stream with no delimiter' do
        str = "hello"
        stream = StringIO.new(str)
        @job.input_stream(stream)
        assert_equal 1, @job.input_collection.count
        @job.each_input_slice do |record, header|
          assert_equal [str], record
        end
      end

      should 'handle last line ending with a delimiter' do
        str = @lines.join("\r\n")
        str << "\r\n"
        stream = StringIO.new(str)
        @job.input_stream(stream)
        assert_equal 1, @job.input_collection.count
        @job.each_input_slice do |record, header|
          assert_equal @lines, record
        end
      end

      should 'handle a slice size of 1' do
        str = @lines.join("\n")
        stream = StringIO.new(str)
        @job.slice_size = 1
        @job.input_stream(stream)
        assert_equal @lines.size, @job.input_collection.count, @job.input_collection.find.to_a
        index = 0
        @job.each_input_slice do |record, header|
          assert_equal [ @lines[index] ], record
          index += 1
        end
      end

      should 'handle a small stream the same size as slice_size' do
        str = @lines.join("\n")
        stream = StringIO.new(str)
        @job.slice_size = @lines.size
        @job.input_stream(stream)
        assert_equal 1, @job.input_collection.count, @job.input_collection.find.to_a
        @job.each_input_slice do |record, header|
          assert_equal @lines, record
        end
      end

      should 'handle a custom 1 character delimiter' do
        str = @lines.join('$')
        stream = StringIO.new(str)
        @job.slice_size = 1
        @job.input_stream(stream, delimiter: '$')
        assert_equal @lines.size, @job.input_collection.count, @job.input_collection.find.to_a
        index = 0
        @job.each_input_slice do |record, header|
          assert_equal [ @lines[index] ], record
          index += 1
        end
      end

      should 'handle a custom multi-character delimiter' do
        delimiter = '$DELIMITER$'
        str = @lines.join(delimiter)
        stream = StringIO.new(str)
        @job.slice_size = 1
        @job.input_stream(stream, delimiter: delimiter)
        assert_equal @lines.size, @job.input_collection.count, @job.input_collection.find.to_a
        index = 0
        @job.each_input_slice do |record, header|
          assert_equal [ @lines[index] ], record
          index += 1
        end
      end

      should 'compress records' do
        @job.compress = true

        str = @lines.join("\n")
        stream = StringIO.new(str)
        @job.slice_size = 1
        @job.input_stream(stream)
        assert_equal @lines.size, @job.input_collection.count, @job.input_collection.find.to_a
        index = 0
        @job.each_input_slice do |record, header|
          assert_equal [ @lines[index] ], record
          index += 1
        end
        # Confirm that the slice stored was actually compressed
        @job.input_collection.find_one do |record|
          assert_equal Zlib::Deflate.deflate(@lines.first), record['slice'].to_s, record.inspect
        end
      end

      should 'encrypt records' do
        @job.encrypt = true

        str = @lines.join("\n")
        stream = StringIO.new(str)
        @job.slice_size = 1
        @job.input_stream(stream)
        assert_equal @lines.size, @job.input_collection.count, @job.input_collection.find.to_a
        index = 0
        @job.each_input_slice do |record, header|
          assert_equal [ @lines[index] ], record
          index += 1
        end
        # Confirm that the slice stored was actually encrypted
        @job.input_collection.find_one do |record|
          assert_equal SymmetricEncryption.cipher.binary_encrypt(@lines.first, true, compress=false), record['slice'].to_s, record.inspect
        end
      end

      should 'compress and encrypt records' do
        @job.encrypt = true
        @job.compress = true

        str = @lines.join("\n")
        stream = StringIO.new(str)
        @job.slice_size = 1
        @job.input_stream(stream)
        assert_equal @lines.size, @job.input_collection.count, @job.input_collection.find.to_a
        index = 0
        @job.each_input_slice do |record, header|
          assert_equal [ @lines[index] ], record
          index += 1
        end
        # Confirm that the slice stored was actually compressed & encrypted
        @job.input_collection.find_one do |record|
          assert_equal SymmetricEncryption.cipher.binary_encrypt(@lines.first, true, compress=true), record['slice'].to_s, record.inspect
        end
      end

    end

    context '#output_stream' do
      should 'handle no results' do
        stream = StringIO.new('')
        @job.output_stream(stream)
        assert_equal "", stream.string, stream.string.inspect
      end

      should 'handle 1 result' do
        @job.input_slice([ @lines.first ])
        @job.start!
        @job.work(@server)
        failures = []
        @job.each_failed_record { |r, h| failures << { header: h, record: r } }
        assert_equal 0, @job.slices_failed, failures
        assert_equal true, @job.completed?
        stream = StringIO.new('')
        @job.output_stream(stream)
        assert_equal @lines.first + "\n", stream.string, stream.string.inspect
      end

      should 'handle many results' do
        @job.slice_size = 1
        slices = @lines.dup
        @job.input_records { slices.shift }
        @job.start!
        @job.work(@server)
        failures = []
        @job.each_failed_record { |r, h| failures << { header: h, record: r } }
        assert_equal 0, @job.slices_failed, failures
        assert_equal true, @job.completed?
        stream = StringIO.new('')
        @job.output_stream(stream)
        assert_equal @lines.join("\n") + "\n", stream.string, stream.string.inspect
      end

      should 'decompress results' do
        @job.compress = true
        @job.slice_size = 1
        slices = @lines.dup
        @job.input_records { slices.shift }
        @job.start!
        @job.work(@server)
        failures = []
        @job.each_failed_record { |r, h| failures << { header: h, record: r } }
        assert_equal 0, @job.slices_failed, failures
        assert_equal true, @job.completed?, @job.state
        stream = StringIO.new('')
        @job.output_stream(stream)
        assert_equal @lines.join("\n") + "\n", stream.string, stream.string.inspect
      end

      should 'decrypt results' do
        @job.encrypt = true
        @job.slice_size = 1
        slices = @lines.dup
        @job.input_records { slices.shift }
        @job.start!
        @job.work(@server)
        failures = []
        @job.each_failed_record { |r, h| failures << { header: h, record: r } }
        assert_equal 0, @job.slices_failed, failures
        assert_equal true, @job.completed?, @job.state
        stream = StringIO.new('')
        @job.output_stream(stream)
        assert_equal @lines.join("\n") + "\n", stream.string, stream.string.inspect
      end

      should 'decompress & decrypt results' do
        @job.compress = true
        @job.encrypt = true
        @job.slice_size = 1
        slices = @lines.dup
        @job.input_records { slices.shift }
        @job.start!
        @job.work(@server)
        failures = []
        @job.each_failed_record { |r, h| failures << { header: h, record: r } }
        assert_equal 0, @job.slices_failed, failures
        assert_equal :complete, @job.sub_state
        assert_equal true, @job.completed?, @job.state
        stream = StringIO.new('')
        @job.output_stream(stream)
        assert_equal @lines.join("\n") + "\n", stream.string, stream.string.inspect
      end

    end

    context '.config' do
      should 'support multiple databases' do
        assert_equal 'test_rocket_job', RocketJob::BatchJob.collection.db.name
        job = RocketJob::BatchJob.new
        assert_equal 'test_rocket_job_work', job.input_collection.db.name
        assert_equal 'test_rocket_job_work', job.output_collection.db.name
      end
    end

  end
end