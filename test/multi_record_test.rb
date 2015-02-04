require File.join(File.dirname(__FILE__), 'test_helper')

# Unit Test for BatchJob::MultiRecord
class MultiRecordJobTest < Minitest::Test
  context BatchJob::MultiRecord do
    setup do
      BatchJob::Single.destroy_all
      @lines = [
        'this is some',
        'data',
        #        '',
        'a',
        'that we can delimit',
        'as necessary'
      ]
      @job = BatchJob::MultiRecord.create(
        description:     @description,
        collect_output: true,
        repeatable:      true,
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
        assert h[:wait_seconds]
        assert h[:status] =~ /Queued for \d+.\d\d seconds/
      end
    end

    context '#input_block' do
      should 'write blocks' do
        @lines.each { |line| @job.input_block([line]) }

        assert_equal @lines.size, @job.record_count
        assert_equal @lines.size, @job.input_collection.count
      end
    end

    context '#input_records' do
      should 'support block size of 1' do
        @job.block_size = 1
        lines = @lines.dup
        result = @job.input_records { lines.shift }
        assert_equal (1..@lines.size), result

        assert_equal @lines.size, @job.record_count
        assert_equal @lines.size, @job.input_collection.count
      end

      should 'support block size of 2' do
        @job.block_size = 2
        lines = @lines.dup
        result = @job.input_records { lines.shift }
        assert_equal (1..@lines.size), result
        block_count = (0.5 + @lines.size.to_f / 2).to_i

        assert_equal @lines.size, @job.record_count
        assert_equal block_count, @job.input_collection.count
      end
    end

    context '#work' do
      should 'read all records' do
        assert_equal 0, @job.record_count
        @job.block_size = 1
        @lines.each { |row| @job.input_block([row]) }
        @job.start!

        assert_equal @lines.size, @job.record_count

        count = 0
        @job.work('server_name') do |data, header|
          assert_equal @lines[count], data
          count += 1
        end
        assert_equal true, @job.completed?
        assert_equal 0, @job.failed_blocks
        assert_equal @lines.size, count
      end

      should 'retry on exception' do
        @job.block_size = 1
        @lines.each { |line| @job.input_block([line]) }
        @job.start!

        assert_equal @lines.size, @job.record_count

        count = 0
        @job.work('server_name') do |data, header|
          count += 1
          raise 'Oh no'
        end
        assert_equal false, @job.completed?
        assert_equal @lines.size, @job.failed_blocks
        assert_equal @lines.size, count
        # Must stay in the queue
        assert_equal @lines.size, @job.input_collection.count

        # Should not process failed jobs
        count = 0
        @job.work('server_name') do |data, header|
          count += 1
        end
        assert_equal 0, count

        # Make records available for processing again
        @job.retry_failed_blocks

        # Re-process the failed jobs
        count = 0
        results = []
        @job.work('server_name') do |record, header|
          results << record
          count += 1
        end
        assert_equal 0, @job.failed_blocks
        assert_equal results.size, count
        assert_equal @lines, results
        assert_equal true, @job.completed?
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
        @job.each_record do |record, header|
          assert_equal [''], record
        end
      end

      should 'handle a linux stream' do
        str = @lines.join("\n")
        stream = StringIO.new(str)
        @job.input_stream(stream)
        assert_equal 1, @job.input_collection.count
        @job.each_record do |record, header|
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
        @job.each_record do |record, header|
          assert_equal [str], record
        end
      end

      should 'handle last line ending with a delimiter' do
        str = @lines.join("\r\n")
        str << "\r\n"
        stream = StringIO.new(str)
        @job.input_stream(stream)
        assert_equal 1, @job.input_collection.count
        @job.each_record do |record, header|
          assert_equal @lines, record
        end
      end

      should 'handle a block size of 1' do
        str = @lines.join("\n")
        stream = StringIO.new(str)
        @job.block_size = 1
        @job.input_stream(stream)
        assert_equal @lines.size, @job.input_collection.count, @job.input_collection.find.to_a
        index = 0
        @job.each_record do |record, header|
          assert_equal [ @lines[index] ], record
          index += 1
        end
      end

      should 'handle a small stream the same size as block_size' do
        str = @lines.join("\n")
        stream = StringIO.new(str)
        @job.block_size = @lines.size
        @job.input_stream(stream)
        assert_equal 1, @job.input_collection.count, @job.input_collection.find.to_a
        @job.each_record do |record, header|
          assert_equal @lines, record
        end
      end

      should 'handle a custom 1 character delimiter' do
        str = @lines.join('$')
        stream = StringIO.new(str)
        @job.block_size = 1
        @job.input_stream(stream, delimiter: '$')
        assert_equal @lines.size, @job.input_collection.count, @job.input_collection.find.to_a
        index = 0
        @job.each_record do |record, header|
          assert_equal [ @lines[index] ], record
          index += 1
        end
      end

      should 'handle a custom multi-character delimiter' do
        delimiter = '$DELIMITER$'
        str = @lines.join(delimiter)
        stream = StringIO.new(str)
        @job.block_size = 1
        @job.input_stream(stream, delimiter: delimiter)
        assert_equal @lines.size, @job.input_collection.count, @job.input_collection.find.to_a
        index = 0
        @job.each_record do |record, header|
          assert_equal [ @lines[index] ], record
          index += 1
        end
      end

      should 'compress records' do
        @job.compress = true

        str = @lines.join("\n")
        stream = StringIO.new(str)
        @job.block_size = 1
        @job.input_stream(stream)
        assert_equal @lines.size, @job.input_collection.count, @job.input_collection.find.to_a
        index = 0
        @job.each_record do |record, header|
          assert_equal [ @lines[index] ], record
          index += 1
        end
        # Confirm that the data stored was actually compressed
        @job.input_collection.find_one do |record|
          assert_equal Zlib::Deflate.deflate(@lines.first), record['data'].to_s, record.inspect
        end
      end

      should 'encrypt records' do
        @job.encrypt = true

        str = @lines.join("\n")
        stream = StringIO.new(str)
        @job.block_size = 1
        @job.input_stream(stream)
        assert_equal @lines.size, @job.input_collection.count, @job.input_collection.find.to_a
        index = 0
        @job.each_record do |record, header|
          assert_equal [ @lines[index] ], record
          index += 1
        end
        # Confirm that the data stored was actually encrypted
        @job.input_collection.find_one do |record|
          assert_equal SymmetricEncryption.cipher.binary_encrypt(@lines.first, true, compress=false), record['data'].to_s, record.inspect
        end
      end

      should 'compress and encrypt records' do
        @job.encrypt = true
        @job.compress = true

        str = @lines.join("\n")
        stream = StringIO.new(str)
        @job.block_size = 1
        @job.input_stream(stream)
        assert_equal @lines.size, @job.input_collection.count, @job.input_collection.find.to_a
        index = 0
        @job.each_record do |record, header|
          assert_equal [ @lines[index] ], record
          index += 1
        end
        # Confirm that the data stored was actually compressed & encrypted
        @job.input_collection.find_one do |record|
          assert_equal SymmetricEncryption.cipher.binary_encrypt(@lines.first, true, compress=true), record['data'].to_s, record.inspect
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
        @job.input_block([ @lines.first ])
        @job.start!
        @job.work('worker') { |block| block }
        assert_equal true, @job.completed?
        assert_equal 0, @job.failed_blocks
        stream = StringIO.new('')
        @job.output_stream(stream)
        assert_equal @lines.first + "\n", stream.string, stream.string.inspect
      end

      should 'handle many results' do
        @job.block_size = 1
        blocks = @lines.dup
        @job.input_records { blocks.shift }
        @job.start!
        @job.work('worker') { |block| block }
        assert_equal true, @job.completed?
        assert_equal 0, @job.failed_blocks
        stream = StringIO.new('')
        @job.output_stream(stream)
        assert_equal @lines.join("\n") + "\n", stream.string, stream.string.inspect
      end

      should 'decompress results' do
        @job.compress = true
        @job.block_size = 1
        blocks = @lines.dup
        @job.input_records { blocks.shift }
        @job.start!
        @job.work('worker') { |block| block }
        assert_equal true, @job.completed?
        assert_equal 0, @job.failed_blocks
        stream = StringIO.new('')
        @job.output_stream(stream)
        assert_equal @lines.join("\n") + "\n", stream.string, stream.string.inspect
      end

      should 'decrypt results' do
        @job.encrypt = true
        @job.block_size = 1
        blocks = @lines.dup
        @job.input_records { blocks.shift }
        @job.start!
        @job.work('worker') { |block| block }
        assert_equal true, @job.completed?
        assert_equal 0, @job.failed_blocks
        stream = StringIO.new('')
        @job.output_stream(stream)
        assert_equal @lines.join("\n") + "\n", stream.string, stream.string.inspect
      end

      should 'decompress & decrypt results' do
        @job.compress = true
        @job.encrypt = true
        @job.block_size = 1
        blocks = @lines.dup
        @job.input_records { blocks.shift }
        @job.start!
        @job.work('worker') { |block| block }
        assert_equal true, @job.completed?
        assert_equal 0, @job.failed_blocks
        stream = StringIO.new('')
        @job.output_stream(stream)
        assert_equal @lines.join("\n") + "\n", stream.string, stream.string.inspect
      end

    end

    context '.config' do
      should 'support multiple databases' do
        assert_equal 'test_batch_job', BatchJob::MultiRecord.collection.db.name
        job = BatchJob::MultiRecord.new
        assert_equal 'test_batch_job_work', job.input_collection.db.name
        assert_equal 'test_batch_job_work', job.output_collection.db.name
      end
    end

  end
end