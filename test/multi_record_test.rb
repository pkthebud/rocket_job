require File.join(File.dirname(__FILE__), 'test_helper')

# Unit Test for BatchJob::MultiRecord
class MultiRecordJobTest < Minitest::Test
  context BatchJob::MultiRecord do
    setup do
      BatchJob::MultiRecord.destroy_all
      @data = [
        [ 'col1',  'col2',   'col3' ],
        [ 'vala1', 'vala2', 'vala3' ],
        [ 'valb1', 'valb2', 'valb3' ],
        [ 'valc1', 'valc2', 'valc3' ],
      ]
      @description = 'Hello World'
      @job = BatchJob::MultiRecord.new(
        description: @description
      )
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

    should '#<<' do
      @data.each { |row| @job << row }

      assert_equal @data.size, @job.record_count
      assert_equal @data.size, @job.records_collection.count
    end

    context '#load_records' do
      should 'support block size of 1' do
        @job.block_size = 1
        blocks = @data.dup
        result = @job.load_records { blocks.shift }
        assert_equal (1..4), result

        assert_equal @data.size, @job.record_count
        assert_equal @data.size, @job.records_collection.count
      end

      should 'support block size of 2' do
        @job.block_size = 2
        blocks = @data.dup
        result = @job.load_records { blocks.shift }
        assert_equal (1..2), result

        assert_equal @data.size / 2, @job.record_count
        assert_equal @data.size / 2, @job.records_collection.count
      end
    end

    context '#work' do
      should 'read all records' do
        @data.each { |row| @job << row }
        @job.save!

        assert_equal @data.size, @job.record_count

        count = 0
        @job.work('server_name') do |data, header|
          assert_equal @data[count], data
          count += 1
        end
        assert_equal @data.size, count
      end

      should 'retry on exception' do
        @data.each { |row| @job << row }
        @job.save!

        assert_equal @data.size, @job.record_count

        count = 0
        @job.work('server_name') do |data, header|
          count += 1
          raise 'Oh no'
        end
        assert_equal @data.size, count
        # Should have been re-queued
        assert_equal @data.size, @job.records_collection.count

        count = 0
        #          @job.work('server_name', true) do |data, header|
        #            assert_equal @data[count], data
        #            assert_equal 1, header['retry_count']
        #            count += 1
        #          end
        #          assert_equal @data.size, count
      end
    end

    context '#load_stream' do
      setup do
        @array = [
          'this is some',
          'data',
          'that we can delimit',
          'as necessary'
        ]
        @job = BatchJob::MultiRecord.create(
          collect_results: true,
          repeatable:      true,
        )
      end

      teardown do
        @job.destroy if @job && !@job.new_record?
      end

      should 'handle empty streams' do
        str = ""
        stream = StringIO.new(str)
        @job.load_stream(stream)
        assert_equal 0, @job.records_collection.count
      end

      should 'handle a stream consisting only of the delimiter' do
        str = "\n"
        stream = StringIO.new(str)
        @job.load_stream(stream)
        assert_equal 1, @job.records_collection.count
        @job.each_record do |record, header|
          assert_equal [''], record
        end
      end

      should 'handle a linux stream' do
        str = @array.join("\n")
        stream = StringIO.new(str)
        @job.load_stream(stream)
        assert_equal 1, @job.records_collection.count
        @job.each_record do |record, header|
          assert_equal @array, record
        end
      end

      should 'handle a windows stream' do
        str = @array.join("\r\n")
        stream = StringIO.new(str)
        @job.load_stream(stream)
      end

      should 'handle a one line stream with a delimiter' do
        str = "hello\r\n"
        stream = StringIO.new(str)
        @job.load_stream(stream)
      end

      should 'handle a one line stream with no delimiter' do
        str = "hello"
        stream = StringIO.new(str)
        @job.load_stream(stream)
        assert_equal 1, @job.records_collection.count
        @job.each_record do |record, header|
          assert_equal [str], record
        end
      end

      should 'handle last line ending with a delimiter' do
        str = @array.join("\r\n")
        str << "\r\n"
        stream = StringIO.new(str)
        @job.load_stream(stream)
        assert_equal 1, @job.records_collection.count
        @job.each_record do |record, header|
          assert_equal @array, record
        end
      end

      should 'handle a block size of 1' do
        str = @array.join("\n")
        stream = StringIO.new(str)
        @job.load_stream(stream, block_size: 1)
        assert_equal @array.size, @job.records_collection.count, @job.records_collection.find.to_a
        index = 0
        @job.each_record do |record, header|
          assert_equal [ @array[index] ], record
          index += 1
        end
      end

      should 'handle a small stream the same size as block_size' do
        str = @array.join("\n")
        stream = StringIO.new(str)
        @job.load_stream(stream, block_size: @array.size)
        assert_equal 1, @job.records_collection.count, @job.records_collection.find.to_a
        @job.each_record do |record, header|
          assert_equal @array, record
        end
      end

      should 'handle a custom 1 character delimiter' do
        str = @array.join('$')
        stream = StringIO.new(str)
        @job.load_stream(stream, block_size: 1, delimiter: '$')
        assert_equal @array.size, @job.records_collection.count, @job.records_collection.find.to_a
        index = 0
        @job.each_record do |record, header|
          assert_equal [ @array[index] ], record
          index += 1
        end
      end

      should 'handle a custom multi-character delimiter' do
        delimiter = '$DELIMITER$'
        str = @array.join(delimiter)
        stream = StringIO.new(str)
        @job.load_stream(stream, block_size: 1, delimiter: delimiter)
        assert_equal @array.size, @job.records_collection.count, @job.records_collection.find.to_a
        index = 0
        @job.each_record do |record, header|
          assert_equal [ @array[index] ], record
          index += 1
        end
      end

      should 'compress records' do
        @job.compress = true

        str = @array.join("\n")
        stream = StringIO.new(str)
        @job.load_stream(stream, block_size: 1)
        assert_equal @array.size, @job.records_collection.count, @job.records_collection.find.to_a
        index = 0
        @job.each_record do |record, header|
          assert_equal [ @array[index] ], record
          index += 1
        end
        # Confirm that the data stored was actually compressed
        @job.records_collection.find_one do |record|
          assert_equal Zlib::Deflate.deflate(@array.first), record['data'].to_s, record.inspect
        end
      end

      should 'encrypt records' do
        @job.encrypt = true

        str = @array.join("\n")
        stream = StringIO.new(str)
        @job.load_stream(stream, block_size: 1)
        assert_equal @array.size, @job.records_collection.count, @job.records_collection.find.to_a
        index = 0
        @job.each_record do |record, header|
          assert_equal [ @array[index] ], record
          index += 1
        end
        # Confirm that the data stored was actually encrypted
        @job.records_collection.find_one do |record|
          assert_equal SymmetricEncryption.cipher.binary_encrypt(@array.first, true, compress=false), record['data'].to_s, record.inspect
        end
      end

      should 'compress and encrypt records' do
        @job.encrypt = true
        @job.compress = true

        str = @array.join("\n")
        stream = StringIO.new(str)
        @job.load_stream(stream, block_size: 1)
        assert_equal @array.size, @job.records_collection.count, @job.records_collection.find.to_a
        index = 0
        @job.each_record do |record, header|
          assert_equal [ @array[index] ], record
          index += 1
        end
        # Confirm that the data stored was actually compressed & encrypted
        @job.records_collection.find_one do |record|
          assert_equal SymmetricEncryption.cipher.binary_encrypt(@array.first, true, compress=true), record['data'].to_s, record.inspect
        end
      end

    end

    context '#unload' do
      setup do
        @array = [
          'this is some',
          'data',
          'that we can delimit',
          'as necessary'
        ]
        @job = BatchJob::MultiRecord.create(
          collect_results: true,
          repeatable:      true,
        )
      end

      teardown do
        @job.destroy if @job && !@job.new_record?
      end

      should 'handle no results' do
        stream = StringIO.new('')
        @job.unload(stream)
        assert_equal "", stream.string, stream.string.inspect
      end

      should 'handle 1 result' do
        @job << [ @array.first ]
        @job.work('worker') { |block| block }
        stream = StringIO.new('')
        @job.unload(stream)
        assert_equal @array.first + "\n", stream.string, stream.string.inspect
      end

      should 'handle many results' do
        @job.block_size = 1
        blocks = @array.dup
        result = @job.load_records { blocks.shift }
        @job.work('worker') { |block| block }
        stream = StringIO.new('')
        @job.unload(stream)
        assert_equal @array.join("\n") + "\n", stream.string, stream.string.inspect
      end

      should 'decompress results' do
        @job.compress = true
        @job.block_size = 1
        blocks = @array.dup
        result = @job.load_records { blocks.shift }
        @job.work('worker') { |block| block }
        stream = StringIO.new('')
        @job.unload(stream)
        assert_equal @array.join("\n") + "\n", stream.string, stream.string.inspect
      end

      should 'decrypt results' do
        @job.encrypt = true
        @job.block_size = 1
        blocks = @array.dup
        result = @job.load_records { blocks.shift }
        @job.work('worker') { |block| block }
        stream = StringIO.new('')
        @job.unload(stream)
        assert_equal @array.join("\n") + "\n", stream.string, stream.string.inspect
      end

      should 'decompress & decrypt results' do
        @job.compress = true
        @job.encrypt = true
        @job.block_size = 1
        blocks = @array.dup
        result = @job.load_records { blocks.shift }
        @job.work('worker') { |block| block }
        stream = StringIO.new('')
        @job.unload(stream)
        assert_equal @array.join("\n") + "\n", stream.string, stream.string.inspect
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