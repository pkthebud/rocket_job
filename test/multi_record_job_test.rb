require File.join(File.dirname(__FILE__), 'test_helper')

# Unit Test for BatchJob::MultiRecordJob
class MultiRecordJobTest < Minitest::Test
  context BatchJob::MultiRecordJob do
    setup do
      BatchJob::MultiRecordJob.destroy_all
      @data = [
        [ 'col1',  'col2',   'col3' ],
        [ 'vala1', 'vala2', 'vala3' ],
        [ 'valb1', 'valb2', 'valb3' ],
        [ 'valc1', 'valc2', 'valc3' ],
      ]
      @description = 'Hello World'
      @job = BatchJob::MultiRecordJob.new(
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

    should '#add_records' do
      result = @job.add_records(@data)
      assert_equal (1..4), result

      assert_equal @data.size, @job.record_count
      assert_equal @data.size, @job.records_collection.count
    end

    context '#process_records' do
      should 'read all records' do
        @data.each { |row| @job << row }
        @job.save!

        assert_equal @data.size, @job.record_count

        count = 0
        @job.process_records('server_name') do |data, header|
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
        @job.process_records('server_name') do |data, header|
          count += 1
          raise 'Oh no'
        end
        assert_equal @data.size, count
        # Should have been re-queued
        assert_equal @data.size, @job.records_collection.count

        count = 0
        #          @job.process_records('server_name', true) do |data, header|
        #            assert_equal @data[count], data
        #            assert_equal 1, header['retry_count']
        #            count += 1
        #          end
        #          assert_equal @data.size, count
      end
    end

    context '#add_text_stream' do
      setup do
        @array = [
          'this is some',
          'data',
          'that we can delimit',
          'as necessary'
        ]
        @job = BatchJob::MultiRecordJob.create(
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
        @job.add_text_stream(stream)
        assert_equal 0, @job.records_collection.count
      end

      should 'handle a stream consisting only of the delimiter' do
        str = "\n"
        stream = StringIO.new(str)
        @job.add_text_stream(stream)
        assert_equal 1, @job.records_collection.count
        assert_equal [''], @job.records_collection.find_one['data']
      end

      should 'handle a linux stream' do
        str = @array.join("\n")
        stream = StringIO.new(str)
        @job.add_text_stream(stream)
        assert_equal 1, @job.records_collection.count
        assert_equal @array, @job.records_collection.find_one['data']
      end

      should 'handle a windows stream' do
        str = @array.join("\r\n")
        stream = StringIO.new(str)
        @job.add_text_stream(stream)
      end

      should 'handle a one line stream with a delimiter' do
        str = "hello\r\n"
        stream = StringIO.new(str)
        @job.add_text_stream(stream)
      end

      should 'handle a one line stream with no delimiter' do
        str = "hello"
        stream = StringIO.new(str)
        @job.add_text_stream(stream)
        assert_equal 1, @job.records_collection.count
        assert_equal [str], @job.records_collection.find_one['data']
      end

      should 'handle last line ending with a delimiter' do
        str = @array.join("\r\n")
        str << "\r\n"
        stream = StringIO.new(str)
        @job.add_text_stream(stream)
        assert_equal 1, @job.records_collection.count
        assert_equal @array, @job.records_collection.find_one['data']
      end

      should 'handle a block size of 1' do
        str = @array.join("\n")
        stream = StringIO.new(str)
        @job.add_text_stream(stream, block_size: 1)
        assert_equal @array.size, @job.records_collection.count, @job.records_collection.find.to_a
        assert_equal @array.first, @job.records_collection.find_one['data']
      end

      should 'handle a small stream the same size as block_size' do
      end

      should 'handle a custom 1 character delimiter' do
      end

      should 'handle a custom multi-character delimiter' do
      end

      should 'compress records' do
        #assert_equal @array.join(@job.compress_delimiter), @job.records_collection.find_one['data']
      end

      should 'encrypt records' do
      end

      should 'compress and encrypt records' do
      end

    end

    context '#write_results' do
      should 'handle no results' do
      end

      should 'handle 1 result' do
      end

      should 'handle many results' do
      end

      should 'decompress results' do
      end

      should 'decrypt results' do
      end

      should 'decompress & decrypt results' do
      end

    end

    context '.config' do
      should 'support multiple databases' do
        assert_equal 'test_batch_job', BatchJob::MultiRecordJob.collection.db.name
        job = BatchJob::MultiRecordJob.new
        assert_equal 'test_batch_job_work', job.records_collection.db.name
        assert_equal 'test_batch_job_work', job.results_collection.db.name
      end
    end

  end
end