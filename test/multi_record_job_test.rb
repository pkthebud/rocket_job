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

  end
end