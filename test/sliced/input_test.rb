require_relative '../test_helper'

# Unit Test for RocketJob::SlicedJob
module Sliced
  class InputTest < Minitest::Test
    context RocketJob::Sliced::Input do
      setup do
        @input = RocketJob::Sliced::Input.new(
          name:       'rocket_job.slices.test',
          slice_size: 2
        )
        @input.clear
        @server_name = 'ThisIsMe'
      end

      teardown do
        @input.drop
      end

      context 'initialize' do
        should 'create index' do
          assert @input.collection.index_information['state_1__id_1'], 'must have state and _id index'
        end
      end

      context '#upload' do
        setup do
          skip
        end

        context 'file' do
          should 'text' do
          end

          should 'zip' do
          end

          should 'gzip' do
          end

          should 'decrypt' do
          end

          should 'autodetect zip' do
          end

          should 'autodetect text' do
          end
        end

        context 'stream' do
          should 'text' do
          end

          should 'zip' do
          end

          should 'gzip' do
          end

          should 'decrypt' do
          end

          should 'raise exception when autodetect' do
          end
        end
      end

      context '#upload_records' do
        should 'upload records according to slice_size' do
          records = (1..10).to_a
          @input.upload_records do
            records.shift
          end
          assert_equal 5, @input.size
          assert_equal [1,2], @input.first.to_a
          assert_equal [9,10], @input.last.to_a
        end

        should 'upload no records' do
          @input.upload_records do
            nil
          end
          assert_equal 0, @input.size
        end

        should 'upload odd records according to slice_size' do
          records = (1..11).to_a
          @input.upload_records do
            records.shift
          end
          assert_equal 6, @input.size
          assert_equal [1,2], @input.first.to_a
          assert_equal [11], @input.last.to_a
        end
      end

      context 'counts' do
        should 'count jobs' do
          @first = RocketJob::Sliced::Slice.new(records: ['hello', 'world'])
          @input << @first
          assert_equal 1, @input.size

          @second = RocketJob::Sliced::Slice.new(records: ['more', 'records', 'and', 'more'])
          @input << @second
          assert_equal 2, @input.size

          @third = RocketJob::Sliced::Slice.new(records: ['this', 'is', 'the', 'last'])
          @input << @third
          assert_equal 3, @input.size

          assert_equal 3, @input.queued_count
          assert_equal 0, @input.active_count
          assert_equal 0, @input.failed_count

          assert slice = @input.next_slice(@server_name)
          assert_equal @first.id, slice.id
          assert_equal true, slice.running?
          assert_equal 2, @input.queued_count
          assert_equal 1, @input.active_count
          assert_equal 0, @input.failed_count

          assert slice = @input.next_slice(@server_name)
          assert_equal @second.id, slice.id
          assert_equal true, slice.running?
          assert_equal 1, @input.queued_count
          assert_equal 2, @input.active_count
          assert_equal 0, @input.failed_count

          slice.failure
          @input.update(slice)
          failed_slice = slice
          assert_equal 1, @input.queued_count
          assert_equal 1, @input.active_count
          assert_equal 1, @input.failed_count

          assert slice = @input.next_slice(@server_name)
          assert_equal @third.id, slice.id
          assert_equal true, slice.running?
          assert_equal 0, @input.queued_count
          assert_equal 2, @input.active_count
          assert_equal 1, @input.failed_count

          assert_equal nil, @input.next_slice(@server_name)
          assert_equal 0, @input.queued_count
          assert_equal 2, @input.active_count
          assert_equal 1, @input.failed_count

          failed_slice.retry
          @input.update(failed_slice)
          assert_equal true, failed_slice.queued?
          assert_equal 1, @input.queued_count
          assert_equal 2, @input.active_count
          assert_equal 0, @input.failed_count

          assert slice = @input.next_slice(@server_name)
          assert_equal @second.id, slice.id
          assert_equal true, slice.running?
          assert_equal 0, @input.queued_count
          assert_equal 3, @input.active_count
          assert_equal 0, @input.failed_count
        end
      end

      context '#each_failed_record' do
        should 'return the correct failed record' do
          @first = RocketJob::Sliced::Slice.new(records: ['hello', 'world'])
          @first.start
          @input << @first
          assert_equal 1, @input.size

          @second = RocketJob::Sliced::Slice.new(records: ['more', 'records', 'and', 'more'])
          @second.start
          @input << @second
          assert_equal 2, @input.size

          @third = RocketJob::Sliced::Slice.new(records: ['this', 'is', 'the', 'last'])
          @third.start
          @input << @third
          assert_equal 3, @input.size

          exception = nil
          begin
            blah
          rescue Exception => exc
            exception = exc
          end

          @second.failure(exception, 2)
          @input.update(@second)
          count = 0
          @input.each_failed_record do |record, slice|
            count += 1
            assert_equal 'records', record
            assert_equal @second.id, slice.id
            assert_equal @second.to_a, slice.to_a
          end
          assert_equal 1, count, 'No failed records returned'
        end
      end

      context '#requeue_failed' do
        should 'requeue failed slices' do
          @first = RocketJob::Sliced::Slice.new(records: ['hello', 'world'])
          @input << @first
          assert_equal 1, @input.size

          @second = RocketJob::Sliced::Slice.new(records: ['more', 'records', 'and', 'more'])
          @input << @second
          assert_equal 2, @input.size

          @third = RocketJob::Sliced::Slice.new(records: ['this', 'is', 'the', 'last'])
          @input << @third
          assert_equal 3, @input.size

          exception = nil
          begin
            blah
          rescue Exception => exc
            exception = exc
          end

          @second.start
          @second.failure(exception, 2)
          @input.update(@second)

          assert_equal 2, @input.queued_count
          assert_equal 0, @input.active_count
          assert_equal 1, @input.failed_count

          assert_equal 1, @input.requeue_failed

          assert_equal 3, @input.queued_count
          assert_equal 0, @input.active_count
          assert_equal 0, @input.failed_count
        end
      end

      context '#requeue_running' do
        should 'requeue running slices' do
          @first = RocketJob::Sliced::Slice.new(records: ['hello', 'world'], server_name: @server_name)
          @first.start
          @input << @first
          assert_equal 1, @input.size

          @second = RocketJob::Sliced::Slice.new(records: ['more', 'records', 'and', 'more'], server_name: @server_name)
          @input << @second
          assert_equal 2, @input.size

          @third = RocketJob::Sliced::Slice.new(records: ['this', 'is', 'the', 'last'], server_name: 'other')
          @third.start
          @input << @third
          assert_equal 3, @input.size

          assert_equal 1, @input.queued_count
          assert_equal 2, @input.active_count
          assert_equal 0, @input.failed_count

          assert_equal 1, @input.requeue_running(@server_name)

          assert_equal 2, @input.queued_count
          assert_equal 1, @input.active_count
          assert_equal 0, @input.failed_count
        end
      end

      context '#next_slice' do
        should 'return the next available slice' do
          assert_equal nil, @input.next_slice(@server_name)

          @first = RocketJob::Sliced::Slice.new(records: ['hello', 'world'])
          @input << @first
          @second = RocketJob::Sliced::Slice.new(records: ['more', 'records', 'and', 'more'])
          @input << @second
          @third = RocketJob::Sliced::Slice.new(records: ['this', 'is', 'the', 'last'])
          @third.start
          @input << @third
          assert_equal 3, @input.size

          assert slice = @input.next_slice(@server_name)
          assert_equal @first.id, slice.id
          assert_equal true, slice.running?
          assert_equal @server_name, slice.server_name
          slice = @input.find(slice.id)
          assert_equal true, slice.running?
          assert_equal @server_name, slice.server_name

          assert slice = @input.next_slice(@server_name)
          assert_equal @second.id, slice.id
          assert_equal true, slice.running?
          assert_equal @server_name, slice.server_name
          slice = @input.find(slice.id)
          assert_equal true, slice.running?
          assert_equal @server_name, slice.server_name

          assert_equal nil, @input.next_slice(@server_name)
          assert_equal 3, @input.size
        end
      end

    end
  end
end