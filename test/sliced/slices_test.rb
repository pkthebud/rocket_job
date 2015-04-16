require_relative '../test_helper'

# Unit Test for RocketJob::SlicedJob
module Sliced
  class SlicesTest < Minitest::Test
    context RocketJob::Sliced::Slices do
      setup do
        @slices = RocketJob::Sliced::Slices.new(
          name:       'rocket_job.slices.test',
          slice_size: 2
        )
        @slices.collection.remove({})
        assert_equal 0, @slices.size

        @first = RocketJob::Sliced::Slice.new
        @first << 'hello'
        @first << 'world'
        @slices << @first
        assert_equal 1, @slices.size

        @second = RocketJob::Sliced::Slice.new
        @second << 'more'
        @second << 'records'
        @second << 'and'
        @second << 'more'
        @slices << @second
        assert_equal 2, @slices.size

        @third = RocketJob::Sliced::Slice.new
        @third << 'this'
        @third << 'is'
        @third << 'the'
        @third << 'last'
        @slices << @third
        assert_equal 3, @slices.size

        # TODO
        #   compress: true
        #   encrypt: true
      end

      teardown do
        @slices.destroy
      end

      context '#count' do
        should 'count slices' do
          assert_equal 3, @slices.count
          assert_equal 3, @slices.size
          assert_equal 3, @slices.length
        end
      end

      context '#each' do
        should 'count slices' do
          count = 0
          @slices.each do |slice|
            count += 1
          end
          assert_equal 3, count
        end
      end

      context '#first' do
        should 'return the first slice' do
          assert slice = @slices.first
          assert_equal @first.id, slice.id
          assert_equal @first.to_a, slice.to_a
        end
      end

      context '#last' do
        should 'return the last slice' do
          assert slice = @slices.last
          assert_equal @third.id, slice.id
          assert_equal @third.to_a, slice.to_a
        end
      end

      context '#<<' do
        should 'insert a slice' do
          count = @slices.count
          @slices << RocketJob::Sliced::Slice.new(records: [1,2,3,4])
          assert_equal count + 1, @slices.count
        end
        should 'insert an array of records as a new slice' do
          count = @slices.count
          @slices << [1,2,3,4]
          assert_equal count + 1, @slices.count
        end
      end

      context '#insert' do
        should 'insert a slice' do
          count = @slices.count
          @slices.insert(RocketJob::Sliced::Slice.new(records: [1,2,3,4]))
          assert_equal count + 1, @slices.count
        end
        should 'insert an array of records as a new slice' do
          count = @slices.count
          @slices.insert([1,2,3,4])
          assert_equal count + 1, @slices.count
        end
      end

      context '#remove' do
        should 'remove a specific slice' do
          assert_equal 3, @slices.count
          @slices.remove(@second)
          assert_equal 2, @slices.count
          assert_equal @first.id, @slices.first.id
          assert_equal @third.id, @slices.last.id
        end
      end

      context '#destroy' do
        should 'destroy all slices in this collection' do
          assert_equal 3, @slices.count
          @slices.destroy
          assert_equal 0, @slices.count
        end
      end

      context '#update' do
        should 'update a specific slice' do
          assert_equal 3, @slices.count
          assert_equal @first.id, @slices.first.id
          assert_equal 2, @first.count
          @first << 'one more'
          assert_equal 3, @first.count
          @slices.update(@first)
          assert_equal 3, @slices.count
          assert first = @slices.first
          assert_equal first.id, @first.id
          assert_equal 3, first.count
          assert_equal 'one more', first.last
        end
      end

    end
  end
end