# encoding: UTF-8
require_relative '../test_helper'

# Unit Test for RocketJob::SlicedJob
module Sliced
  class SliceTest < Minitest::Test
    context RocketJob::Sliced::Slice do
      setup do
        @slice = RocketJob::Sliced::Slice.new
        assert_equal 0, @slice.size
      end

      context '#size' do
        should 'return the records size' do
          @slice << 'hello'
          assert_equal 1, @slice.size
        end
      end

      context '#<<' do
        should 'add records' do
          assert_equal 0, @slice.size
          @slice << 'hello'
          assert_equal 1, @slice.size
          @slice.insert('next')
          assert_equal 2, @slice.size
          assert_equal 'hello', @slice.first
        end
      end

      context '#to_a' do
        should 'return the array of records' do
          @slice << 'hello'
          @slice << 'world'
          assert_equal 2, @slice.size
          arr = @slice.to_a
          assert_equal ['hello', 'world'], arr, arr.inspect
        end
      end

      context '#records' do
        should 'return the array of records' do
          @slice << 'hello'
          @slice << 'world'
          assert_equal 2, @slice.size
          arr = @slice.records
          assert_equal ['hello', 'world'], arr, arr.inspect
        end

        should 'set the array of records' do
          @slice << 'hello'
          @slice << 'world'
          assert_equal 2, @slice.size
          @slice.records = ['hello', 'world']
          assert_equal 2, @slice.size
          arr = @slice.records
          assert_equal ['hello', 'world'], arr, arr.inspect
        end
      end

      context '#failure' do
        should 'without exception' do
          @slice.start
          @slice.server_name = 'me'
          assert_equal nil, @slice.failure_count
          @slice.failure
          assert_equal 1, @slice.failure_count
          assert_equal nil, @slice.exception
        end

        should 'with exception' do
          @slice.start
          @slice.server_name = 'me'
          exception = nil
          begin
            blah
          rescue Exception => exc
            exception = exc
          end
          @slice.failure(exception, 21)
          assert_equal 1, @slice.failure_count
          assert @slice.exception
          assert_equal exception.class.name, @slice.exception.class_name
          assert_equal exception.message,    @slice.exception.message
          assert_equal exception.backtrace,  @slice.exception.backtrace
          assert_equal 'me', @slice.exception.server_name
          assert_equal 21,   @slice.exception.record_number
        end
      end

      context '#save' do
        should 'not allow save to be called' do
          assert_raises NoMethodError do
            @slice.save!
          end
        end

        should 'not allow save via other methods' do
          assert_raises NoMethodError do
            @slice.start!
          end
        end
      end

      context 'serialization' do
        setup do
          @records = [ 'hello', 'world', 1, 3.25, Time.now.utc, [1,2], { 'a' => 43 }, true, false, :symbol, nil, /regexp/ ]
          @slice.concat(@records)
          @server_name = 'server'
          @slice.server_name = @server_name
          @failure_count = 3
          @slice.failure_count = @failure_count
          @exception = nil
          begin
            blah
          rescue Exception => exc
            @exception = exc
          end
          @slice.exception = RocketJob::JobException.from_exception(@exception)
          @slice.exception.record_number = 21
          assert_equal @records.size, @slice.size
          assert_equal @records, @slice.to_a, @slice.to_a.inspect
        end

        should 'plain' do
          assert bson = @slice.to_bson
          assert_equal :queued,        bson['state'], bson.inspect
          assert_equal @records,       bson['records'], bson.inspect
          assert_equal @server_name,   bson['server_name'], bson.inspect
          assert_equal @failure_count, bson['failure_count'], bson.inspect
          assert_equal @slice.id,      bson['_id'], bson.inspect

          slice = RocketJob::Sliced::Slice.from_bson(bson)
          assert_equal :queued,          slice.state, bson.inspect
          assert_equal @records.inspect, slice.to_a.inspect, bson.inspect
          assert_equal @server_name,     slice.server_name, bson.inspect
          assert_equal @failure_count,   slice.failure_count
          assert_equal @slice.id,        slice.id
          assert slice.exception
          assert_equal @exception.class.name, slice.exception.class_name
          assert_equal @exception.message,    slice.exception.message
          assert_equal @exception.backtrace,  slice.exception.backtrace
          assert_equal 21,                    slice.exception.record_number
        end

        should 'compressed' do
          assert bson = @slice.to_bson(compress: true)
          assert_equal :queued, bson['state'], bson.inspect
          str = BSON.serialize('r' => @slice.to_a)
          compressed = Zlib::Deflate.deflate(str.to_s)
          assert_equal compressed, bson['records'].to_s, bson.inspect

          slice = RocketJob::Sliced::Slice.from_bson(bson)
          assert_equal :queued,          slice.state, bson.inspect
          assert_equal @records.inspect, slice.to_a.inspect, bson.inspect
          assert_equal @server_name,     slice.server_name, bson.inspect
          assert_equal @failure_count,   slice.failure_count
          assert_equal @slice.id,        slice.id
          assert slice.exception
          assert_equal @exception.class.name, slice.exception.class_name
          assert_equal @exception.message,    slice.exception.message
          assert_equal @exception.backtrace,  slice.exception.backtrace
          assert_equal 21,                    slice.exception.record_number
        end

        should 'encrypted' do
          assert bson = @slice.to_bson(encrypt: true)
          assert_equal :queued, bson['state'], bson.inspect
          str = BSON.serialize('r' => @slice.to_a)
          decrypted = SymmetricEncryption.cipher.binary_decrypt(bson['records'].to_s)
          assert_equal str, decrypted, bson.inspect

          slice = RocketJob::Sliced::Slice.from_bson(bson)
          assert_equal :queued,          slice.state, bson.inspect
          assert_equal @records.inspect, slice.to_a.inspect, bson.inspect
          assert_equal @server_name,     slice.server_name, bson.inspect
          assert_equal @failure_count,   slice.failure_count
          assert_equal @slice.id,        slice.id
          assert slice.exception
          assert_equal @exception.class.name, slice.exception.class_name
          assert_equal @exception.message,    slice.exception.message
          assert_equal @exception.backtrace,  slice.exception.backtrace
          assert_equal 21,                    slice.exception.record_number
        end

        should 'encrypted and compressed' do
          assert bson = @slice.to_bson(encrypt: true, compress: true)
          assert_equal :queued, bson['state'], bson.inspect
          str = BSON.serialize('r' => @slice.to_a)
          decrypted = SymmetricEncryption.cipher.binary_decrypt(bson['records'].to_s)
          assert_equal str, decrypted, bson.inspect

          slice = RocketJob::Sliced::Slice.from_bson(bson)
          assert_equal :queued,          slice.state, bson.inspect
          assert_equal @records.inspect, slice.to_a.inspect, bson.inspect
          assert_equal @server_name,     slice.server_name, bson.inspect
          assert_equal @failure_count,   slice.failure_count
          assert_equal @slice.id,        slice.id
          assert slice.exception
          assert_equal @exception.class.name, slice.exception.class_name
          assert_equal @exception.message,    slice.exception.message
          assert_equal @exception.backtrace,  slice.exception.backtrace
          assert_equal 21,                    slice.exception.record_number
        end
      end

      should 'transition states' do
        assert_equal :queued, @slice.state
        @slice.start
        assert_equal :running, @slice.state
        @slice.fail
        assert_equal :failed, @slice.state
        @slice.retry
        assert_equal :running, @slice.state
        @slice.complete
        assert_equal :completed, @slice.state
      end

    end
  end
end
