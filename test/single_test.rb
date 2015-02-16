require_relative 'test_helper'
require_relative 'workers/single'

# Unit Test for BatchJob::Single
class SingleTest < Minitest::Test
  context BatchJob::Single do
    setup do
      @server = BatchJob::Server.new
      @server.started
      @description = 'Hello World'
      @arguments   = [ 1 ]
      @job = BatchJob::Single.new(
        description:         @description,
        klass:               'Workers::Single',
        arguments:           @arguments,
        destroy_on_complete: false
      )
    end

    teardown do
      @job.destroy if @job && !@job.new_record?
    end

    context '.config' do
      should 'support multiple databases' do
        assert_equal 'test_batch_job', BatchJob::Single.collection.db.name
      end
    end

    context '#save!' do
      should 'save a blank job' do
        @job.save!
        assert_nil   @job.server
        assert_nil   @job.completed_at
        assert       @job.created_at
        assert_equal @description, @job.description
        assert_equal false, @job.destroy_on_complete
        assert_nil   @job.expires_at
        assert_nil   @job.group
        assert_equal @arguments, @job.arguments
        assert_equal 0, @job.percent_complete
        assert_equal 50, @job.priority
        assert_equal true, @job.repeatable
        assert_equal 0, @job.failure_count
        assert_nil   @job.run_at
        assert_nil   @job.schedule
        assert_nil   @job.started_at
        assert_equal :queued, @job.state
      end
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

    context '#work' do
      should 'call default perform method' do
        @job.start!
        assert_equal 1, @job.work(@server)
        assert_equal true, @job.completed?
        assert_equal 2,    Workers::Single.result
      end

      should 'call specific method' do
        @job.method = :sum
        @job.arguments = [ 23, 45 ]
        @job.start!
        assert_equal 1, @job.work(@server)
        assert_equal true, @job.completed?
        assert_equal 68,    Workers::Single.result
      end

      should 'destroy on complete' do
        @job.destroy_on_complete = true
        @job.start!
        assert_equal 1, @job.work(@server)
        assert_equal nil, BatchJob::Single.find_by_id(@job.id)
      end

      should 'silence logging when log_level is set' do
        @job.destroy_on_complete = true
        @job.log_level           = :warn
        @job.method              = :noisy_logger
        @job.arguments           = []
        @job.start!
        logged = false
        Workers::Single.logger.stub(:log_internal, -> { logged = true }) do
          assert_equal 1, @job.work(@server)
        end
        assert_equal false, logged
      end

      should 'raise logging when log_level is set' do
        @job.destroy_on_complete = true
        @job.log_level           = :trace
        @job.method              = :debug_logging
        @job.arguments           = []
        @job.start!
        logged = false
        # Raise global log level to :info
        SemanticLogger.stub(:default_level_index, 3) do
          Workers::Single.logger.stub(:log_internal, -> { logged = true }) do
            assert_equal 1, @job.work(@server)
          end
        end
        assert_equal false, logged
      end
    end

  end
end
