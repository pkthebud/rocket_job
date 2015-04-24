require_relative '../test_helper'

# Unit Test for RocketJob::SlicedJob
module Sliced
  class OutputTest < Minitest::Test
    context RocketJob::Sliced::Output do
      setup do
        @output = RocketJob::Sliced::Output.new(
          name:       'rocket_job.slices.test',
          slice_size: 2
        )
        @output.clear
      end

      teardown do
        @output.drop
      end

      context '#download' do
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

    end
  end
end