require 'csv'
module RocketJob
  module Utility
    # For parsing a single line of CSV at a time
    # 2 to 3 times better performance than CSV.parse_line and considerably less
    # garbage collection required
    class CSVRow
      # CSV cannot read just a single line without building a new CSV object every time
      # Fake it out by re-winding a stream and replace the line after every read
      # Hard code the row separator so that it does not try to auto-detect it
      def initialize(encoding=RocketJob::UTF8_ENCODING)
        @io  = StringIO.new(''.force_encoding(encoding))
        @csv = CSV.new(@io, row_sep: '')
      end

      # Parse a single line of CSV data
      # Parameters
      #   line [String]
      #     A single line of CSV data without any line terminators
      def parse(line)
        @io.rewind
        @io.truncate(0)
        @io << line
        @io.rewind
        @csv.shift
      end

      # Return the supplied array as a single line CSV string
      def to_csv(array)
        @io.rewind
        @io.truncate(0)
        @csv << array
        @io.rewind
        @io.string
      end
    end

  end
end
