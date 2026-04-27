# frozen_string_literal: true

module SearchEngine
  module Logging
    # Thread-safe output routing for indexation progress logging.
    #
    # When the instrumentation context includes `bulk_silent: true`,
    # all output is routed to {File::NULL}. Otherwise, output goes to `$stdout`.
    # Error output via `warn()` is unaffected — it always reaches `$stderr`.
    #
    # @since M10
    module Output
      NULL_IO = File.open(File::NULL, 'w')

      module_function

      # @return [Boolean] true when the current thread context indicates silent mode
      def silent?
        SearchEngine::Instrumentation.context[:bulk_silent] == true
      end

      # @return [IO] the appropriate output stream for progress logging
      def io
        silent? ? NULL_IO : $stdout
      end

      # Write a line to the progress output (suppressed in silent mode).
      # @param args [Array] arguments forwarded to `IO#puts`
      # @return [nil]
      def puts(*args)
        io.puts(*args)
      end
    end
  end
end
