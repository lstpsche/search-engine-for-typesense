# frozen_string_literal: true

module SearchEngine
  module Logging
    # Shared terminal cursor hide/show management for animated output.
    #
    # Ensures at most one +at_exit+ handler is registered globally to restore
    # cursor visibility, regardless of how many {Spinner} or {LiveRenderer}
    # instances are created during a process lifetime.
    #
    # @since M9
    module CursorGuard
      @registered = false
      @mutex = Mutex.new

      module_function

      # Hide the terminal cursor and register a safety restore via +at_exit+.
      #
      # @param io [IO] output stream
      # @return [void]
      def hide(io)
        io.write("\e[?25l")
        io.flush
        register_restore(io)
      end

      # Restore the terminal cursor.
      #
      # @param io [IO] output stream
      # @return [void]
      def show(io)
        io.write("\e[?25h")
        io.flush
      end

      # Register a one-time +at_exit+ handler to restore cursor visibility.
      # Thread-safe; only the first call has any effect.
      #
      # @param io [IO] output stream
      # @return [void]
      def register_restore(io)
        @mutex.synchronize do
          return if @registered

          at_exit { io.write("\e[?25h") if io.respond_to?(:write) }
          @registered = true
        end
      end
    end
  end
end
