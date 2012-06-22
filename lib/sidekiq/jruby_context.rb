module Sidekiq
  class JrubyContext
    include Util
    include Celluloid

    attr_accessor :instance

    def initialize
      @instance = org.jruby.embed.ScriptingContainer.new(org.jruby.embed.LocalContextScope::THREADSAFE)
      @instance.set_compat_version(org.jruby.CompatVersion::RUBY1_9)
      @instance.set_argv(ARGV)
      @instance.run_scriptlet("ENV.merge(#{ENV.to_hash.to_s})")
    end

    def run(code)
      @instance.run_scriptlet(code)
    end
    alias :run_scriptlet :run

    def threaded_run(code)
      Thread.new do
        @instance.run_scriptlet(code)
      end
    end

    def parse(code)
      @instance.parse(code)
    end

    def finalize
      @instance.finalize
    end

    def terminate
      @instance.terminate
    end

  end
end
