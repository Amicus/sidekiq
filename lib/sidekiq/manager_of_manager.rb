require 'sidekiq/manager'
require 'java' #this is only for JRUBY!
require 'fileutils'

require 'sidekiq/context'

trap 'INT' do
  # Handle Ctrl-C in JRuby like MRI
  # http://jira.codehaus.org/browse/JRUBY-4637
  Thread.main.raise Interrupt
end

module Sidekiq
 class ManagerOfManager
   include Util
   #include Celluloid

   attr_accessor :monitor_path, :transitioning, :context #:primary_context, :secondary_context

   def initialize(monitor_path = "/tmp/reload_worker.txt")
     @monitor_path = monitor_path
     @transitioning = false
     @poll_timeout = 15
   end

   def start
     logger.info 'new context'
     new_context
     logger.info 'starting new context'
     startup_context(context)
     logger.info 'starting polling'
     start_polling_monitor
     logger.info 'polling monitor'
   end

   def start_polling_monitor
     begin
       while true
         logger.info('monitor poll')
         if !transitioning and File.exist?(monitor_path)
           logger.info 'transitioning'
           transitioning = true
           FileUtil.rm(monitor_path)
           old_context = context
           logger.info 'new context'
           new_context!
           logger.info 'startup context'
           startup_context(context)
           stop(old_context)
           finalize(old_context)
           transitioning = false
         end
         sleep @poll_timeout
       end
     rescue Interrupt
       stop(context)
       finalize(context)
       exit(0)
     end
   end

   def boot_strap_ruby
     <<-EOS
       require 'rubygems'
       require 'bundler'
       Bundler.setup
       require 'sidekiq/cli'
       cli = Sidekiq::CLI.instance
       cli.parse
       manager = nil
       poller = nil
       def bootstrap!
        manager = Sidekiq::Manager.new(Sidekiq.options)
        poller = Sidekiq::Retry::Poller.new
        puts 'manager start'
        manager.start!
        puts 'poller start'
        poller.start!
        puts "internal context did somethin"
        manager
       end
       bootstrap!
     EOS
   end

   def new_context
     @context = Context.new
   end

   #TODO: DRY this
   #def setup_secondary_context!
   #  secondary_context = org.jruby.embed.ScriptingContainer.new(org.jruby.embed.LocalContextScope::THREADSAFE)
   #  secondary_context.setCompatVersion(org.jruby.CompatVersion::RUBY1_9)
   #  secondary_context.run_scriptlet("ENV.merge(#{ENV.to_hash.to_s})")
   #  secondary_context
   #end

   def finalize_context(context)
     context.finalize
   end

   def stop(context)
     context.run_scriptlet("manager.stop(:shutdown => true, :timeout => 3600*24); poller.terminate if poller.alive?")
   end

   def startup_context(context)
     context.run_scriptlet!(boot_strap_ruby)
   end
 end
end
