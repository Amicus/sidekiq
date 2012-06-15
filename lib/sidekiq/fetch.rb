require 'sidekiq'
require 'celluloid'

module Sidekiq
  ##
  # The Fetcher blocks on Redis, waiting for a message to process
  # from the queues.  It gets the message and hands it to the Manager
  # to assign to a ready Processor.
  class Fetcher
    include Celluloid
    include Sidekiq::Util

    TIMEOUT = 1

    def initialize(mgr, queues)
      @mgr = mgr
      #TODO: This breaks redis backend
      @queues = queues  #.map { |q| "queue:#{q}" }
      @unique_queues = @queues.uniq
    end

    # Fetching is straightforward: the Manager makes a fetch
    # request for each idle processor when Sidekiq starts and
    # then issues a new fetch request every time a Processor
    # finishes a message.
    #
    # Because we have to shut down cleanly, we can't block
    # forever and we can't loop forever.  Instead we reschedule
    # a new fetch if the current fetch turned up nothing.
    def fetch
      watchdog('Fetcher#fetch died') do
        begin
          queue = nil
          msg = nil
          job = Sidekiq.data_store.pop_message(*queues_cmd)
          if job
            queue = job[:queue]
            msg = job[:message]
            @mgr.assign!(msg, queue.gsub(/.*queue:/, ''))
          else
            after(0) { fetch } unless @mgr.stopped?
          end
        rescue => ex
          logger.error("Error fetching message: #{ex}")
          logger.error(ex.backtrace.first)
          sleep(TIMEOUT)
          after(0) { fetch } unless @mgr.stopped?
        end
      end
    end

    private

    # Creating the Redis#blpop command takes into account any
    # configured queue weights. By default Redis#blpop returns
    # data from the first queue that has pending elements. We
    # recreate the queue command each time we invoke Redis#blpop
    # to honor weights and avoid queue starvation.
    def queues_cmd
      queues = @queues.sample(@unique_queues.size).uniq
      queues.concat(@unique_queues - queues)
      queues << TIMEOUT
    end
  end
end
