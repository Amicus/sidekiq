require 'sidekiq'
require 'sidekiq/util'
require 'celluloid'
require 'multi_json'

module Sidekiq
  ##
  # Sidekiq's retry support assumes a typical development lifecycle:
  # 0. push some code changes with a bug in it
  # 1. bug causes message processing to fail, sidekiq's middleware captures
  #    the message and pushes it onto a retry queue
  # 2. sidekiq retries messages in the retry queue multiple times with
  #    an exponential delay, the message continues to fail
  # 3. after a few days, a developer deploys a fix.  the message is
  #    reprocessed successfully.
  # 4. if 3 never happens, sidekiq will eventually give up and throw the
  #    message away.
  module Retry

    # delayed_job uses the same basic formula
    MAX_COUNT = 25
    DELAY = proc { |count| (count ** 4) + 15 }
    POLL_INTERVAL = 15

    ##
    # The Poller checks Redis every N seconds for messages in the retry
    # set have passed their retry timestamp and should be retried.  If so, it
    # just pops the message back onto its original queue so the
    # workers can pick it up like any other message.
    class Poller
      include Celluloid
      include Sidekiq::Util

      def poll
        watchdog('retry poller thread died!') do

          Sidekiq.data_store.poll

          after(POLL_INTERVAL) { poll }
        end
      end
    end
  end
end
