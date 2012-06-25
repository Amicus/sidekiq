require 'digest'
require 'multi_json'

module Sidekiq
  module Middleware
    module Client
      class UniqueJobs
        HASH_KEY_EXPIRATION = 30 * 60

        #TODO: this isn't implemented in Mongo yet
        def call(worker_class, item, queue)
          enabled = worker_class.get_sidekiq_options['unique']
          if enabled
            payload_hash = Digest::MD5.hexdigest(Sidekiq.dump_json(item))
            unique = Sidekiq.data_store.job_taken?(payload_hash, HASH_KEY_EXPIRATION)
            yield if unique
          else
            yield
          end
        end

      end
    end
  end
end
