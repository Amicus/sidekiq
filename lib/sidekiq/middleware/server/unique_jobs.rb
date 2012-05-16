require 'multi_json'

module Sidekiq
  module Middleware
    module Server
      class UniqueJobs
        def call(*args)
          yield
        ensure
          json = Sidekiq.dump_json(args[1])
          hash = Digest::MD5.hexdigest(json)
          Sidekiq.data_store.forget_job(hash)
        end
      end
    end
  end
end
