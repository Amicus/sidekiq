require 'multi_json'

module Sidekiq
  module Middleware
    module Server
      class FailureJobs
        def call(*args)
          yield
        rescue => e
          data = {
            :failed_at => Time.now.strftime("%Y/%m/%d %H:%M:%S %Z"),
            :payload => args[1],
            :exception => e.class.to_s,
            :error => e.to_s,
            :backtrace => e.backtrace,
            :worker => args[1]['class'],
            :queue => args[2]
          }
          Sidekiq.data_store.fail_job(data)
          raise
        end
      end
    end
  end
end
