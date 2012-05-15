require 'rubygems'
require 'mongo'

module Sidekiq
  class MongoConection
    include Sidekiq::DataStore

    def self.create(options={})
      url = options[:url] || ENV['MONGOTOGO_URL']
      replica_set = options[:replica_set]
      # need a connection for Fetcher and Retry
      size = options[:size] || (Sidekiq.server? ? (Sidekiq.options[:concurrency] + 2) : 5)
      namespace = options[:namespace]
      if replica_set
        database = ReplSetConnection.new(replica_set, :read => :secondary,
                                         :pool_size => size, :timeout => 5).db(namespace)
      else
        database = new(url, :pool_size => size, :timeout => 5).db(namespace)
      end
    end
  end
end