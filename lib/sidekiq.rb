require 'sidekiq/version'
require 'sidekiq/client'
require 'sidekiq/worker'
require 'sidekiq/data_store'
require 'sidekiq/mongo_connection'
require 'sidekiq/redis_connection'
require 'sidekiq/util'

require 'sidekiq/extensions/action_mailer'
require 'sidekiq/extensions/active_record'
require 'sidekiq/rails' if defined?(::Rails)

module Sidekiq

  DEFAULTS = {
    :queues => [],
    :concurrency => 25,
    :require => '.',
    :environment => nil,
    :timeout => 8,
    :enable_rails_extensions => true,
  }

  def self.options
    @options ||= DEFAULTS.dup
  end

  def self.options=(opts)
    @options = opts
  end

  ##
  # Configuration for Sidekiq server, use like:
  #
  #   Sidekiq.configure_server do |config|
  #     config.redis = { :namespace => 'myapp', :size => 25, :url => 'redis://myhost:8877/mydb' }
  #     config.server_middleware do |chain|
  #       chain.add MyServerHook
  #     end
  #   end
  def self.configure_server
    yield self if server?
  end

  ##
  # Configuration for Sidekiq client, use like:
  #
  #   Sidekiq.configure_client do |config|
  #     config.redis = { :namespace => 'myapp', :size => 1, :url => 'redis://myhost:8877/mydb' }
  #   end
  def self.configure_client
    yield self unless server?
  end

  def self.server?
    defined?(Sidekiq::CLI)
  end

  def self.redis(&block)
    @redis ||= Sidekiq.data_store.pool
    raise ArgumentError, "requires a block" if !block
    @redis.with(&block)
  end

  def self.redis=(hash)
    if hash.is_a?(Hash)
      self.data_store = {:redis => hash}
      #@redis = RedisConnection.create(hash)
    elsif hash.is_a?(ConnectionPool)
      raise NotImplementedError, "No longer support passing in a connection pool"
      @redis = hash
    else
      raise ArgumentError, "redis= requires a Hash or ConnectionPool"
    end
  end

  def self.monitor=(monitor)
    options[:monitor] = monitor
  end

  def self.data_store
    @data_store ||= Sidekiq::DataStore.create
  end

  def self.data_store=(hash)
    if hash.is_a?(Hash)
      @redis = nil
      @data_store = Sidekiq::DataStore.create(hash)
    else
      raise ArgumentError, "data_store= needs to be a hash"
    end
  end

  def self.client_middleware
    @client_chain ||= Client.default_middleware
    yield @client_chain if block_given?
    @client_chain
  end

  def self.server_middleware
    @server_chain ||= Processor.default_middleware
    yield @server_chain if block_given?
    @server_chain
  end

  def self.load_json(string)
    MultiJson.decode(string)
  end

  def self.dump_json(object)
    MultiJson.encode(object)
  end

end
