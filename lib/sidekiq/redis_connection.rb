require 'connection_pool'
require 'redis'
require 'redis/namespace'

module Sidekiq
  class RedisConnection
    include Sidekiq::DataStore

    def self.create(options={})
      RedisConnection.new(options)
    end

    def self.build_client(url, namespace)
      client = Redis.connect(:url => url)
      if namespace
        Redis::Namespace.new(namespace, :redis => client)
      else
        client
      end
    end
    private_class_method :build_client
  end

  def initialize (options={})
    url = options[:url] || ENV['REDISTOGO_URL'] || 'redis://localhost:6379/0'
    # need a connection for Fetcher and Retry
    size = options[:size] || (Sidekiq.server? ? (Sidekiq.options[:concurrency] + 2) : 5)

    @pool = ConnectionPool.new(:timeout => 1, :size => size) do
      build_client(url, options[:namespace])
    end
  end

  def push_job(queue, payload)
    pool.with do |conn|
      _, pushed = conn.multi do
        conn.sadd('queues', queue)
        conn.rpush("queue:#{queue}", payload)
      end
      pushed
    end
  end

  def enqueue(queue, message)
    pool.with do |conn|
      conn.lpush("queue:#{queue}", message)
    end
  end

  def clear_workers(process_id)
    pool.with do |conn|
      workers = conn.smembers('workers')
      workers.each do |name|
        conn.srem('workers', name) if name =~ /:#{process_id}-/
      end
    end
  end

  def fail_job(job_data)
    pool.with do |conn|
      conn.rpush(:failed, Sidekiq.dump_json(job_data))
    end
  end

  def retry(job_data, time)
    pool.with do |conn|
      conn.zadd('retry', time, job_data)
    end
  end

  def registered_queues
    pool.with { |conn| conn.smembers('queues') }
  end

  def registered_workers
    pool.with { |conn| conn.smembers('workers')}
  end

  attr_reader :pool
end
