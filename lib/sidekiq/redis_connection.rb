require 'connection_pool'
require 'redis'
require 'redis/namespace'

module Sidekiq
  module DataStore

    # Return the type of the data store backend.
    def name
      not_implemented
    end

    # Add the job held in 'payload' to the 'queue' to be processed
    def push_job(queue, payload)
      not_implemented
    end

    # queues the message held in 'msg' to the queue 'queue' to be
    # consumed later
    def enqueue(queue, message)
      not_implemented
    end

    # Unregisters workers with names that match the given process id
    def clear_workers(process_id)
      not_implemented
    end

    # Unregisters all registered workers.
    def clear_all_workers
      not_implemented
    end

    # Register in the data store that the job in 'job_data' has failed.
    def fail_job(job_data)
      not_implemented
    end

    # Schedule a previously failed job for retry
    def retry(job_data, time)
      not_implemented
    end

    # Return all the scheduled retries with the supplied 'score'
    def retries_with_score(score)
      not_implemented
    end

    # Return a list of names for all the queues that have been registered
    # for this data store.
    def registered_queues
      not_implemented
    end

    # Returns a list of queues, sorted by queue length
    def sorted_queues
      not_implemented
    end

    # Return a list of names for all the workers that have been registered
    # for this data store.
    def registered_workers
      not_implemented
    end

    # Return an array of worker-message pairs
    def worker_jobs
      not_implemented
    end

    # Return the processed job stats
    def processed_stats
      not_implemented
    end

    # Return the failed job stats
    def failed_stats
      not_implemented
    end

    # Return the number of pending retries scheduled.
    def pending_retry_count
      not_implemented
    end

    # Return the scheduled retry jobs.
    def pending_retries
      not_implemented
    end

    # Return the location of the data store.
    def location
      not_implemented
    end

    # Returns a list of the first 'n' messages of 'queue'
    def get_first(n, queue)
      not_implemented
    end

    # Purges the named queue
    def delete_queue(name)
      not_implemented
    end

    # Enqueue the retries scheduled for execution at 'time'
    def enqueue_scheduled_retries(time)
      not_implemented
    end

    # Delete the retries scheduled for execution at 'time'
    def delete_scheduled_retries(time)
      not_implemented
    end

    # Pops a message from one of the supplied queues
    def pop_message
      not_implemented
    end

    # Schedules the job from 'queue' to be performed by the 'worker'
    def process_job(worker, message, queue)
      not_implemented
    end

    # Register that this worker has failed it's last task
    def fail_worker(worker)
      not_implemented
    end

    # Clear the completed worker. 'dying' indicates if the worker
    # succeeded or not.
    def clear_worker(worker, dying)
      not_implemented
    end

    # Report if the job in 'hash' is already being processed
    def job_taken?(hash, expiration)
      not_implemented
    end

    # Unregister the job
    def forget_job(hash)
      not_implemented
    end


#    def not_implemented(method_name)
#      raise NotImplementedError method_name + "Not implemented!"
#    end

    def create(options={})
      RedisConnection.new(options)
    end
  end







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

    def clear_all_workers
      pool.with do |conn|
        workers = conn.smembers('workers')
        workers.each do |name|
          conn.srem('workers', name)
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

    def retries_with_score(score)
      pool.with do |conn|
        results = conn.zrangebyscore('retry', score, score)
        results.map { |msg| Sidekiq.load_json(msg) }
      end
    end

    def registered_queues
      pool.with { |conn| conn.smembers('queues') }
    end

    def sorted_queues
      queues = registered_queues
      pool.with do |conn|
        queues.map { |q|
          [q, conn.llen("queue:#{q}") || 0]
        }.sort { |x,y| x[1] <=> y[1] }
      end
    end

    def registered_workers
      pool.with { |conn| conn.smembers('workers') }
    end

    def worker_jobs
      workers = registered_workers
      pool.with { |conn|
        workers.map { |w|
          msg = conn.get("worker:#{w}")
          msg ? [w, Sidekiq.load_json(msg)] : nil
        }.compact.sort { |x| x[1] ? -1 : 1 }
      }
    end

    def processed_stats
      pool.with { |conn| conn.get('stat:processed') } || 0
    end

    def failed_stats
      pool.with { |conn| conn.get('stat:failed') } || 0
    end

    def pending_retry_count
      pool.with { |conn| conn.zcard('retry') }
    end

    def pending_retries
      pool.with do |conn|
        results = conn.zrange('retry', 0, 25, :withscores => true)
        results.each_slice(2).map { |msg, score| [Sidekiq.load_json(msg), Float(score)] }
      end
    end

    def location
      pool.with { |conn| conn.client.location }
    end

    def get_first(n, name)
      pool.with {|conn| conn.lrange("queue:#{name}", 0, n) }.map { |str| Sidekiq.load_json(str) }
    end

    def delete_queue(name)
      pool.with do |conn|
        conn.del("queue:#{name}")
        conn.srem("queues", name)
      end
    end

    def enqueue_scheduled_retries(time)
      pool.with do |conn|
        results = conn.zrangebyscore('retry', score, score)
        conn.zremrangebyscore('retry', score, score)
        results.map do |message|
          msg = Sidekiq.load_json(message)
          conn.rpush("queue:#{msg['queue']}", message)
        end
      end
    end

    def pop_message(*queues)
      pool.with { |conn| conn.blpop(*queues) }
    end

    def poll
    pool.with do |conn|
      # A message's "score" in Redis is the time at which it should be retried.
      # Just check Redis for the set of messages with a timestamp before now.
      messages = nil
      now = Time.now.to_f.to_s
      (messages, _) = conn.multi do
        conn.zrangebyscore('retry', '-inf', now)
        conn.zremrangebyscore('retry', '-inf', now)
      end

      messages.each do |message|
        logger.debug { "Retrying #{message}" }
        msg = Sidekiq.load_json(message)
        conn.rpush("queue:#{msg['queue']}", message)
      end
    end
    end

    def delete_scheduled_retries(time)
      pool.with do |conn|
        conn.zremrangebyscore('retry', time, time)
      end
    end

    def process_job(worker, message, queue)
      pool.with do |conn|
        conn.multi do
          conn.sadd('workers', worker)
          conn.setex("worker:#{worker}:started", DEFAULT_EXPIRY, Time.now.to_s)
          hash = {:queue => queue, :payload => message, :run_at => Time.now.strftime("%Y/%m/%d %H:%M:%S %Z")}
          conn.setex("worker:#{worker}", DEFAULT_EXPIRY, Sidekiq.dump_json(hash))
        end
      end
    end

    def fail_worker(worker)
      pool.with do |conn|
        conn.multi do
          conn.incrby("stat:failed", 1)
          conn.del("stat:processed:#{worker}")
        end
      end
    end

    def clear_worker(worker, dying)
      pool.with do |conn|
        conn.multi do
          conn.srem("workers", worker)
          conn.del("worker:#{worker}")
          conn.del("worker:#{worker}:started")
          conn.incrby("stat:processed", 1)
          conn.incrby("stat:processed:#{worker}", 1) unless dying
        end
      end
    end

    def job_taken?(hash, expiration)
      unique = false
      pool.with do |conn|
        conn.watch(hash)

        if conn.get(hash)
          conn.unwatch
        else
          unique = conn.multi do
            conn.setex(hash, expiration, 1)
          end
        end
      end
      return unique
    end

    def forget_job(hash)
      pool.with {|conn| conn.del(hash) }
    end

    attr_reader :pool
  end
end
