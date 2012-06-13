require 'sidekiq/util'

module Sidekiq
  module DataStore
    include Sidekiq::Util

    class InvalidDataStore < StandardError; end

    def self.create(options={})
      if options[:mongo]
        MongoConnection.create(options[:mongo])
      elsif options[:redis]
        RedisConnection.create(options[:redis])
      else
        raise InvalidDataStore, "You must specify a mongo or redis data store"
      end
    end

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

    # Clear the data store
    def flush
      not_implemented
    end

    # Poll for retries
    def poll
      not_implemented
    end

#    def not_implemented(method_name)
#      raise NotImplementedError method_name + "Not implemented!"
#    end
  end
end