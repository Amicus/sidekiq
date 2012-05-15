
module Sidekiq
  module DataStore

    # Return the type of the data store backend.
    def name
      not_implemented __method__
    end

    # Add the job held in 'payload' to the 'queue' to be processed
    def push_job(queue, payload)
      not_implemented __method__
    end

    # queues the message held in 'msg' to the queue 'queue' to be
    # consumed later
    def enqueue(queue, message)
      not_implemented __method__
    end

    # Unregisters workers with names that match the given process id
    def clear_workers(process_id)
      not_implemented __method__
    end

    # Register in the data store that the job in 'job_data' has failed.
    def fail_job(job_data)
      not_implemented __method__
    end

    # Schedule a previously failed job for retry
    def retry(job_data, time)
      not_implemented __method__
    end

    # Return a list of names for all the queues that have been registered
    # for this data store.
    def registered_queues
      not_implemented __method__
    end

    # Return a list of names for all the workers that have been registered
    # for this data store.
    def registered_workers
      not_implemented __method__
    end

    def store
      not_implemented __method__
    end

    def update
      not_implemented __method__
    end

    def remove
      not_implemented __method__
    end

    def retry
      not_implemented __method__
    end

    def clear
      not_implemented __method__
    end

    def not_implemented(method_name)
      raise NotImplementedError method_name + "Not implemented!"
    end
  end
end