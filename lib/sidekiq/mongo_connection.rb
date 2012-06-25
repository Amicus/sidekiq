require 'rubygems'
require 'mongo'
require 'set'
require 'yaml'

module Sidekiq
  class MongoConnection
    include Sidekiq::DataStore

    def self.create(options={})
      MongoConnection.new(options)
    end

    def initialize(options={})
      options = options.dup
      host = options.delete(:host)
      port = options.delete(:port)
      replica_set = options.delete(:replica_set)
      # need a connection for Fetcher and Retry
      size = options[:size] || (Sidekiq.server? ? (Sidekiq.options[:concurrency] + 2) : 5)

      namespace_prefix = "sidekiq"
      namespace_suffix = options.delete(:namespace)
      namespace = "#{namespace_prefix}_#{namespace_suffix}"

      if replica_set
        options[:read] = :secondary
        @database = Mongo::ReplSetConnection.new(replica_set, options).db(namespace)
      elsif host
        @database = Mongo::Connection.new(host, port, options).db(namespace)
      else
        raise ArgumentError, "Missing required arguments for initializing MongoDB data store."
      end
    end

    def name
      "MongoDB"
    end

    def enqueue(queue, payload)
      @database['queues'].insert({'name' => queue,
                                  'message' => payload,
                                  'inserted' => Time.now.strftime("%Y/%m/%d %H:%M:%S %Z"),
                                  'owned' => 'false'
                                 })
    end

    #TODO: clean up the api to get rid of this method
    def push_job(queue, payload)
      enqueue(queue, payload)
    end

    def clear_workers(process_id)
      @database['workers'].remove({:name => /:#{process_id}-/})
    end

    def clear_all_workers
      @database['workers'].remove({})
    end

    def clear_worker(worker, dying)
      @database['workers'].remove({'name' => worker.to_s})

      @database['stats'].find_and_modify({:query => {'type' => 'processed', 'worker' => 'all'},
                                          :update => {"$inc" => {'count' => 1}},
                                          :upsert => true})

      @database['stats'].find_and_modify({:query => {'type' => 'processed', 'worker' => worker.to_s},
                                          :update => {"$inc" => {'count' => 1}},
                                          :upsert => true}) unless dying
    end

    def fail_job(job_data)
      @database['failed'].find_and_modify({:query => {'type' => "jobs"},
                                           :update => {"$push" => {'list'=> Sidekiq.dump_json(job_data),
                                           :upsert => true}}})
    end

    def retry(job_data, time)
      @database['retries'].insert({:time => time, :job => job_data})
    end

    def registered_queues
      queue_docs = @database['queues'].find({}, {:fields => {'_id' => 0,
                                                             'name' => 1,
                                                             'message' => 0,
                                                             'inserted' => 0,
                                                             'owned' => 0}
                                                }
                                           )
      queues = Set.new
      while queue_docs.has_next? do
        doc = queue_docs.next
        queues.add(doc['name'])
      end
      return queues.to_a
    end

    def sorted_queues
      queue_docs = @database['queues'].find({})
      queues = {}
      while queue_docs.has_next? do
        doc = queue_docs.next
        count = 0
        if queues.has_key?(doc['name'])
          if doc['owned'].eql?('false')
            count = queues[doc['name']] + 1
          end
        end
        queues[doc['name']] = count
      end

      queue_array = []
      queues.each_pair do |k,v|
        queue_array << [k, v]
      end

      queue_array.sort { |x,y| x[1] <=> y[1] }
    end

    def registered_workers
      worker_docs = @database['workers'].find({}, {:fields => {'_id' => 0, 'name' => 1, 'job' => 0}})
      workers = []
      while worker_docs.has_next? do
        worker = worker_docs.next
        workers << worker['name']
      end
      return workers
    end

    def worker_jobs
      worker_docs = @database['workers'].find({}, {:fields => {'_id' => 0, 'name' => 1, 'job' => 1}})
      workers = []
      while worker_docs.has_next? do
        doc = worker_docs.next
        msg = doc['job']
        worker = msg ? [doc['name'], Sidekiq.load_json(msg)] : nil
        workers << worker
      end
      workers.compact.sort{ |x| x[1] ? -1 : 1 }
    end

    def processed_stats
      get_stat('processed', 'all')
    end

    def failed_stats
      get_stat('failed', 'all')
    end

    def get_stat(type, worker)
      stat = @database['stats'].find_one({'type' => type, 'worker' => worker})
      stat ? stat['count'] : 0
    end

    #TODO: find location method for mongo client
    def location
      "localhost"
    end

    def get_first(n, name)
      queue_docs = @database['queues'].find({'name' => name},
                                            {:fields => {'_id' => 1,
                                                         'name' => 1,
                                                         'message' => 1,
                                                         'inserted' => 1,
                                                         'owned' => 1}
                                            }
                                      ).sort('_id', :asc).limit(n)
      results = []
      while queue_docs.has_next?
        next_doc = queue_docs.next
        results << Sidekiq.load_json(next_doc['message'])
      end
      return results
    end

    def delete_queue(name)
      @database['queues'].remove({'name' => name})
    end

    # TODO: DRY
    def enqueue_scheduled_retries(time)
      results = @database['retries'].find({'time' => time})
      while results.has_next? do
        result = results.next
        job = result['job']
        msg = Sidekiq.load_json(job)
        queue = msg['queue']
        enqueue(queue, msg)
      end
      @database['retries'].remove({'time' => time})
    end

    def pop_message(*queues)
      queue_strings = queues.map {|q| q.to_s}
      queue = @database['queues'].find_and_modify({:query => {'name' => {"$in" => queue_strings},
                                                              'owned' => 'false'},
                                                   :update => {"$set" => {'owned' => 'true'}},
                                                   :fields => {'name' => 1, 'message' => 1},
                                                   :new => false})
      if queue
        [queue['name'], queue['message']]
      else
        sleep 1
      end

    end

    #TODO: Atomicity
    def poll
      now = Time.now.to_f
      retries = @database['retries'].find({'time' => {"$lte" => now}})
      while retries.has_next? do
        to_retry = retries.next
        job = to_retry['job']
        logger.debug { "Retrying #{job}"}
        msg = Sidekiq.load_json(job)
        queue = msg['queue']
        enqueue(queue, job)
      end
      delete_scheduled_retries(now)
    end

    def delete_scheduled_retries(time)
      @database['retries'].remove({'time' => {"$lte" => time}})
    end

    def retries_with_score(score)
      results = @database['retries'].find({:time => {"$lte" => score}},
                                          {:fields => {'job' => 1}})
      jobs = []
      while results.has_next? do
        next_result = results.next
        jobs << Sidekiq.load_json(next_result['job'])
      end
      return jobs
    end

    def pending_retry_count
      @database['retries'].find({}).count()
    end

    def pending_retries
      retry_docs = @database['retries'].find({}, {}).sort('time', :asc)
      retries = []
      while retry_docs.has_next? and retries.size <= 25 do
        doc = retry_docs.next
        retries << [Sidekiq.load_json(doc['job']), Float(doc['time'])]
      end
      return retries
    end

    #TODO: mongo analogue of setex method:
    #TODO: conn.setex("worker:#{worker}:started", DEFAULT_EXPIRY, Time.now.to_s)
    #TODO: conn.setex("worker:#{worker}", DEFAULT_EXPIRY, Sidekiq.dump_json(hash))
    def process_job(worker, message, queue)
      job_hash = Sidekiq.dump_json({'queue' => queue, 'payload' => message, 'run_at' => Time.now.strftime("%Y/%m/%d %H:%M:%S %Z")})
      @database['workers'].find_and_modify({:query => {'name' => worker.to_s},
                                            :update => {"$set" => {'job' => job_hash}},
                                            :upsert => true})
    end

    def fail_worker(worker)
      @database['stats'].find_and_modify({:query => {'type' => 'failed', 'worker' => worker.to_s},
                                          :update => {"$inc" => {:count => 1}},
                                          :upsert => true})
      @database['stats'].find_and_modify({:query => {'type' => 'failed', 'worker' => 'all'},
                                          :update => {"$inc" => {:count => 1}},
                                          :upsert => true})
      @database['stats'].remove({'type' => 'processed', :worker => worker.to_s})
    end

    def flush
      collections = @database.collection_names
      collections.each do |collection|
        @database.drop_collection(collection) unless collection =~ /^system\./
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

  end
end
