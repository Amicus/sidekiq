require 'rubygems'
require 'mongo'

module Sidekiq
  class MongoConection
    include Sidekiq::DataStore

    def self.create(options={})
      MongoConnection.new(options)
    end

    def self.initialize(options={})
      url = options[:url] || ENV['MONGO_URL'] || 'mongodb://localhost'
      replica_set = options[:replica_set]
      port = options[:port]
      # need a connection for Fetcher and Retry
      size = options[:size] || (Sidekiq.server? ? (Sidekiq.options[:concurrency] + 2) : 5)
      namespace = options[:namespace]
      if replica_set
        options[:read] = :secondary
        @database = Mongo::ReplSetConnection.new(replica_set, port, options).db(namespace)
      elsif url
        @database = Mongo::Connection.new(url, port, options).db(namespace)
      else
        raise ArgumentError("Missing required arguments.")
      end
    end

    def enqueue(queue, payload)
      @database['queues'].find_and_modify({:query => {:queue => queue},
                                           :update => {"$push" => {:messages => payload}},
                                           :upsert => true})
    end

    def clear_workers(process_id)
      @database['workers'].remove({:name => /:#{process_id}-/})
    end

    def clear_all_workers
      @database['workers'].remove({})
    end

    def fail_job(job_data)
      @database['failed'].find_and_modify({:query => {:type => "jobs"},
                                           :update => {"$push" => {:list => Sidekiq.dump_json(job_data),
                                           :upsert => true}}})
    end

    def retry(job_data, time)
      @database['retries'].insert({:time => time, :job => job_data})
    end

    def retries_with_score(score)
      results = @database['retries'].find({:time => score},
                                          {:fields => {:_id => 0, :time => 0, :job => 1}})
      jobs = []
      while results.has_next? do
        next_result = results.next
        jobs << Sidekiq.load_json(next_result[:job])
      end
      return jobs
    end

    def registered_queues
      queue_docs = @database['queues'].find({}, {:fields => {:_id => 0, :queue => 1, :messages => 0}})
      queues = []
      while queue_docs.has_next? do
        doc = queue_docs.next
        queues << doc[:queue]
      end
      return queues
    end

    def sorted_queues
      queue_docs = @database['queues'].find({}, {:fields => {:_id => 0, :queue => 1, :messages => 1}})
      queues = []
      while queue_docs.has_next? do
        doc = queue_docs.next
        queues << [doc[:queue], doc[:messages].length]
      end
      queues.sort { |x,y| x[1] <=> y[1] }
    end

    def registered_workers
      worker_docs = @database['workers'].find({}, {:fields => {:_id => 0, :name => 1, :job => 0}})
      workers = []
      while worker_docs.has_next? do
        worker = worker_docs.next
        workers << worker[:name]
      end
      return workers
    end

    def worker_jobs
      worker_docs = @database['workers'].find({}, {:fields => {:_id => 0, :name => 1, :job => 1}})
      workers = []
      while worker_docs.has_next? do
        doc = worker_docs.next
        msg = doc[:job]
        workers << msg ? [doc[:name], Sidekiq.load_json(msg)] : nil
      end
      workers.compact.sort{ |x| x[1] ? -1 : 1 }
    end

    def processed_stats
      @database['stats'].find({:type => 'processed'})
    end

    def failed_stats
      @database['stats'].find({:type => 'failed'})
    end

    def pending_retry_count
      @database['retries'].find({}).count()
    end

    def pending_retries
      retry_docs = @database['retries'].find({}, {}).sort(:time, :asc)
      retries = []
      while retry_docs.has_next? and retries.size <= 25 do
        doc = retry_docs.next
        retries << [Sidekiq.load_json(doc[:job]), Float(doc[:time])]
      end
      return retries
    end

    #TODO: find location method for mongo client
    def location
      "localhost"
    end

    def get_first(n, name)
      queue = @database['queues'].find({:queue => name})
      queue[:messages].slice(0, n).map {|str| Sidekiq.load_json(str)}
    end

    def delete_queue(name)
      @database['queues'].remove({:queue => name})
    end

    # TODO: Atomicity
    def enqueue_scheduled_retries(time)
      results = @database['retries'].find({:time => time})
      while results.has_next? do
        result = results.next
        job = result[:job]
        msg = Sidekiq.load_json(job)
        queue = msg['queue']
        enqueue(queue, msg)
      end
      delete_scheduled_retries(time)
    end

    def pop_message(*queues)
      pool.with { |conn| conn.blpop(*queues) }
    end

    #TODO: Atomicity
    def poll
      now = Time.now.to_f.to_s
      retries = @database['retries'].find({:time => {"$lte" => now}})
      while retries.has_next? do
        to_retry = retries.next
        job = to_retry[:job]
        logger.debug { "Retrying #{job}"}
        msg = Sidekiq.load_json(job)
        queue = msg['queue']
        enqueue(queue, job)
      end
    end

    def delete_scheduled_retries(time)
      @database['retries'].remove({:time => time})
    end

    #TODO: mongo analogue of setex method:
    #TODO: conn.setex("worker:#{worker}:started", DEFAULT_EXPIRY, Time.now.to_s)
    #TODO: conn.setex("worker:#{worker}", DEFAULT_EXPIRY, Sidekiq.dump_json(hash))
    def process_job(worker, message, queue)
      job_hash = Sidekiq.dump_json({:queue => queue, :payload => message, :run_at => Time.now.strftime("%Y/%m/%d %H:%M:%S %Z")})
      @database['workers'].find_and_modify({:query => {:name => worker},
                                            :update => {:job => job_hash},
                                            :upsert => true})
    end

    def fail_worker(worker)
      @database['stats'].find_and_modify({:query => {:type => 'failed'},
                                          :update => {"$inc" => {:count => 1}},
                                          :upsert => true})
      @database['stats'].remove({:type => 'processed', :worker => worker})
    end

    def clear_worker(worker, dying)
      @database['workers'].remove({:name => worker})
      @database['stats'].find_and_modify({:query => {:type => 'processed', :worker => worker},
                                          :update => {"$inc" => {:count => 1}}}) unless dying
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