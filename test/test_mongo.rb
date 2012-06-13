require 'helper'
require 'sidekiq/processor'
require 'multi_json'
require 'sidekiq/manager'
require 'sidekiq/retry'
require 'sidekiq/middleware/server/retry_jobs'
require 'debugger'

class TestMongo < MiniTest::Unit::TestCase

  # processor
  describe 'with mock setup' do
    before do
      $invokes = 0
      $errors = []
      @boss = MiniTest::Mock.new
      Celluloid.logger = nil
      Sidekiq.data_store = DATA_STORE
      Sidekiq.data_store.flush
    end

    class MockWorker
      include Sidekiq::Worker
      def perform(args)
        raise "kerboom!" if args == 'boom'
        $invokes += 1
      end
    end

    it 'processes as expected' do
      msg = { 'class' => MockWorker.to_s, 'args' => ['myarg'] }
      processor = ::Sidekiq::Processor.new(@boss)
      @boss.expect(:processor_done!, nil, [processor])
      processor.process(msg, 'default')
      @boss.verify
      processor.terminate
      assert_equal 1, $invokes
      assert_equal 0, $errors.size
    end
  end

  # retries
  describe 'middleware' do
    before do
      #@redis = MiniTest::Mock.new
      @data_store = MiniTest::Mock.new
      # Ugh, this is terrible.
      Sidekiq.instance_variable_set(:@data_store, @data_store)

      #def @redis.with; yield self; end
    end

    it 'allows disabling retry' do
      msg = { 'class' => 'Bob', 'args' => [1,2,'foo'], 'retry' => false }
      msg2 = msg.dup
      handler = Sidekiq::Middleware::Server::RetryJobs.new
      assert_raises RuntimeError do
        handler.call('', msg2, 'default') do
          raise "kerblammo!"
        end
      end
      assert_equal msg, msg2
    end

    it 'saves backtraces' do
      @data_store.expect :retry, 1, [Hash, Time]
      msg = { 'class' => 'Bob', 'args' => [1,2,'foo'], 'retry' => true, 'backtrace' => true }
      handler = Sidekiq::Middleware::Server::RetryJobs.new
      c = nil
      assert_raises RuntimeError do
        handler.call('', msg, 'default') do
          c = caller(0); raise "kerblammo!"
        end
      end
      assert msg["error_backtrace"]
      assert_equal c, msg["error_backtrace"]
    end

    it 'saves partial backtraces' do
      @data_store.expect :retry, 1, [Hash, Time]
      msg = { 'class' => 'Bob', 'args' => [1,2,'foo'], 'retry' => true, 'backtrace' => 3 }
      handler = Sidekiq::Middleware::Server::RetryJobs.new
      c = nil
      assert_raises RuntimeError do
        handler.call('', msg, 'default') do
          c = caller(0)[0..3]; raise "kerblammo!"
        end
      end
      assert msg["error_backtrace"]
      assert_equal c, msg["error_backtrace"]
    end

    it 'handles a new failed message' do
      @data_store.expect :retry, 1, [Hash, Time]
      msg = { 'class' => 'Bob', 'args' => [1,2,'foo'], 'retry' => true }
      handler = Sidekiq::Middleware::Server::RetryJobs.new
      assert_raises RuntimeError do
        handler.call('', msg, 'default') do
          raise "kerblammo!"
        end
      end
      assert_equal 'default', msg["queue"]
      assert_equal 'kerblammo!', msg["error_message"]
      assert_equal 'RuntimeError', msg["error_class"]
      assert_equal 0, msg["retry_count"]
      refute msg["error_backtrace"]
      assert msg["failed_at"]
      @data_store.verify
    end

    it 'handles a recurring failed message' do
      @data_store.expect :retry, 1, [Hash, Time]
      now = Time.now.utc
      msg = {"class"=>"Bob", "args"=>[1, 2, "foo"], 'retry' => true, "queue"=>"default", "error_message"=>"kerblammo!", "error_class"=>"RuntimeError", "failed_at"=>now, "retry_count"=>10}
      handler = Sidekiq::Middleware::Server::RetryJobs.new
      assert_raises RuntimeError do
        handler.call('', msg, 'default') do
          raise "kerblammo!"
        end
      end
      assert_equal 'default', msg["queue"]
      assert_equal 'kerblammo!', msg["error_message"]
      assert_equal 'RuntimeError', msg["error_class"]
      assert_equal 11, msg["retry_count"]
      assert msg["failed_at"]
      @data_store.verify
    end

    it 'throws away old messages after too many retries' do
      now = Time.now.utc
      msg = {"class"=>"Bob", "args"=>[1, 2, "foo"], "queue"=>"default", "error_message"=>"kerblammo!", "error_class"=>"RuntimeError", "failed_at"=>now, "retry_count"=>25}
      handler = Sidekiq::Middleware::Server::RetryJobs.new
      assert_raises RuntimeError do
        handler.call('', msg, 'default') do
          raise "kerblammo!"
        end
      end
      @data_store.verify
    end
  end

  describe 'poller' do
    before do
      Sidekiq.data_store = DATA_STORE
      Sidekiq.data_store.flush
    end

    it 'should poll like a bad mother...SHUT YO MOUTH' do
      fake_msg = { 'class' => 'Bob', 'args' => [1,2], 'queue' => 'someq' }
      Sidekiq.data_store.retry(fake_msg, Time.at(Time.now.to_i - 200))

      inst = Sidekiq::Retry::Poller.new
      inst.poll
      assert Sidekiq.data_store.pop_message('someq')[1]['class']
    end
  end

  #manager
  describe 'with data store' do
    before do
      Sidekiq.data_store = DATA_STORE
      Sidekiq.data_store.flush
      $processed = 0
      $mutex = Mutex.new
    end

    class IntegrationWorker
      include Sidekiq::Worker
      sidekiq_options :queue => 'foo'

      def perform(a, b)
        $mutex.synchronize do
          $processed += 1
        end
        a + b
      end
    end

    it 'processes messages' do
      Sidekiq::Util.logger = Logger.new($stderr)
      IntegrationWorker.perform_async(1, 2)
      IntegrationWorker.perform_async(1, 3)

      q = TimedQueue.new
      mgr = Sidekiq::Manager.new(:queues => [:foo], :concurrency => 2)
      mgr.when_done do |_|
        q << 'done' if $processed == 2
      end
      mgr.start!
      result = q.timed_pop(1.0)
      assert_equal 'done', result
      mgr.stop(:shutdown => true, :timeout => 0)

      # Gross bloody hack because I can't get the actor threads
      # to shut down cleanly in the test.  Need @bascule's help here.
      (Thread.list - [Thread.current]).each do |t|
        t.raise Interrupt
      end
    end
  end

  ##stats
  #describe 'with data store' do
  #  before do
  #    #Sidekiq.redis = REDIS
  #    #Sidekiq.redis {|c| c.flushdb }
  #    Sidekiq.data_store = DATA_STORE
  #  end
  #
  #  class DumbWorker
  #    include Sidekiq::Worker
  #
  #    def perform(arg)
  #      raise 'bang' if arg == nil
  #    end
  #  end
  #
  #  it 'updates global stats in the success case' do
  #    msg = { 'class' => DumbWorker.to_s, 'args' => [""] }
  #    boss = MiniTest::Mock.new
  #
  #    Sidekiq.redis do |conn|
  #
  #      set = conn.smembers('workers')
  #      assert_equal 0, set.size
  #
  #      processor = Sidekiq::Processor.new(boss)
  #      boss.expect(:processor_done!, nil, [processor])
  #
  #      assert_equal 0, conn.get('stat:failed').to_i
  #      assert_equal 0, conn.get('stat:processed').to_i
  #      assert_equal 0, conn.get("stat:processed:#{processor}").to_i
  #
  #      processor.process(msg, 'xyzzy')
  #      processor.process(msg, 'xyzzy')
  #      processor.process(msg, 'xyzzy')
  #
  #      assert_equal 0, conn.get('stat:failed').to_i
  #      assert_equal 3, conn.get('stat:processed').to_i
  #      assert_equal 3, conn.get("stat:processed:#{processor}").to_i
  #    end
  #  end
  #
  #  it 'updates global stats in the error case' do
  #    msg = { 'class' => DumbWorker.to_s, 'args' => [nil] }
  #    boss = MiniTest::Mock.new
  #
  #    Sidekiq.redis do |conn|
  #      assert_equal [], conn.smembers('workers')
  #      assert_equal 0, conn.get('stat:failed').to_i
  #      assert_equal 0, conn.get('stat:processed').to_i
  #
  #      processor = Sidekiq::Processor.new(boss)
  #
  #      pstr = processor.to_s
  #      assert_raises RuntimeError do
  #        processor.process(msg, 'xyzzy')
  #      end
  #
  #      assert_equal 1, conn.get('stat:failed').to_i
  #      assert_equal 1, conn.get('stat:processed').to_i
  #      assert_equal nil, conn.get("stat:processed:#{pstr}")
  #    end
  #  end
  #
  #end
end
