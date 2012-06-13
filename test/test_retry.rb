require 'helper'
require 'multi_json'
require 'sidekiq/retry'
require 'sidekiq/middleware/server/retry_jobs'

class TestRetry < MiniTest::Unit::TestCase
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
      @data_store.expect :retry, 1, [String, String]
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
      @data_store.expect :retry, 1, [String, String]
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
      @data_store.expect :retry, 1, [String, String]
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
      @data_store.expect :retry, 1, [String, String]
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

  #describe 'poller' do
  #  before do
  #    @redis = MiniTest::Mock.new
  #    @pool = MiniTest::Mock.new
  #    def @pool.with; yield @redis; end
  #    Sidekiq.redis = REDIS
  #    Sidekiq.data_store.instance_variable_set(:@pool, @pool)
  #  end
  #
  #  it 'should poll like a bad mother...SHUT YO MOUTH' do
  #    fake_msg = Sidekiq.dump_json({ 'class' => 'Bob', 'args' => [1,2], 'queue' => 'someq' })
  #    Sidekiq.data_store.stub(:poll, [[fake_msg], 1]) do
  #      @redis.expect :rpush, 1, ['queue:someq', fake_msg]
  #      inst = Sidekiq::Retry::Poller.new
  #      inst.poll
  #      @redis.verify
  #    end
  #  end
  #end

end
