if ENV.has_key?("SIMPLECOV")
  require 'simplecov'
  SimpleCov.start
end

require 'minitest/unit'
require 'minitest/pride'
require 'minitest/autorun'

require 'sidekiq'
require 'sidekiq/util'
Sidekiq::Util.logger.level = Logger::ERROR

require 'sidekiq/redis_connection'
REDIS = {:url => "redis://localhost/15", :namespace => 'testy'}
DATA_STORE = { :mongo => {:host => "localhost", :port => 27017, :pool_size => 5, :namespace => "sidekiq_test" }, :redis => {:host => "localhost", :port => 27017, :size => 50, :namespace => "sidekiq_test" }}
