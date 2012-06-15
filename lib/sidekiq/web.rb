require 'sinatra/base'
require 'slim'
require 'sprockets'
require 'multi_json'

module Sidekiq
  class SprocketsMiddleware
    def initialize(app, options={})
      @app = app
      @root = options[:root]
      path   =  options[:path] || 'assets'
      @matcher = /^\/#{path}\/*/
      @environment = ::Sprockets::Environment.new(@root)
      @environment.append_path 'assets/javascripts'
      @environment.append_path 'assets/javascripts/vendor'
      @environment.append_path 'assets/stylesheets'
      @environment.append_path 'assets/stylesheets/vendor'
      @environment.append_path 'assets/images'
    end

    def call(env)
      # Solve the problem of people requesting /sidekiq when they need to request /sidekiq/ so
      # that relative links in templates resolve correctly.
      return [301, { 'Location' => "#{env['SCRIPT_NAME']}/", 'Content-Type' => 'text/html' }, ['redirecting']] if env['SCRIPT_NAME'] == env['REQUEST_PATH']

      return @app.call(env) unless @matcher =~ env["PATH_INFO"]
      env['PATH_INFO'].sub!(@matcher,'')
      @environment.call(env)
    end
  end

  class Web < Sinatra::Base
    dir = File.expand_path(File.dirname(__FILE__) + "/../../web")
    set :views,  "#{dir}/views"
    set :root, "#{dir}/public"
    set :slim, :pretty => true
    use SprocketsMiddleware, :root => dir

    helpers do

      def reset_worker_list
        Sidekiq.data_store.clear_all_workers
      end

      def workers
        @workers ||= Sidekiq.data_store.worker_jobs
      end

      def processed
        Sidekiq.data_store.processed_stats
      end

      def failed
        Sidekiq.data_store.failed_stats
      end

      def retry_count
        Sidekiq.data_store.pending_retry_count
      end

      def retries
        Sidekiq.data_store.pending_retries
      end

      def queues
        Sidekiq.data_store.sorted_queues
      end

      def retries_with_score(score)
        Sidekiq.data_store.retries_with_score(score)
      end

      def location
        Sidekiq.data_store.location
      end

      def version
        Sidekiq::VERSION
      end

      def data_store_name
        Sidekiq.data_store.name
      end

      def root_path
        "#{env['SCRIPT_NAME']}/"
      end

      def current_status
        return 'idle' if workers.size == 0
        return 'active'
      end

      def relative_time(time)
        %{<time datetime="#{time.getutc.iso8601}">#{time}</time>}
      end
    end

    get "/" do
      slim :index
    end

    post "/reset" do
      reset_worker_list
      redirect root_path
    end

    get "/queues/:name" do
      halt 404 unless params[:name]
      @name = params[:name]
      @messages = Sidekiq.data_store.get_first(10, @name)
      slim :queue
    end

    post "/queues/:name" do
      Sidekiq.data_store.delete_queue(params[:name])
      redirect root_path
    end

    get "/retries/:score" do
      halt 404 unless params[:score]
      @score = params[:score].to_f
      slim :retry
    end

    post "/retries/:unique_identifier" do
      halt 404 unless params[:score]
      if params['retry']
        Sidekiq.data_store.enqueue_scheduled_retries(unique_identifier)
      elsif params['delete']
        Sidekiq.data_store.delete_scheduled_retries(unique_identifier)
      end
      redirect root_path
    end
  end

end
