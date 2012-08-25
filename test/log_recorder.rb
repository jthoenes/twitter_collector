require 'bundler'
Bundler.require
require 'json'

class LogRecorder

  DIR = File.join(File.dirname(__FILE__), "..", "tweets")

  ROUTING_KEYS = %w(twitter.search_results twitter.stream_results)

  def initialize()
    @logger = Logging.logger['twitter_recorder']
    @logger.appenders = Logging.appenders.stdout
    @logger.level = :debug
  end

  def run
    queue.subscribe do |msg|
      begin
        message = JSON.parse(msg[:payload], :symbolize_names => true)
        filename = File.join(DIR, "#{message[:payload][:tweet_id]}.json")
        if File.exists?(filename)
          @logger.info("Received tweet again: #{message[:payload][:tweet_id]}")
        else
          @logger.info("Writing tweet to file #{filename}")
          File.open(filename, "w") { |file| file.write(msg[:payload]) }
        end
      rescue Exception => e
        @logger.error("#{e} \n" + e.backtrace.join("\n"))
      end
    end
  end

  def queue
    @queue ||= create_queue
  end

  def create_queue
    client = Bunny.new
    client.start
    queue = client.queue('twitter')
    exchange = client.exchange('datainsight', :type => :topic)

    ROUTING_KEYS.each do |key|
      queue.bind(exchange, :key => key)
      @logger.info("Bound to #{key}, listening for events")
    end

    queue
  end
end

LogRecorder.new.run