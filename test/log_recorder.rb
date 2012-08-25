require 'bundler'
Bundler.require

class LogRecorder

  ROUTING_KEYS = %w(twitter.search_results twitter.stream_results)

  def initialize()
    @logger = Logging.logger['twitter_recorder']
    @logger.appenders = Logging.appenders.stdout
    @logger.level = :debug
  end

  def run
    queue.subscribe do |msg|
      begin
        @logger.info("Received a message:\n#{msg}")
      rescue Exception => e
        @logger.error("#{e} \n" + e.backtrace.join("\n"))
      end
    end
  end

  def queue
    @queue ||= create_queue
  end

  def create_queue
    client = Bunny.new ENV['AMQP']
    client.start
    queue = client.queue(ENV['QUEUE'] || 'weekly_reach')
    exchange = client.exchange('datainsight', :type => :topic)

    ROUTING_KEYS.each do |key|
      queue.bind(exchange, :key => key)
      @logger.info("Bound to #{key}, listening for events")
    end

    queue
  end
end

LogRecorder.new.run