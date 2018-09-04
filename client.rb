require 'json'
require_relative 'redis_client'

class Client
  def run
    loop do
      payload = dummy_payload
      puts "queueing #{dummy_payload}"
      RDB.client.xadd('watermill', "MAXLEN", "~", 1_000, "*", "data", JSON.generate(payload))
      sleep(0.5)
    end
  end

  private

  def dummy_payload
    { job_class: 'TestJob', args: [1, 2, 3] }
  end
end

if __FILE__ == $0
  Client.new.run
end
