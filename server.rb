require 'json'
require_relative 'redis_client'

class TestJob
  def perform(jid, one, two, three)
    puts "TestJob <id : #{jid}> #{one}, #{two}, #{three}"
  end
end

class Server
  attr_accessor :name

  def initialize(name:)
    self.name = name
  end

  def run
    puts "Starting server: #{name}"

    # go through the backlog of unACK'ed jobs
    loop do
      old_jobs = RDB.client.xreadgroup('GROUP', 'default', name, 'COUNT', 50, 'BLOCK', 1, 'STREAMS', 'watermill', '0-0')
      _stream_id, jobs = old_jobs.first
      break if jobs.empty?
      process_jobs(jobs)
    end

    puts "reading new jobs: #{name}"

    loop do
      new_jobs = RDB.client.xreadgroup('GROUP', 'default', name, 'COUNT', 1, 'BLOCK', 2_000, 'STREAMS', 'watermill', '>')
      puts new_jobs.inspect
      if new_jobs
        _stream_id, jobs = new_jobs.first
        process_jobs(jobs)
      end
    end
  end

  def process_jobs(jobs)
    jobs.each do |job|
      jid, fields = job
      fields = Hash[*fields]
      data = JSON.parse(fields['data'])
      unless data['job_class']
        ack(jid)
        next
      end
      work = constantize(data['job_class']).new
      work.perform(jid, *data['args'])
      ack(jid)
    end
  end

  def ack(id)
    RDB.client.xack('watermill', 'default', id)
  end

  private

  def constantize(str)
    names = str.split('::')
    names.shift if names.empty? || names.first.empty?
    names.inject(Object) do |constant, name|
      constant.const_defined?(name, false) ? constant.const_get(name, false) : constant.const_missing(name)
    end
  end
end

if __FILE__ == $0
  name = ARGV[0] || abort('Usage: ruby server.rb CONSUMER_NAME')
  Server.new(name: name).run
end