require 'redis'

class RDB
  def self.client
    @client ||= Redis.new
  end
end