# Allow examples to be run directly outside of the Gem
$LOAD_PATH.unshift File.dirname(__FILE__) + '/../lib'
require 'rubygems'
require 'ruby_skynet'

SemanticLogger.default_level = :info
SemanticLogger.add_appender(STDOUT)

# Just echo back any parameters received when the echo method is called
class EchoService
  include RubySkynet::Service

  # Methods implemented by this service
  # Must take a Hash as input
  # Must Return a Hash response or nil for no response
  def echo(params)
    params['echo'] = true
    params
  end
end

# Start the server
RubySkynet::Server.start

puts "Press enter to shutdown server"
gets

