# Allow examples to be run directly outside of the Gem
$LOAD_PATH.unshift File.dirname(__FILE__) + '/../lib'
require 'rubygems'
require 'ruby_skynet'

# Log trace information to a log file
SemanticLogger::Logger.default_level = :trace
SemanticLogger::Logger.appenders << SemanticLogger::Appender::File.new('echo_server.log')

# Specify Port and Hostname to listen for requests on
RubySkynet::Server.port = 2020
RubySkynet::Server.hostname = '127.0.0.1'

# Just echo back any parameters received when the echo method is called
class EchoService
  include RubySkynet::Service

  # Methods implemented by this service
  # Must take a Hash as input
  # Must Return a Hash response or nil for no response
  def echo(params)
    params
  end
end

# Start the server
RubySkynet::Server.start

puts "Press enter to shutdown server"
gets

RubySkynet::Server.stop
