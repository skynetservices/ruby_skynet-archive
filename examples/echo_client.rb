# Allow examples to be run directly outside of the Gem
$LOAD_PATH.unshift File.dirname(__FILE__) + '/../lib'
require 'rubygems'
require 'ruby_skynet'

SemanticLogger.default_level = :info
SemanticLogger.add_appender(STDOUT)

class Echo < RubySkynet::Client
  self.skynet_name = "EchoService"
end

client = Echo.new
p client.echo(:hello => 'world')
