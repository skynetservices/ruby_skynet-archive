# Allow examples to be run directly outside of the Gem
$LOAD_PATH.unshift File.dirname(__FILE__) + '/../lib'
require 'rubygems'
require 'ruby_skynet'

SemanticLogger::Logger.default_level = :trace
SemanticLogger::Logger.appenders << SemanticLogger::Appender::File.new('echo_client.log')

client = RubySkynet::Client.new('EchoService')
p client.call('echo', :hello => 'world')
