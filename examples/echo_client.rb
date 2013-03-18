require 'rubygems'
require 'ruby_skynet'
require 'sync_attr'

SemanticLogger::Logger.default_level = :trace
SemanticLogger::Logger.appenders << SemanticLogger::Appender::File.new('skynet.log')

client = RubySkynet::Client.new('EchoService')
p client.call('echo', :hello => 'world')
