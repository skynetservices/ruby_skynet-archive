# Allow test to be run in-place without requiring a gem install
$LOAD_PATH.unshift File.dirname(__FILE__) + '/../lib'

require 'rubygems'
require 'test/unit'
require 'shoulda'
require 'ruby_skynet'

# Register an appender if one is not already registered
if SemanticLogger::Logger.appenders.size == 0
  SemanticLogger::Logger.default_level = :trace
  SemanticLogger::Logger.appenders << SemanticLogger::Appender::File.new('test.log')
end

RubySkynet::Server.port = 2100
RubySkynet::Server.region = 'Test'
RubySkynet::Server.hostname = '127.0.0.1'

class TestService
  include RubySkynet::Service

  # Methods implemented by this service
  # Must take a Hash as input
  # Must Return a Hash response or nil for no response
  def echo(params)
    params
  end

  def exception(params)
    raise Exception.new("Exception message")
  end
end

# Unit Test for ResilientSocket::TCPClient
class RubySkynetServiceTest < Test::Unit::TestCase
  context 'RubySkynet::Service' do
    context "with server" do
      setup do
        RubySkynet::Server.start
        @service_name = 'TestService'
        @version = 1
        @region = RubySkynet::Server.region
      end

      teardown do
        RubySkynet::Server.stop
      end

      should "be running" do
        assert_equal true, RubySkynet::Server.running?
      end

      context "using a client" do
        setup do
          @client = RubySkynet::Client.new(@service_name, @version, @region)
        end

        should "successfully send and receive data" do
          reply = @client.call(:echo, 'some' => 'parameters')
          assert_equal 'some', reply.keys.first
          assert_equal 'parameters', reply.values.first
        end

        # Cellulloid 0.13.0.pre2 is doing something weird here and preventing the
        # Server from catching the exception
        #   should "handle service exceptions" do
        #     reply = @client.call(:exception, 'some' => 'parameters')
        #   end
      end

    end
  end
end