# Allow test to be run in-place without requiring a gem install
$LOAD_PATH.unshift File.dirname(__FILE__) + '/../lib'

require 'rubygems'
require 'test/unit'
require 'shoulda'
require 'ruby_skynet'

# Register an appender if one is not already registered
SemanticLogger.default_level = :trace
SemanticLogger.add_appender('test.log') if SemanticLogger.appenders.size == 0

class TestService
  include RubySkynet::Service

  # Methods implemented by this service
  # Must take a Hash as input
  # Must Return a Hash response or nil for no response
  def echo(params)
    params
  end

  def exception_test(params)
    raise "Exception message"
  end
end

# Unit Test for ResilientSocket::TCPClient
class ServiceTest < Test::Unit::TestCase
  context 'RubySkynet::Service' do
    context "with server" do
      setup do
        @region = 'Test'
        @service_name = 'TestService'
        @version = 1
        RubySkynet.region = @region
        @server = RubySkynet::Server.new
        @server.register_service(TestService)
        sleep 0.2
      end

      teardown do
        @server.close if @server
        SemanticLogger::Logger.flush
      end

      should "be running" do
        assert_equal true, @server.running?
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
        # Server from receiving the exception
        should "handle service exceptions" do
          reply = @client.call(:exception_test, 'some' => 'parameters')
          assert_equal "Exception message", reply['exception']['message']
        end
      end

    end
  end
end