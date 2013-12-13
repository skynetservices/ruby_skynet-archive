# Allow test to be run in-place without requiring a gem install
$LOAD_PATH.unshift File.dirname(__FILE__) + '/../lib'
$LOAD_PATH.unshift File.dirname(__FILE__)

require 'rubygems'
require 'test/unit'
require 'shoulda'
require 'ruby_skynet'

# Register an appender if one is not already registered
SemanticLogger.default_level = :trace
SemanticLogger.add_appender('test.log', &SemanticLogger::Appender::Base.colorized_formatter) if SemanticLogger.appenders.size == 0

class ClientTestService
  include RubySkynet::Service

  # Methods implemented by this service
  # Must take a Hash as input
  # Must Return a Hash response or nil for no response
  def test1(params)
    { 'result' => 'test1' }
  end

  def sleep(params)
    Kernel.sleep params['duration'] || 1
    { 'result' => 'sleep' }
  end

  def fail(params)
    if params['attempt'].to_i >= 2
      { 'result' => 'fail' }
    else
      nil
    end
  end

end

# Test Client Class
class ClientTestServiceClient < RubySkynet::Client
end

# Unit Test for ResilientSocket::TCPClient
class ClientTest < Test::Unit::TestCase
  context RubySkynet::Client do

    context "without server" do
      should "raise exception when not registered" do
        exception = assert_raise RubySkynet::ServiceUnavailable do
          client = RubySkynet::Client.new('SomeService','*','ClientTest')
          client.call(:test, :hello => 'there')
        end
        assert_match /No servers available for service: SomeService with version: -1 in region: ClientTest/, exception.message
      end

    end

    context "with server" do
      setup do
        @region = 'ClientTest'
        RubySkynet.region = @region
        @server = RubySkynet::Server.new
        @server.register_service(ClientTestService)
        # Give Service Registry time to push out the presence of the service above
        sleep 0.1

        @service_name = 'ClientTestService'
        @version = 1

        @read_timeout = 3.0
      end

      teardown do
        @server.close if @server
      end

      context "with client connection" do
        setup do
          @client = RubySkynet::Client.new(@service_name, @version, @region)
        end

        should "successfully send and receive data" do
          reply = @client.call(:test1, 'some' => 'parameters')
          assert_equal 'test1', reply['result']
        end

        should "timeout on receive" do
          request = { 'duration' => @read_timeout + 0.5}

          exception = assert_raise ResilientSocket::ReadTimeout do
            # Read 4 bytes from server
            @client.call('sleep', request, :read_timeout => @read_timeout)
          end
          assert_match /Timedout after #{@read_timeout} seconds trying to read/, exception.message
        end
      end

      context "using client class" do
        setup do
          @client = ClientTestServiceClient.new("ClientTestService")
        end

        should "successfully send and receive data" do
          assert reply = @client.test1('some' => 'parameters'), "Must return a Hash"
          assert_equal 'test1', reply['result']
        end

        should "timeout on receive" do
          request = { 'duration' => @read_timeout + 0.5}

          exception = assert_raise ResilientSocket::ReadTimeout do
            # Read 4 bytes from server
            @client.sleep(request, :read_timeout => @read_timeout)
          end
          assert_match /Timedout after #{@read_timeout} seconds trying to read/, exception.message
        end
      end

    end
  end
end