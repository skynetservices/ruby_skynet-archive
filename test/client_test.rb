# Allow test to be run in-place without requiring a gem install
$LOAD_PATH.unshift File.dirname(__FILE__) + '/../lib'
$LOAD_PATH.unshift File.dirname(__FILE__)

require 'rubygems'
require 'test/unit'
require 'shoulda'
require 'ruby_skynet'

# Register an appender if one is not already registered
if SemanticLogger::Logger.appenders.size == 0
  SemanticLogger::Logger.default_level = :trace
  SemanticLogger::Logger.appenders << SemanticLogger::Appender::File.new('test.log')
end

class ClientTestService
  include RubySkynet::Service

  # Methods implemented by this service
  # Must take a Hash as input
  # Must Return a Hash response or nil for no response
  def test1(params)
    { 'result' => 'test1' }
  end

  def sleep(params)
    sleep params['duration'] || 1
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

# Unit Test for ResilientSocket::TCPClient
class ClientTest < Test::Unit::TestCase
  context RubySkynet::Client do

    context "without server" do
      should "raise exception when cannot reach server after 5 retries" do
        exception = assert_raise RubySkynet::ServiceUnavailable do
          client = RubySkynet::Client.new('SomeService')
          client.call(:test, :hello => 'there')
        end
        assert_match /No servers available for service: SomeService with version: \* in region: Development/, exception.message
      end

    end

    context "with server" do
      setup do
        @port = 2100
        @region = 'ClientTest'
        @hostname = '127.0.0.1'
        RubySkynet::Server.start(@hostname, @port, @region)

        @service_name = 'ClientTestService'
        @version = 1

        @read_timeout = 3.0
      end

      teardown do
        RubySkynet::Server.stop
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
    end
  end
end