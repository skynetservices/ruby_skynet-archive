# Allow test to be run in-place without requiring a gem install
$LOAD_PATH.unshift File.dirname(__FILE__) + '/../lib'
$LOAD_PATH.unshift File.dirname(__FILE__)

require 'rubygems'
require 'test/unit'
require 'shoulda'
require 'ruby_skynet'
require 'simple_server'
require 'multi_json'

SemanticLogger::Logger.default_level = :trace
SemanticLogger::Logger.appenders << SemanticLogger::Appender::File.new('test.log')

# Unit Test for ResilientSocket::TCPClient
class RubySkynetClientTest < Test::Unit::TestCase
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
        @port = 2000
        @read_timeout = 3.0
        @server = SimpleServer.new(@port)
        @server_name = "localhost:#{@port}"

        # Register service in doozer
        @service_name = "TestService"
        @version = 1
        @region = 'Test'
        @ip_address = "127.0.0.1"
        config = {
          "Config" => {
            "UUID" => "3978b371-15e9-40f8-9b7b-59ae88d8c7ec",
            "Name" => @service_name,
            "Version" => @version.to_s,
            "Region" => @region,
            "ServiceAddr" => {
              "IPAddress" => @ip_address,
              "Port" => @port,
              "MaxPort" => @port + 999
            },
          },
          "Registered" => true
        }
        RubySkynet::Registry.doozer_pool.with_connection do |doozer|
          doozer["/services/#{@service_name}/#{@version}/#{@region}/#{@ip_address}/#{@port}"] = MultiJson.encode(config)
        end
      end

      teardown do
        @server.stop if @server
        # De-register server in doozer
        RubySkynet::Registry.doozer_pool.with_connection do |doozer|
          doozer.delete("/services/#{@service_name}/#{@version}/#{@region}/#{@ip_address}/#{@port}") rescue nil
        end
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