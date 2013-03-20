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

# Used when registering the service below
RubySkynet::Server.port = 2100
RubySkynet::Server.region = 'RegistryTest'
RubySkynet::Server.hostname = '127.0.0.1'

module Registry
  class MyTestService
    include RubySkynet::Service

    # Service name to use when registering with Skynet
    self.service_name = "MyRegistryService"
    self.service_version = 5

    def echo(params)
      params
    end
  end
end

# Unit Test
class RegistryTest < Test::Unit::TestCase
  context 'RubySkynet::Service' do
    
    setup do
      @service_name = 'MyRegistryService'
      @version      = 5
      @region       = RubySkynet::Server.region
      @hostname     = RubySkynet::Server.hostname
      @port         = RubySkynet::Server.port
      # Start monitoring doozer for changes
      RubySkynet::Registry.service_registry
    end

    teardown do
      # Cannot use connection pool once it has been closed
      #RubySkynet::Registry.stop
    end

    context "without a registered service" do
      should "not be in doozer" do
        RubySkynet::Registry.send(:doozer_pool).with_connection do |doozer|
          assert_equal '', doozer[Registry::MyTestService.service_key]
        end
      end
    end

    context "with a registered service" do
      setup do
        RubySkynet::Registry.register_service(Registry::MyTestService, @region, @hostname, @port)
      end

      teardown do
        RubySkynet::Registry.deregister_service(Registry::MyTestService)
      end

      should "have correct service key" do
        assert_equal "/services/#{@service_name}/#{@version}/#{@region}/#{@hostname}/#{@port}", Registry::MyTestService.service_key
      end

      should "be in doozer" do
        RubySkynet::Registry.send(:doozer_pool).with_connection do |doozer|
          assert_equal true, doozer[Registry::MyTestService.service_key].length > 20
        end
      end
    end

  end
end