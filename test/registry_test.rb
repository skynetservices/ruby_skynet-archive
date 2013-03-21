# Allow test to be run in-place without requiring a gem install
$LOAD_PATH.unshift File.dirname(__FILE__) + '/../lib'

require 'rubygems'
require 'test/unit'
require 'shoulda'
require 'ruby_skynet/registry'
require 'semantic_logger'
require 'ruby_skynet/doozer/client'

# Register an appender if one is not already registered
if SemanticLogger::Logger.appenders.size == 0
  SemanticLogger::Logger.default_level = :trace
  SemanticLogger::Logger.appenders << SemanticLogger::Appender::File.new('test.log')
end

# Unit Test
class RegistryTest < Test::Unit::TestCase
  context 'RubySkynet::Service' do

    setup do
      @service_name = 'MyRegistryService'
      @version      = 5
      @region       = 'RegistryTest'
      @hostname     = '127.0.0.1'
      @port         = 2100
      @service_key  = "/services/#{@service_name}/#{@version}/#{@region}/#{@hostname}/#{@port}"
    end

    context "without a registered service" do
      should "not be in doozer" do
        RubySkynet::Registry.send(:doozer_pool).with_connection do |doozer|
          assert_equal '', doozer[@service_key]
        end
      end
    end

    context "with a registered service" do
      setup do
        RubySkynet::Registry.register_service(@service_name, @version, @region, @hostname, @port)
        # Allow time for doozer callback that service was registered
        sleep 0.1
      end

      teardown do
        RubySkynet::Registry.deregister_service(@service_name, @version, @region, @hostname, @port)
        # Allow time for doozer callback that service was deregistered
        sleep 0.1
        # No servers should be in the local registry
        assert_equal nil, RubySkynet::Registry.servers_for(@service_name, @version, @region)
      end

      should "find server using exact match" do
        assert servers = RubySkynet::Registry.servers_for(@service_name, @version, @region)
        assert_equal 1, servers.size
        assert_equal "#{@hostname}:#{@port}", servers.first
      end

      should "find server using * version match" do
        assert servers = RubySkynet::Registry.servers_for(@service_name, '*', @region)
        assert_equal 1, servers.size
        assert_equal "#{@hostname}:#{@port}", servers.first
      end

      should "return nil when service not found" do
        assert_equal nil, RubySkynet::Registry.servers_for('MissingService', @version, @region)
      end

      should "return nil when version not found" do
        assert_equal nil, RubySkynet::Registry.servers_for(@service_name, @version+1, @region)
      end

      should "return nil when region not found" do
        assert_equal nil, RubySkynet::Registry.servers_for(@service_name, @version, 'OtherRegion')
      end

      should "be in doozer" do
        RubySkynet::Registry.send(:doozer_pool).with_connection do |doozer|
          assert_equal true, doozer[@service_key].length > 20
        end
      end
    end

  end
end