# Allow test to be run in-place without requiring a gem install
$LOAD_PATH.unshift File.dirname(__FILE__) + '/../lib'

require 'rubygems'
require 'test/unit'
require 'shoulda'
require 'ruby_skynet'

# Register an appender if one is not already registered
SemanticLogger.default_level = :trace
SemanticLogger.add_appender('test.log') if SemanticLogger.appenders.size == 0

# Unit Test
class ServiceRegistryTest < Test::Unit::TestCase
  context RubySkynet::ServiceRegistry do

    setup do
      @service_name = 'MyRegistryService'
      @version      = 5
      @region       = 'RegistryTest'
      @hostname     = '127.0.0.1'
      @port         = 2100
      @service_key  = "/services/#{@service_name}/#{@version}/#{@region}/#{@hostname}/#{@port}"
      RubySkynet.local_ip_address = @hostname
    end

    teardown do
      RubySkynet.local_ip_address = nil
    end

    context "with a registered service" do
      setup do
        RubySkynet.service_registry.register_service(@service_name, @version, @region, @hostname, @port)
        RubySkynet.service_registry.register_service(@service_name, @version, @region+'BLAH', @hostname, @port)
        # Allow time for registry callback that service was registered
        sleep 0.1
      end

      teardown do
        RubySkynet.service_registry.deregister_service(@service_name, @version, @region, @hostname, @port)
        RubySkynet.service_registry.deregister_service(@service_name, @version, @region+'BLAH', @hostname, @port)
        # Allow time for registry callback that service was deregistered
        sleep 0.1
        # No servers should be in the local registry
        #        assert_equal nil, RubySkynet.service_registry.servers_for(@service_name, @version, @region)
      end

      should "find server using exact match" do
        assert (servers = RubySkynet.service_registry.servers_for(@service_name, @version, @region)), RubySkynet.service_registry.to_h.inspect
        assert_equal 1, servers.size
        assert_equal "#{@hostname}:#{@port}", servers.first
      end

      should "find server using * version match" do
        assert (servers = RubySkynet.service_registry.servers_for(@service_name, '*', @region)), RubySkynet.service_registry.to_h.inspect
        assert_equal 1, servers.size
        assert_equal "#{@hostname}:#{@port}", servers.first
      end

      context "with multiple servers" do
        setup do
          @second_hostname     = '127.0.10.1'
          RubySkynet.service_registry.register_service(@service_name, @version, @region, @hostname, @port+1)
          RubySkynet.service_registry.register_service(@service_name, @version, @region, @hostname, @port+3)
          RubySkynet.service_registry.register_service(@service_name, @version-1, @region, @hostname, @port+2)
          RubySkynet.service_registry.register_service(@service_name, @version, @region, @second_hostname, @port)
        end

        teardown do
          RubySkynet.service_registry.deregister_service(@service_name, @version, @region, @hostname, @port+1)
          RubySkynet.service_registry.deregister_service(@service_name, @version, @region, @hostname, @port+3)
          RubySkynet.service_registry.deregister_service(@service_name, @version-1, @region, @hostname, @port+2)
          RubySkynet.service_registry.deregister_service(@service_name, @version, @region, @second_hostname, @port)
        end

        should "using * version match" do
          assert (servers = RubySkynet.service_registry.servers_for(@service_name, '*', @region)), RubySkynet.service_registry.to_h.inspect
          expected = RubySkynet::ServiceRegistry.respond_to?(:score_for_server) ? 3 : 4
          assert_equal expected, servers.size, servers
          assert_equal true, servers.include?("#{@hostname}:#{@port}"), servers
          assert_equal true, servers.include?("#{@hostname}:#{@port+1}"), servers
          assert_equal true, servers.include?("#{@hostname}:#{@port+3}"), servers
        end
      end

      should "return nil when service not found" do
        assert_raise RubySkynet::ServiceUnavailable do
          RubySkynet.service_registry.servers_for('MissingService', @version, @region)
        end
      end

      should "return nil when version not found" do
        assert_raise RubySkynet::ServiceUnavailable do
          RubySkynet.service_registry.servers_for(@service_name, @version+1, @region)
        end
      end

      should "return nil when region not found" do
        assert_raise RubySkynet::ServiceUnavailable do
          RubySkynet.service_registry.servers_for(@service_name, @version, 'OtherRegion')
        end
      end
    end

    context "scoring" do
      [
        ['192.168.11.0',  4 ],
        ['192.168.11.10', 3 ],
        ['192.168.10.0',  2 ],
        ['192.5.10.0',    1 ],
        ['10.0.11.0',     0 ],
      ].each do |test|
        should "handle score #{test[1]}" do
          skip "Not applicable to Static Service Registry" unless RubySkynet::ServiceRegistry.respond_to?(:score_for_server)
          local_ip_address = "192.168.11.0"
          assert_equal test[1], RubySkynet::ServiceRegistry.score_for_server(test[0], local_ip_address), "Local: #{local_ip_address} Server: #{test[0]} Score: #{test[1]}"
        end
      end
    end

  end
end