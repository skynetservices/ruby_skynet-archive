require 'sync_attr'

module RubySkynet
  include SyncAttr

  # Returns the default region for all Ruby Skynet Clients and Services
  def self.region
    @@region ||= 'Development'
  end

  # Sets the default region to use for Skynet Clients and Services
  def self.region=(region)
    @@region = region
  end

  # Returns the service_path where services are located
  def self.services_path
    @@services_path ||= 'app/services'
  end

  # Sets the service_path where services are located
  def self.services_path=(services_path)
    @@services_path = services_path
  end

  # Returns the starting port for the server to listen on
  # If this port is in use the next available port will be used
  # upto 999 above the server_port value
  def self.server_port
    @@server_port ||= 2000
  end

  def self.server_port=(server_port)
    @@server_port = server_port
  end

  # The ip address at which this server instance can be reached
  # by remote Skynet clients
  # Note: Must be an IP address, not the hostname
  def self.local_ip_address
    @@local_ip_address ||= Common::local_ip_address
  end

  def self.local_ip_address=(local_ip_address)
    @@local_ip_address = local_ip_address
  end

  # Returns the services registry consisting of service names
  # and the hosts on which they are running
  sync_cattr_reader :services do
    ServiceRegistry.new(
      :root_path => "/services",
      :doozer    => doozer_config
    )
  end

  # Default doozer configuration
  # To replace this default, set the config as follows:
  #    RubySkynet::Client.doozer_config = { .... }
  #
  #   :servers [Array of String]
  #     Array of URL's of doozer servers to connect to with port numbers
  #     ['server1:2000', 'server2:2000']
  #
  #     The second server will only be attempted once the first server
  #     cannot be connected to or has timed out on connect
  #     A read failure or timeout will not result in switching to the second
  #     server, only a connection failure or during an automatic reconnect
  #
  #   :read_timeout [Float]
  #     Time in seconds to timeout on read
  #     Can be overridden by supplying a timeout in the read call
  #
  #   :connect_timeout [Float]
  #     Time in seconds to timeout when trying to connect to the server
  #
  #   :connect_retry_count [Fixnum]
  #     Number of times to retry connecting when a connection fails
  #
  #   :connect_retry_interval [Float]
  #     Number of seconds between connection retry attempts after the first failed attempt
  sync_cattr_accessor :doozer_config do
    {
      :servers                => ['127.0.0.1:8046'],
      :read_timeout           => 5,
      :connect_timeout        => 3,
      :connect_retry_interval => 1,
      :connect_retry_count    => 30
    }
  end

  # Load the Encryption Configuration from a YAML file
  #  filename:
  #    Name of file to read.
  #        Mandatory for non-Rails apps
  #        Default: Rails.root/config/ruby_skynet.yml
  #  environment:
  #    Which environment config to load. Usually: production, development, etc.
  #    Default: Rails.env
  def self.configure!(filename=nil, environment=nil)
    config_file = filename.nil? ? Rails.root.join('config', 'ruby_skynet.yml') : Pathname.new(filename)
    raise "ruby_skynet config not found. Create a config file at: config/ruby_skynet.yml" unless config_file.file?

    cfg = YAML.load(ERB.new(File.new(config_file).read).result)[environment || Rails.env]
    raise("Environment #{Rails.env} not defined in config/ruby_skynet.yml") unless cfg

    RubySkynet.region = cfg.delete(:region) if [:region]
    RubySkynet.services_path = cfg.delete(:services_path) if [:services_path]
    RubySkynet.server_port = cfg.delete(:server_port) if [:server_port]
    RubySkynet.local_ip_address = cfg.delete(:local_ip_address) if [:local_ip_address]
    RubySkynet.doozer_config = cfg.delete(:doozer) if [:doozer]

    cfg.each_pair {|k,v| RubySkynet::Server.logger.warn "Ignoring unknown RubySkynet config option #{k} => #{v}"}
  end

end
