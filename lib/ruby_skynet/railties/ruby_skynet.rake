namespace :ruby_skynet do

  desc "Start the Ruby Skynet Server.\n Rails Example: rake ruby_skynet:server\n Without Rails: SKYNET_ENV=production SKYNET_CONFIG=config/ruby_skynet rake ruby_skynet:server"
  task :server => :environment do
    # Configuration is automatically loaded when running under Rails
    # so skip it here under Rails
    unless defined?(Rails)
      # Environment to use in config file
      environment = ENV['SKYNET_ENV']

      # Environment to use in config file
      cfg_file = ENV['SKYNET_CONFIG']

      # Load the configuration file
      RubySkynet.configure!(cfg_file, environment)
    end

    # Connect to services registry
    RubySkynet.services

    RubySkynet::Server.load_services

    begin
      # Start the server
      RubySkynet::Server.start
      RubySkynet::Server.wait_until_server_stops
    ensure
      RubySkynet::Server.stop
    end

  end

end
