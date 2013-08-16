namespace :ruby_skynet do

  desc "Start the Ruby Skynet Server.\n Rails Example: rake ruby_skynet:server\n Without Rails: SKYNET_ENV=production SKYNET_CONFIG=config/ruby_skynet rake ruby_skynet:server"
  task :server => :environment do
    # Configuration is automatically loaded when running under Rails
    # so skip it here under Rails
    unless defined?(Rails)
      # Environment to use in config file
      # Defaults to Rails.env
      environment = ENV['SKYNET_ENV']

      # Environment to use in config file
      # Defaults to config/ruby_skynet.yml
      cfg_file = ENV['SKYNET_CONFIG']

      # Load the configuration file
      RubySkynet.configure!(cfg_file, environment)
    end

    ruby_skynet_server = RubySkynet::Server.new
    ruby_skynet_server.register_services_in_path(TestService)

    at_exit do
      ruby_skynet_server.close
    end

    ruby_skynet_server.wait_until_server_stops
  end

end
