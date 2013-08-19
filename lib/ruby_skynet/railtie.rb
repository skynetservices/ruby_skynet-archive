module RubySkynet #:nodoc:
  class Railtie < Rails::Railtie #:nodoc:

    # Exposes RubySkynet configuration to the Rails application configuration.
    #
    # @example Set up configuration in the Rails app.
    #   module MyApplication
    #     class Application < Rails::Application
    #       config.ruby_skynet.region = "Development"
    #     end
    #   end
    config.ruby_skynet = ::RubySkynet

    rake_tasks do
      load "ruby_skynet/railties/ruby_skynet.rake"
    end

    # Load RubySkynet Configuration once rails has started
    initializer 'ruby_skynet.initialize' do
      config_file = Rails.root.join("config", "ruby_skynet.yml")
      if config_file.file?
        ::RubySkynet.configure!(config_file, Rails.env)
      else
        puts "\nRuby Skynet config not found."
        puts "To generate one for the first time: rails generate ruby_skynet:config\n\n"
      end
    end

  end
end
