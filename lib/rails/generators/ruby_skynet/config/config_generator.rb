module RubySkynet
  module Generators
    class ConfigGenerator < Rails::Generators::Base
      desc "Creates a Ruby Skynet configuration file at config/ruby_skynet.yml"

      argument :key_path, :type => :string, :optional => false

      def self.source_root
        @_ruby_skynet_source_root ||= File.expand_path("../templates", __FILE__)
      end

      def app_name
        Rails::Application.subclasses.first.parent.to_s.underscore
      end

      def create_config_file
        template 'ruby_skynet.yml', File.join('config', "ruby_skynet.yml")
      end

    end
  end
end
