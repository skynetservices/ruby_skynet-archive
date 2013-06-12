require 'semantic_logger'

#
# RubySkynet Service
#
# Supports
#   Hosting Skynet Services
#   Skynet Service registration
#
module RubySkynet
  module Service

    def self.included(base)
      base.extend ::RubySkynet::Base::ClassMethods
      base.class_eval do
        include SemanticLogger::Loggable
      end
      # Register the service with the Server
      # The server will publish the server to services registry when the server is running
      Server.register_service(base)
    end

  end
end


