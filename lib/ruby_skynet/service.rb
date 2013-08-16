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
      base.extend ::RubySkynet::Common::ClassMethods
      base.class_eval do
        include SemanticLogger::Loggable
      end
    end

  end
end


