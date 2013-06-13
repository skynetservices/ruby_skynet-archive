require 'semantic_logger'
module RubySkynet
  module Zookeeper
    autoload :Registry,        'ruby_skynet/zookeeper/registry'
    autoload :CachedRegistry,  'ruby_skynet/zookeeper/cached_registry'
    module Json
      autoload :Deserializer,  'ruby_skynet/zookeeper/json/deserializer'
      autoload :Serializer,    'ruby_skynet/zookeeper/json/serializer'
    end
  end
end