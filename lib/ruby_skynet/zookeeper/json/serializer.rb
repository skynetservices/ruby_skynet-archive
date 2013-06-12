require 'multi_json'
module RubySkynet
  module Zookeeper
    module Json

      # Serialize to JSON for storing in Doozer
      module Serializer
        def self.serialize(value)
          if value.is_a?(Hash) || value.is_a?(Array)
            MultiJson.encode(desymbolize(value))
          elsif value.is_a?(Symbol)
            desymbolize_symbol(value)
          else
            value.to_s
          end
        end

        # Returns the supplied value with symbols converted to a string prefixed
        # with ':'
        def self.desymbolize(v)
          if v.is_a?(Hash)
            desymbolize_hash(v)
          elsif v.is_a?(Array)
            desymbolize_array(v)
          elsif v.is_a?(Symbol)
            desymbolize_symbol(v)
          else
            v.to_s
          end
        end

        # Returns a new hash with all symbol keys and values as strings starting with ':'
        def self.desymbolize_hash(hash)
          h = hash.dup
          hash.each_pair do |k, v|
            # Convert values in the hash
            h[k] = desymbolize(v)

            # Convert key to a string if it is a symbol
            h[desymbolize_symbol(k)] = h.delete(k) if k.is_a?(Symbol)
          end
          h
        end

        # Returns a new Array with any symbols returned as symbol strings
        def self.desymbolize_array(a)
          a.collect {|v| desymbolize(v)}
        end

        def self.desymbolize_symbol(s)
          ":#{s}"
        end

      end
    end
  end
end