require 'yaml'
require 'multi_json'
module RubySkynet
  module Zookeeper
    module Json

      # Deserialize from JSON entries in Zookeeper
      module Deserializer
        def self.deserialize(value)
          return value if value.nil? || (value == '')

          if value.strip.start_with?('{') || value.strip.start_with?('[{')
            symbolize(MultiJson.load(value))
          else
            symbolize_string(value)
          end
        end

        # Returns the supplied value symbolized
        def self.symbolize(v)
          if v.is_a?(Hash)
            symbolize_hash(v)
          elsif v.is_a?(Array)
            symbolize_array(v)
          elsif v.is_a?(String)
            symbolize_string(v)
          else
            v
          end
        end

        # Returns a new hash updated with keys and values that are strings
        # starting with ':' are turned into symbols
        def self.symbolize_hash(hash)
          h = hash.dup
          hash.each_pair do |k, v|
            # Convert values in the hash
            h[k] = symbolize(v)

            # Convert key to a symbol if it is a symbol string
            h[k[1..-1].to_sym] = h.delete(k) if k.is_a?(String) && k.start_with?(':')
          end
          h
        end

        # Returns a new Array with any symbols strings returned as symbols
        def self.symbolize_array(a)
          a.collect {|v| symbolize(v)}
        end

        # Returns a new string with the string parsed and symbol string converted to a symbol
        def self.symbolize_string(s)
          # JSON Parser cannot parse non-hash/array values
          value = YAML.load(s)
          # Now check for symbols which are strings starting with ':'
          value.is_a?(String) && value.start_with?(':') ? value[1..-1].to_sym : value
        rescue Exception
          s
        end

      end
    end
  end
end