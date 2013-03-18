module RubySkynet
  module Common

    # Returns a BSON document read from the socket.
    # Returns nil if the operation times out or if a network
    #         connection failure occurs
    def self.read_bson_document(socket)
      bytebuf = BSON::ByteBuffer.new
      # Read 4 byte size of following BSON document
      bytes = socket.read(4)

      # No more data
      return unless bytes

      # Read BSON document
      sz = bytes.unpack("V")[0]
      raise "Invalid Data received from server:#{bytes.inspect}" unless sz

      bytebuf.append!(bytes)
      bytebuf.append!(socket.read(sz - 4))
      raise "Celluloid is not returning #{sz} requested bytes. #{bytebuf.length} bytes returned" unless sz == bytebuf.length
      return BSON.deserialize(bytebuf)
    end

  end
end