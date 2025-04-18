require "base64"

module AWS
  module EventStream
    # Each message has this structure:
    #
    #   total_length   : Int32  (4 bytes)
    #   headers_length : Int32  (4 bytes)
    #   prelude_crc    : UInt32  (4 bytes)
    #   headers        : Bytes  (headers_length bytes)
    #   payload        : Bytes  (total_length - headers_length - 16 bytes)
    #   message_crc    : UInt32  (4 bytes)
    #
    # Note: We read the raw headers block here but do not parse individual header fields

    struct EventMessage
      getter headers : Hash(String, String)
      getter payload : String

      def initialize(@headers, @payload)
      end
    end

    class EventStream
      include Iterator(EventMessage)

      def initialize(@io : IO)
      end

      def check_crc(headers_string, payload_string, total_length : Int32, headers_length : Int32, prelude_crc : UInt32, message_crc : UInt32)
        prelude_buffer = IO::Memory.new(8)
        prelude_buffer.write_bytes(total_length, IO::ByteFormat::BigEndian)
        prelude_buffer.write_bytes(headers_length, IO::ByteFormat::BigEndian)

        calculated_crc = Digest::CRC32.checksum(prelude_buffer.to_slice)
        if calculated_crc != prelude_crc
          raise "Prelude CRC mismatch"
        end

        message_buffer = IO::Memory.new(total_length - 4)
        message_buffer.write_bytes(total_length, IO::ByteFormat::BigEndian)
        message_buffer.write_bytes(headers_length, IO::ByteFormat::BigEndian)
        message_buffer.write_bytes(prelude_crc, IO::ByteFormat::BigEndian)
        message_buffer.write(headers_string.to_slice)
        message_buffer.write(payload_string.to_slice)

        calculated_message_crc = Digest::CRC32.checksum(message_buffer.to_slice)
        if calculated_message_crc != message_crc
          raise "Message CRC mismatch"
        end
      end

      def headers_from_string(string : String) : Hash(String, String)
        # example :
        # "\x0b:event-type\x07\x00\x05chunk\x0d:content-type\x07\x00\x10application/json\x0d:message-type\x07\x00\x05event"
        # format is key-length-byte, key, type-byte??, value-length-bytes, value
        headers_hash = Hash(String, String).new
        bytes = string.to_slice
        i = 0
        while i < bytes.size
          key_length = bytes[i].to_u8
          key = bytes[i + 1, key_length]
          type_byte = bytes[i + key_length + 1].to_u8
          value_length = (bytes[i + key_length + 2].to_u16 << 8) | bytes[i + key_length + 3].to_u16
          value = bytes[i + key_length + 4, value_length]
          headers_hash[key.to_s] = value.to_s
          i += key_length + 4 + value_length
        end
        headers_hash
      end

      def next
        if @io.closed?
          return stop
        end
        total_length : Int32 = @io.read_bytes(Int32, IO::ByteFormat::BigEndian)
        headers_length : Int32 = @io.read_bytes(Int32, IO::ByteFormat::BigEndian)
        prelude_crc = @io.read_bytes(UInt32, IO::ByteFormat::BigEndian)

        headers_string = @io.read_string(headers_length)
        headers_hash = headers_from_string(headers_string)
        # total_length includes: 4 + 4 + headers_length + payload + 4 for CRC
        # So payload length = total_length - 4(total len) - 4(headers len) - 4(prelude_crc) - headers_length - 4(message CRC)
        payload_length = total_length - 4 - 4 - 4 - headers_length - 4

        if payload_length < 0
          raise "Payload length is negative"
        end

        payload_string = @io.read_string(payload_length)
        message_crc = @io.read_bytes(UInt32, IO::ByteFormat::BigEndian)

        EventMessage.new(
          headers_hash,
          payload_string
        )
      end
    end
  end
end
