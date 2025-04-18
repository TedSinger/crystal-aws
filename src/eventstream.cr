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
      getter headers : String
      getter payload : String

      def initialize(@headers : String, @payload)
      end

      def check_crc(total_length : Int32, headers_length : Int32, prelude_crc : UInt32, message_crc : UInt32)
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
        message_buffer.write(@headers.to_slice)
        message_buffer.write(@payload.to_slice)

        calculated_message_crc = Digest::CRC32.checksum(message_buffer.to_slice)
        if calculated_message_crc != message_crc
          raise "Message CRC mismatch"
        end

      end
    end


    class EventStream
      include Iterator(EventMessage)

      def initialize(@io : IO)
      end
    
      def next
        if @io.closed?
          return stop
        end
        total_length : Int32 = @io.read_bytes(Int32, IO::ByteFormat::BigEndian)
        headers_length : Int32 = @io.read_bytes(Int32, IO::ByteFormat::BigEndian)
        prelude_crc = @io.read_bytes(UInt32, IO::ByteFormat::BigEndian)

        headers_string = @io.read_string(headers_length)
        # total_length includes: 4 + 4 + headers_length + payload + 4 for CRC
        # So payload length = total_length - 4(total len) - 4(headers len) - 4(prelude_crc) - headers_length - 4(message CRC)
        payload_length = total_length - 4 - 4 - 4 - headers_length - 4
    
        if payload_length < 0
          raise "Payload length is negative"
        end

        payload_string = @io.read_string(payload_length)
        message_crc = @io.read_bytes(UInt32, IO::ByteFormat::BigEndian)

        e = EventMessage.new(
          headers_string,
          payload_string
        )
        e.check_crc(total_length, headers_length, prelude_crc, message_crc)
        e
      end
    end
  end
end