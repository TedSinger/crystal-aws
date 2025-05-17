require "json"

module AWS
  module BedrockRuntime
    macro handle_json_access_pattern(known_primitive_keys = [] of String, known_object_keys = [] of String)
      def [](key : String)
        {% for known_key in known_primitive_keys %}
        return JSON::Any.new(@{{known_key.id}}) if key == {{known_key}}
        {% end %}
        {% for known_key in known_object_keys %}
        return JSON.parse(@{{known_key.id}}.to_json) if key == {{known_key}}
        {% end %}
        @json_unmapped[key]
      end
    end

    class ConverseStreamEvent
      include JSON::Serializable
      include JSON::Serializable::Unmapped

      AWS::BedrockRuntime.handle_json_access_pattern([] of String, [] of String)

      def self.from_event_payload(json_str : String) : ConverseStreamEvent
        payload_hash = JSON.parse(json_str).as_h
        if payload_hash["contentBlockIndex"]? && payload_hash["delta"]?
          ContentBlockDelta.from_json(json_str)
        elsif payload_hash["stopReason"]?
          ContentBlockStop.from_json(json_str)
        elsif payload_hash["metrics"]?
          Metrics.from_json(json_str)
        else
          # {"contentBlockIndex" => 0, "p" => "abcdefghij"}
          ConverseStreamEvent.from_json(json_str)
        end
      end

      class ContentBlockDelta < ConverseStreamEvent
        # {"contentBlockIndex" => 0, "delta" => {"text" => " situation or topic you're inqu"}, "p" => "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNO"}
        property contentBlockIndex : Int32
        property delta : Delta

        AWS::BedrockRuntime.handle_json_access_pattern(["contentBlockIndex", "delta"], [] of String)

        class Delta < ConverseStreamEvent
          property text : String?
          property toolUse : ToolUse?
          AWS::BedrockRuntime.handle_json_access_pattern(["text", "toolUse"], [] of String)

          class ToolUse < ConverseStreamEvent
            property input : String
            AWS::BedrockRuntime.handle_json_access_pattern(["input"], [] of String)
          end
        end
      end

      class ContentBlockStop < ConverseStreamEvent
        # {"p" => "abcdefghijklmnopqrstu", "stopReason" => "end_turn"}
        property stopReason : String
        AWS::BedrockRuntime.handle_json_access_pattern(["stopReason"], [] of String)
      end

      class Metrics < ConverseStreamEvent
        # {"metrics" => {"latencyMs" => 2467}, "p" => "abcdefghijklmnopqrstuvwxyzABCDEFGHIJ", "usage" => {"inputTokens" => 304, "outputTokens" => 70, "totalTokens" => 374}}
        property metrics : Metrics
        property usage : Usage
        AWS::BedrockRuntime.handle_json_access_pattern(["metrics", "usage"], [] of String)

        class Metrics < ConverseStreamEvent
          property latencyMs : Int32
          AWS::BedrockRuntime.handle_json_access_pattern(["latencyMs"], [] of String)
        end

        class Usage < ConverseStreamEvent
          property inputTokens : Int32
          property outputTokens : Int32
          property totalTokens : Int32
          AWS::BedrockRuntime.handle_json_access_pattern(["inputTokens", "outputTokens", "totalTokens"], [] of String)
        end
      end
    end

    class InvokeStreamEvent
      include JSON::Serializable
      include JSON::Serializable::Unmapped

      AWS::BedrockRuntime.handle_json_access_pattern([] of String, [] of String)

      def self.extract_payload(event : EventStream::EventMessage) : String
        payload_hash = JSON.parse(String.new(event.payload)).as_h
        # named "bytes" but that doesn't make sense for JSON
        encoded_bytes = payload_hash["bytes"].as_s
        # The only other field is "p" which appears to be a sanity check. Its value is some amount of the alphabet, in order, lowercase, then uppercase, then digits.
        inner_json_bytes = Base64.decode(encoded_bytes)
        String.new(inner_json_bytes)
      end

      def self.from_event_payload(json_str : String) : InvokeStreamEvent
        raw = JSON.parse(json_str).as_h

        case raw["type"]
        when "message_start"
          MessageStart.from_json(json_str)
        when "content_block_start"
          ContentBlockStart.from_json(json_str)
        when "content_block_delta"
          ContentBlockDelta.from_json(json_str)
        when "content_block_stop"
          ContentBlockStop.from_json(json_str)
        else
          # No subtype - everything can be accessed through the unmapped json
          InvokeStreamEvent.from_json(json_str)
        end
      end

      class MessageStart < InvokeStreamEvent
        # {"type" => "message_start", "message" => {"id" => "msg_bdrk_01GuZRyDETP2CY6ZsiYoLgZT", "type" => "message", "role" => "assistant", "model" => "claude-3-5-sonnet-20241022", "content" => [], "stop_reason" => nil, "stop_sequence" => nil, "usage" => {"input_tokens" => 91, "cache_creation_input_tokens" => 0, "cache_read_input_tokens" => 0, "output_tokens" => 7}}}
        property type : String
        property message : Message
        AWS::BedrockRuntime.handle_json_access_pattern(["type"], ["message"])

        class Message < InvokeStreamEvent
          property id : String
          property type : String
          property role : String
          property model : String
          property content : Array(JSON::Any)
          property stop_reason : String?
          property stop_sequence : String?
          property usage : Usage
          AWS::BedrockRuntime.handle_json_access_pattern(["id", "type", "role", "model", "content", "stop_reason", "stop_sequence"], ["usage"])

          class Usage < InvokeStreamEvent
            property input_tokens : Int32
            property cache_creation_input_tokens : Int32?
            property cache_read_input_tokens : Int32?
            property output_tokens : Int32
            AWS::BedrockRuntime.handle_json_access_pattern(["input_tokens", "cache_creation_input_tokens", "cache_read_input_tokens", "output_tokens"], [] of String)
          end
        end
      end

      class ContentBlockStart < InvokeStreamEvent
        # {"type" => "content_block_start", "index" => 0, "content_block" => {"type" => "text", "text" => ""}}
        property type : String
        property index : Int32
        property content_block : ContentBlock
        AWS::BedrockRuntime.handle_json_access_pattern(["type", "index"], ["content_block"])

        class ContentBlock < InvokeStreamEvent
          property type : String
          property text : String
          AWS::BedrockRuntime.handle_json_access_pattern(["type", "text"], [] of String)
        end
      end

      class ContentBlockDelta < InvokeStreamEvent
        # {"type" => "content_block_delta", "index" => 0, "delta" => {"type" => "text_delta", "text" => "\n\nA jungle fowl wandere"}}
        property type : String
        property index : Int32
        property delta : Delta

        AWS::BedrockRuntime.handle_json_access_pattern(["type", "index"], ["delta"])

        class Delta < InvokeStreamEvent
          property type : String
          property text : String
          AWS::BedrockRuntime.handle_json_access_pattern(["type", "text"], [] of String)
        end
      end

      class ContentBlockStop < InvokeStreamEvent
        # {"type" => "content_block_stop", "index" => 0}
        property type : String
        property index : Int32
        AWS::BedrockRuntime.handle_json_access_pattern(["type", "index"], [] of String)
      end
    end
  end
end
