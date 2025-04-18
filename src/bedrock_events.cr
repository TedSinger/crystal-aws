require "json"

module AWS
  module BedrockRuntime
    abstract class BedrockRuntimeEvent
      include JSON::Serializable

      property type : String

      def self.specialize_from_json(json : String) : BedrockRuntimeEvent
        h = JSON.parse(json).as_h
        case h["type"]
        when "message_start"
          MessageStart.from_json(json)
        when "content_block_start"
          ContentBlockStart.from_json(json)
        when "content_block_delta"
          ContentBlockDelta.from_json(json)
        when "content_block_stop"
          ContentBlockStop.from_json(json)
        else
          raise "Unknown event type: #{json}"
        end
      end

      class MessageStart < BedrockRuntimeEvent
        # {"type" => "message_start", "message" => {"id" => "msg_bdrk_01GuZRyDETP2CY6ZsiYoLgZT", "type" => "message", "role" => "assistant", "model" => "claude-3-5-sonnet-20241022", "content" => [], "stop_reason" => nil, "stop_sequence" => nil, "usage" => {"input_tokens" => 91, "cache_creation_input_tokens" => 0, "cache_read_input_tokens" => 0, "output_tokens" => 7}}}
        include JSON::Serializable

        property type : String
        property message : Message

        struct Message
          include JSON::Serializable

          property id : String
          property type : String
          property role : String
          property model : String
          property content : Array(JSON::Any)
          @[JSON::Field(key: "stop_reason")]
          property stop_reason : String?
          @[JSON::Field(key: "stop_sequence")]
          property stop_sequence : String?
          property usage : Usage

          struct Usage
            include JSON::Serializable

            @[JSON::Field(key: "input_tokens")]
            property input_tokens : Int32
            @[JSON::Field(key: "cache_creation_input_tokens")]
            property cache_creation_input_tokens : Int32?
            @[JSON::Field(key: "cache_read_input_tokens")]
            property cache_read_input_tokens : Int32?
            @[JSON::Field(key: "output_tokens")]
            property output_tokens : Int32
          end
        end
      end

      class ContentBlockStart < BedrockRuntimeEvent
        # {"type" => "content_block_start", "index" => 0, "content_block" => {"type" => "text", "text" => ""}}
        include JSON::Serializable

        property type : String
        property index : Int32
        @[JSON::Field(key: "content_block")]
        property content_block : ContentBlock

        struct ContentBlock
          include JSON::Serializable

          property type : String
          property text : String
        end
      end

      class ContentBlockDelta < BedrockRuntimeEvent
        # {"type" => "content_block_delta", "index" => 0, "delta" => {"type" => "text_delta", "text" => "\n\nA jungle fowl wandere"}}
        include JSON::Serializable

        property type : String
        property index : Int32
        property delta : Delta

        struct Delta
          include JSON::Serializable

          property type : String
          property text : String
        end
      end

      class ContentBlockStop < BedrockRuntimeEvent
        # {"type" => "content_block_stop", "index" => 0}
        include JSON::Serializable

        property type : String
        property index : Int32
      end
    end
  end
end
