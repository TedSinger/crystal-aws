require "json"
require "awscr-signer"
require "base64"

require "./eventstream"
require "./client"

module AWS
  module BedrockRuntime
    class BedrockRuntimeEvent
      include JSON::Serializable
      class MessageStart

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
            property cache_creation_input_tokens : Int32
            @[JSON::Field(key: "cache_read_input_tokens")]
            property cache_read_input_tokens : Int32
            @[JSON::Field(key: "output_tokens")]
            property output_tokens : Int32
          end
        end
      end

      class ContentBlockStart
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

      class ContentBlockDelta
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

      class ContentBlockStop
        # {"type" => "content_block_stop", "index" => 0}
        include JSON::Serializable

        property type : String
        property index : Int32
      end
    end

    class Client < AWS::Client
      SERVICE_NAME = "bedrock-runtime"
      def initialize(
          @access_key_id = AWS.access_key_id,
          @secret_access_key = AWS.secret_access_key,
          @region = AWS.region,
          @endpoint = URI.parse("https://bedrock-runtime.#{region}.amazonaws.com"),
        )
        @signer = Awscr::Signer::Signers::V4.new("bedrock", region, access_key_id, secret_access_key)
        @connection_pools = Hash({String, Int32?, Bool}, DB::Pool(HTTP::Client)).new
      end


      def generic_event_to_bedrock_event(event : EventStream::EventMessage) : Hash(String, JSON::Any)
        payload_hash = JSON.parse(String.new(event.payload)).as_h
        # named "bytes" but that doesn't make sense for JSON
        encoded_bytes = payload_hash["bytes"].as_s
        # The only other field is "p" which appears to be a sanity check. Its value is some amount of the alphabet, in order, lowercase, then uppercase, then digits.
        inner_json_bytes = Base64.decode(encoded_bytes)
        inner_json_str = String.new(inner_json_bytes)
        ret = JSON.parse(inner_json_str).as_h
        puts BedrockRuntimeEvent.from_json(inner_json_str)
        ret
      end

      def invoke_model_with_response_stream(
        model_id : String,
        body : String,
        accept : String = "application/json",
        content_type : String = "application/json",
        guardrail_identifier : String? = nil,
        guardrail_version : String? = nil,
        performance_config_latency : String? = nil,
        trace : String? = nil
      ) : Iterator(Hash(String, JSON::Any))
        headers = HTTP::Headers.new
        headers["X-Amzn-Bedrock-Accept"] = accept
        headers["Content-Type"] = content_type


        if guardrail_identifier
          headers["X-Amzn-Bedrock-GuardrailIdentifier"] = guardrail_identifier
        end

        if guardrail_version
          headers["X-Amzn-Bedrock-GuardrailVersion"] = guardrail_version
        end

        if performance_config_latency
          headers["X-Amzn-Bedrock-PerformanceConfig-Latency"] = performance_config_latency
        end

        if trace
          headers["X-Amzn-Bedrock-Trace"] = trace
        end

        http do |client|
          client.post(
            path: "/model/#{model_id}/invoke-with-response-stream",
            headers: headers,
            body: body
          ) do |response|
            io = response.body_io
            return EventStream::EventStream.new(io).map { |event| generic_event_to_bedrock_event(event) }
          end
        end
        
      end

    end
  end
end