require "json"
require "awscr-signer"
require "base64"

require "./eventstream"
require "./client"

module AWS
  module BedrockRuntime
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


      def generic_event_to_bedrock_event(event : EventStream::EventMessage) : JSON::Any
        payload_hash = JSON.parse(event.payload).as_h
        # named "bytes" but that doesn't make sense for JSON
        encoded_bytes = payload_hash["bytes"].as_s
        inner_json_bytes = Base64.decode(encoded_bytes)
        inner_json_str = String.new(inner_json_bytes)
        JSON.parse(inner_json_str)
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
      ) : Iterator(JSON::Any)
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