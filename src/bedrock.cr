require "json"
require "awscr-signer"
require "base64"
require "http/status"

require "./eventstream"
require "./client"
require "./bedrock_events"

module AWS
  module BedrockRuntime
    class Client < AWS::Client
      SERVICE_NAME = "bedrock-runtime"

      def initialize(
        @access_key_id = AWS.access_key_id,
        @secret_access_key = AWS.secret_access_key,
        @region = AWS.region,
        @endpoint : URI = URI.parse("https://bedrock-runtime.#{region}.amazonaws.com"),
        @sts_token : String? = nil,
      )
        @signer = Awscr::Signer::Signers::V4.new("bedrock", region, access_key_id, secret_access_key)
        @connection_pools = Hash({String, Int32?, Bool}, DB::Pool(HTTP::Client)).new
      end

      def converse_stream(
        model_id : String,
        body : String,
      ) : Iterator(String) | Tuple(HTTP::Status, String)
        # https://docs.aws.amazon.com/bedrock/latest/APIReference/API_runtime_ConverseStream.html
        # a bit different from invoke_model_with_response_stream
        headers = HTTP::Headers.new
        headers["Content-Type"] = "application/json"

        case @sts_token
        when String
          headers["X-Amz-Security-Token"] = @sts_token.not_nil!
        when Nil
          # do nothing
        else
          raise "Invalid STS token: #{@sts_token}"
        end

        http do |client|
          client.post(
            path: "/model/#{model_id}/converse-stream",
            headers: headers,
            body: body
          ) do |response|
            if response.success?
              io = response.body_io
              return EventStream::EventStream.new(io).map { |event| String.new(event.payload) }
            else
              return HTTP::Status.new(response.status_code), response.body_io.gets_to_end
            end
          end
        end
      end

      def invoke_model_with_response_stream(
        model_id : String,
        body : String,
        accept : String = "application/json",
        guardrail_identifier : String? = nil,
        guardrail_version : String? = nil,
        performance_config_latency : String? = nil,
        trace : String? = nil,
      ) : Iterator(String) | Tuple(HTTP::Status, String)
        headers = HTTP::Headers.new
        headers["X-Amzn-Bedrock-Accept"] = accept
        headers["Content-Type"] = "application/json"

        case @sts_token
        when String
          headers["X-Amz-Security-Token"] = @sts_token.not_nil!
        when Nil
          # do nothing
        else
          raise "Invalid STS token: #{@sts_token}"
        end

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
            if response.success?
              io = response.body_io
              return EventStream::EventStream.new(io).map { |event| InvokeStreamEvent.extract_payload(event) }
            else
              return HTTP::Status.new(response.status_code), response.body_io.gets_to_end
            end
          end
        end
      end
    end
  end
end
