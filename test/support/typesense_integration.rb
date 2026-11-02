# frozen_string_literal: true

require 'net/http'
require 'json'
require 'securerandom'

# Fixtures are deliberately restricted to explicitly configured disposable loopback servers.
module TypesenseIntegration
  def setup_typesense
    skip 'Set TYPESENSE_INTEGRATION=1 and TYPESENSE_TEST_ISOLATED=1 for disposable servers' unless
      ENV['TYPESENSE_INTEGRATION'] == '1' && ENV['TYPESENSE_TEST_ISOLATED'] == '1'

    host = ENV.fetch('TYPESENSE_HOST')
    unless %w[127.0.0.1 localhost ::1].include?(host)
      raise ArgumentError, 'Integration fixtures require a loopback host'
    end

    @integration_config = SearchEngine::Config.new
    @integration_config.host = host
    @integration_config.port = Integer(ENV.fetch('TYPESENSE_PORT'))
    @integration_config.protocol = 'http'
    @integration_config.api_key = ENV.fetch('TYPESENSE_API_KEY')
    @integration_config.default_infix = 'off'
    @integration_config.use_cache = false
    @integration_config.retries = { attempts: 1, backoff: 0 }
    @client = SearchEngine::Client.new(config: @integration_config)
    @original_client = SearchEngine.config.client
    SearchEngine.config.client = @client
    @owned_collections = []
    @owned_aliases = []
    @owned_keys = []
  end

  def teardown_typesense
    return unless @owned_collections

    @owned_aliases.reverse_each { |name| request_json(:delete, "/aliases/#{name}") }
    @owned_keys.reverse_each { |id| request_json(:delete, "/keys/#{id}") }
    @owned_collections.reverse_each { |name| @client.delete_collection(name) }
  ensure
    SearchEngine.config.client = @original_client if @owned_collections
  end

  def fixture_collection(fields)
    name = "query_fixture_#{Process.pid}_#{SecureRandom.hex(6)}"
    @client.create_collection(name: name, fields: fields)
    @owned_collections << name
    name
  end

  def import_fixture(collection, documents)
    data = documents.map { |document| JSON.generate(document) }.join("\n")
    body = request_body(:post, "/collections/#{collection}/documents/import", data,
                        params: { action: 'upsert' }, expected: 200
    )
    body.lines.each { |line| assert JSON.parse(line).fetch('success'), line }
  end

  def request_json(method, path, body = nil, **options)
    JSON.parse(request_body(method, path, body, **options))
  end

  def request_body(method, path, body = nil, params: {}, key: @integration_config.api_key, expected: nil)
    uri = URI::HTTP.build(host: @integration_config.host, port: @integration_config.port,
                          path: path, query: params.empty? ? nil : URI.encode_www_form(params)
    )
    request = Net::HTTP.const_get(method.to_s.capitalize).new(uri)
    request['X-TYPESENSE-API-KEY'] = key
    request['Content-Type'] = 'application/json'
    request.body = body.is_a?(String) ? body : JSON.generate(body) unless body.nil?
    response = Net::HTTP.start(uri.host, uri.port, open_timeout: 5, read_timeout: 120) { |http| http.request(request) }
    accepted = expected ? response.code.to_i == expected : response.is_a?(Net::HTTPSuccess)
    assert accepted, "#{method.upcase} #{path}: HTTP #{response.code}: #{response.body}"
    response.body
  end

  def fixture_key(collection, **parameters)
    result = request_json(:post, '/keys',
                          { description: 'Disposable search fixture', actions: ['documents:search'],
                            collections: [collection] }.merge(parameters)
    )
    @owned_keys << result.fetch('id')
    result
  end

  def scoped_fixture_key(parent, parameters)
    sdk = Typesense::Client.new(api_key: @integration_config.api_key,
                                nodes: [{ host: @integration_config.host, port: @integration_config.port,
                                          protocol: 'http' }]
                               )
    sdk.keys.generate_scoped_search_key(parent.fetch('value'), parameters)
  end

  def eventually(timeout: 5)
    deadline = Process.clock_gettime(Process::CLOCK_MONOTONIC) + timeout
    loop do
      return if yield

      flunk 'Timed out waiting for the expected native-cache state' if
        Process.clock_gettime(Process::CLOCK_MONOTONIC) >= deadline
      sleep 0.1
    end
  end
end
