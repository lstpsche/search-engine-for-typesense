# frozen_string_literal: true

require 'json'
require 'securerandom'
require 'search_engine'

module Docs
  # Console and CLI examples that own their live collections and require no database.
  module QueryExamples
    # Coordinates used by the radius, viewport and distance examples.
    class Venue < SearchEngine::Base
      collection 'query_demo_venues'
      identify_by :id
      attribute :name, :string
      attribute :location, :geopoint
      query_by :name
    end

    # Deterministic external vectors require no embedding provider.
    class Article < SearchEngine::Base
      collection 'query_demo_articles'
      identify_by :id
      attribute :title, :string
      embedding :embedding, suffix: false, num_dim: 3
      query_by :title
    end

    # Server-side embeddings are used only with an explicit live opt-in.
    class SemanticArticle < SearchEngine::Base
      collection 'query_demo_semantic_articles'
      identify_by :id
      attribute :title, :string
      embedding :embedding, suffix: false, from: [:title], model: 'ts/all-MiniLM-L12-v2'
      query_by :title
    end

    CENTER = { lat: 54.6858, lng: 25.2877 }.freeze
    POLYGON = [[54.695, 25.300], [54.695, 25.275], [54.675, 25.275], [54.675, 25.300]].freeze
    ARTICLES = [
      { id: 'a', title: 'chair', embedding: [1, 0, 0] },
      { id: 'b', title: 'chair', embedding: [0, 1, 0] },
      { id: 'c', title: 'table', embedding: [0.9, 0.1, 0] }
    ].freeze

    module_function

    def relations
      polygon = "location:(#{POLYGON.flatten.join(',')})"
      {
        radius: Venue.where_geo(:location, within_radius: CENTER.merge(radius: '5 km'))
                .order_geo(:location, from: CENTER),
        polygon: Venue.where_geo(:location, within_polygon: POLYGON),
        viewport: Venue.order_eval(polygon, direction: :desc).order_geo(:location, from: CENTER),
        nearest: Article.vector_search(query: [1, 0, 0], k: 3).order(vector_distance: :asc),
        similar: Article.find_similar('a', k: 3),
        hybrid: Article.search('chair').vector_search(query: [1, 0, 0], alpha: 0.8, k: 3),
        semantic: SemanticArticle.search('chair').vector_search(alpha: 0.8, k: 3),
        history: SemanticArticle.vector_search(queries: ['phone, case', 'tablet'], weights: [0.5, 0.5])
      }
    end

    def highlight_params
      { q: 'chair', query_by: 'title', highlight_fields: 'title', include_fields: 'id,title',
        highlight_start_tag: '<mark>', highlight_end_tag: '</mark>' }
    end

    # Compile examples by default; live execution requires an isolated local endpoint.
    # @return [void]
    def run(live: false, auto_embedding: false, io: $stdout)
      raise ArgumentError, 'auto_embedding requires live: true' if auto_embedding && !live

      relations.each { |name, relation| io.puts "#{name}: #{JSON.generate(relation.to_h)}" }
      io.puts "highlight: #{JSON.generate(highlight_params)}"
      fixture = SearchEngine::Result.new(
        { 'found' => 1, 'hits' => [{ 'document' => { 'id' => 'a', 'title' => 'chair' },
                                   'vector_distance' => 0.0 }] }, klass: Article
      )
      check(fixture.hits.first.vector_distance == 0.0, 'Backend distance metadata was lost')
      return io.puts('Compilation and metadata fixtures OK (no server requests)') unless live

      run_live(auto_embedding: auto_embedding, io: io)
    end

    def run_live(auto_embedding:, io:)
      config = isolated_config
      client = SearchEngine::Client.new(config: config)
      original_client = SearchEngine.config.client
      originals = [Venue, Article, SemanticArticle].to_h { |model| [model, model.collection] }
      owned = []
      SearchEngine.config.client = client
      begin
        prepare_fixtures(client, owned)
        check_live_queries(client, io)
        run_semantic(client, owned, io) if auto_embedding
        io.puts 'Live geo, vector and highlighting checks OK'
      ensure
        begin
          owned.reverse_each { |name| client.delete_collection(name) }
        ensure
          originals.each { |model, name| model.collection(name) }
          SearchEngine.config.client = original_client
        end
      end
    end

    def prepare_fixtures(client, owned)
      prepare_collection(client, owned, Venue,
                         [{ name: 'name', type: 'string' }, { name: 'location', type: 'geopoint' }],
                         [{ id: 'near', name: 'Cathedral', location: [54.6858, 25.2877] },
                          { id: 'next', name: 'Tower', location: [54.6867, 25.2912] },
                          { id: 'far', name: 'Kaunas', location: [54.8985, 23.8858] }]
      )
      prepare_collection(client, owned, Article,
                         [{ name: 'title', type: 'string' },
                          { name: 'embedding', type: 'float[]', num_dim: 3 }], ARTICLES
      )
    end

    def check_live_queries(client, io)
      queries = relations
      check(hit_ids(queries[:radius].execute) == %w[near next], 'Radius results differ')
      check(hit_ids(queries[:polygon].execute).sort == %w[near next], 'Polygon results differ')
      check(hit_ids(queries[:viewport].execute) == %w[near next far], 'Viewport order differs')
      nearest = queries[:nearest].execute
      check(hit_ids(nearest) == %w[a c b], 'Nearest-neighbor order differs')
      check(nearest.hits.first.vector_distance.abs < 0.00001, 'Expected zero distance')
      check(hit_ids(queries[:similar].execute).first == 'c', 'Similarity results differ')
      hybrid = queries[:hybrid].execute
      check(hybrid.found.positive?, 'Hybrid search returned no hits')
      highlighted = client.search(collection: Article.collection, params: highlight_params,
                                  url_opts: { use_cache: false }
      )
      snippet = highlighted.hits.first.snippet_for(:title)
      check(snippet.include?('<mark>chair</mark>'), 'Expected a highlighted title')
      distances = nearest.hits.map { |hit| [hit.instance_variable_get(:@id), hit.vector_distance] }
      io.puts JSON.generate(nearest: distances, highlight: snippet)
    end

    def isolated_config
      unless ENV['TYPESENSE_TEST_ISOLATED'] == '1'
        raise ArgumentError, 'Set TYPESENSE_TEST_ISOLATED=1 for an explicitly disposable server'
      end

      host = ENV.fetch('TYPESENSE_HOST')
      raise ArgumentError, 'Live examples require a loopback host' unless %w[127.0.0.1 localhost ::1].include?(host)

      config = SearchEngine::Config.new
      config.host = host
      config.port = Integer(ENV.fetch('TYPESENSE_PORT'))
      config.protocol = 'http'
      config.api_key = ENV.fetch('TYPESENSE_API_KEY')
      config.default_infix = 'off'
      config.use_cache = false
      config.retries = { attempts: 1, backoff: 0 }
      config.timeout_ms = 120_000
      config
    end

    def prepare_collection(client, owned, model, fields, documents)
      name = "query_demo_#{Process.pid}_#{SecureRandom.hex(6)}"
      client.create_collection(name: name, fields: fields)
      owned << name
      model.collection(name)
      response = client.import_documents(collection: name,
                                         jsonl: documents.map { |document| JSON.generate(document) }.join("\n")
                                        )
      response.lines.each do |line|
        row = JSON.parse(line)
        check(row.fetch('success'), "Import failed: #{row['error']}")
      end
    end

    def run_semantic(client, owned, io)
      prepare_collection(client, owned, SemanticArticle,
                         [{ name: 'title', type: 'string' },
                          { name: 'embedding', type: 'float[]',
                            embed: { from: ['title'], model_config: { model_name: 'ts/all-MiniLM-L12-v2' } } }],
                         ARTICLES.map { |document| document.slice(:id, :title) }
      )
      %i[semantic history].each do |name|
        result = relations.fetch(name).execute
        check(result.found.positive?, "#{name} returned no hits")
        check(result.hits.first.vector_distance.is_a?(Numeric), "#{name} distance is missing")
        io.puts "#{name}: #{JSON.generate(hit_ids(result))}"
      end
    end

    def hit_ids(result)
      result.hits.map { |hit| hit.instance_variable_get(:@id) }
    end

    def check(condition, message)
      raise message unless condition
    end
  end
end
