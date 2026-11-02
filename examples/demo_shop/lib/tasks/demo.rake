# frozen_string_literal: true

namespace :demo do
  namespace :queries do
    desc 'Compile geo/vector examples without a database or server'
    task :compile do
      require_relative '../docs/query_examples'
      Docs::QueryExamples.run
    end

    desc 'Run examples on an explicitly configured disposable Typesense server'
    task :live do
      require_relative '../docs/query_examples'
      Docs::QueryExamples.run(live: true, auto_embedding: ENV['TYPESENSE_EMBEDDING_INTEGRATION'] == '1')
    end
  end

  namespace :index do
    desc 'Apply schema and rebuild products index'
    task all: :environment do
      puts '[demo] Applying schema for products...'
      Rake::Task['search_engine:schema:apply'].invoke('products')
      puts '[demo] Done.'
    end
  end
end
