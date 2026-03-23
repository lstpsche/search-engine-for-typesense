# frozen_string_literal: true

require 'test_helper'

class GroupingInstrumentationTest < Minitest::Test
  class Product < SearchEngine::Base
    collection 'products_grouping_instrumentation'
    identify_by :id
    attribute :brand_id, :integer
  end

  def test_grouping_compile_event_emitted_once_with_payload
    skip_unless_asn!

    received = []
    sub = ActiveSupport::Notifications.subscribe('search_engine.grouping.compile') do |*args|
      ev = ActiveSupport::Notifications::Event.new(*args)
      received << ev
    end

    begin
      rel = Product.all.group_by(:brand_id, limit: 1, missing_values: true)
      compiled = rel.to_typesense_params
      refute_nil compiled
    ensure
      ActiveSupport::Notifications.unsubscribe(sub)
    end

    assert_equal 1, received.length
    payload = received.first.payload

    assert_equal 'GroupingInstrumentationTest::Product', payload[:collection]
    assert_equal 'brand_id', payload[:group_by]
    assert_equal 1, payload[:group_limit]
    assert_equal true, payload[:missing_values]
    assert payload[:duration_ms].to_f >= 0.0 if payload.key?(:duration_ms)
  end

  def test_grouping_compile_event_not_emitted_without_grouping
    skip_unless_asn!

    received = []
    sub = ActiveSupport::Notifications.subscribe('search_engine.grouping.compile') do |*args|
      ev = ActiveSupport::Notifications::Event.new(*args)
      received << ev
    end

    begin
      compiled = Product.all.to_typesense_params
      refute_nil compiled
    ensure
      ActiveSupport::Notifications.unsubscribe(sub)
    end

    assert_equal 0, received.length
  end

  private

  def skip_unless_asn!
    skip 'ActiveSupport::Notifications not available' unless defined?(ActiveSupport::Notifications)
  end
end
