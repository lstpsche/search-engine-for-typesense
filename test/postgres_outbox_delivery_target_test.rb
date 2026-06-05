# frozen_string_literal: true

require 'test_helper'

class PostgresOutboxDeliveryTargetTest < Minitest::Test
  DeliveryTarget = SearchEngine::PostgresOutbox::DeliveryTarget

  TargetLike = Struct.new(:key, :queue_name)

  def test_normalizes_key_and_queue_name_to_strings
    target = DeliveryTarget.new(key: :target_1, queue_name: :target_queue)

    assert_equal 'target_1', target.key
    assert_equal 'target_queue', target.queue_name
  end

  def test_normalizes_hash_input
    target = DeliveryTarget.normalize({ key: :target_1, queue_name: :target_queue })

    assert_equal 'target_1', target.key
    assert_equal 'target_queue', target.queue_name
  end

  def test_normalizes_string_key_hash_input
    target = DeliveryTarget.normalize({ 'key' => :target_1, 'queue_name' => :target_queue })

    assert_equal 'target_1', target.key
    assert_equal 'target_queue', target.queue_name
  end

  def test_normalizes_target_like_object
    target = DeliveryTarget.normalize(TargetLike.new(:target_1, :target_queue))

    assert_equal 'target_1', target.key
    assert_equal 'target_queue', target.queue_name
  end

  def test_returns_existing_target
    target = DeliveryTarget.new(key: :target_1, queue_name: :target_queue)

    assert_same target, DeliveryTarget.normalize(target)
  end

  def test_rejects_blank_key
    assert_raises(ArgumentError) do
      DeliveryTarget.new(key: ' ', queue_name: :target_queue)
    end
  end

  def test_rejects_blank_queue_name
    assert_raises(ArgumentError) do
      DeliveryTarget.new(key: :target_1, queue_name: ' ')
    end
  end

  def test_rejects_unsupported_input
    assert_raises(ArgumentError) do
      DeliveryTarget.normalize(:target_1)
    end
  end
end
