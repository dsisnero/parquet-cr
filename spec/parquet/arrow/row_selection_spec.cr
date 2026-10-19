require "../../spec_helper"

# Ported from `parquet/src/arrow/arrow_reader/selection/` (apache/arrow-rs
# 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b): `selector.rs` (the
# RLE-backed `RowSelection`: normalization, consecutive ranges, offset/limit,
# trim, split_off, counts, and the selector transforms), `boolean.rs` (the
# bitmap-backed `MaskSelection`: `from_boolean_buffer`, `from_filters`,
# `as_mask`, `MaskRunIter`, the mask primitives and their fuzz equivalences),
# `algebra.rs` (`and_then`/`intersection`/`union`) and the mixed-backing parts
# of `mod.rs` (equality, concatenation, counts).
#
# Divergences: `usize` counts are `Int32`; the fuzz loops use a seeded Crystal
# `Random` instead of `rand`; the `OnceLock` cache pointer/capacity assertions
# are replaced by observable behaviour (the cache-drop test checks that the
# clone's RLE cache is empty).
private alias PArrow = Parquet::Arrow
private alias RowSelector = Parquet::Arrow::RowSelector
private alias RowSelection = Parquet::Arrow::RowSelection
private alias MaskSelection = Parquet::Arrow::MaskSelection
private alias RowSelectionInner = Parquet::Arrow::RowSelectionInner

private def selectors(selection : RowSelection) : Array(RowSelector)
  selection.to_a
end

private def mask_of(bits : Array(Bool)) : ::Arrow::BooleanBuffer
  ::Arrow::BooleanBuffer.from_slice(bits)
end

private def mask_of(selection : RowSelection) : ::Arrow::BooleanBuffer
  selection.as_mask.not_nil!("expected a mask-backed selection")
end

private def mask_bits(mask : ::Arrow::BooleanBuffer) : Array(Bool)
  Array(Bool).new(mask.len) { |i| mask.value(i) }
end

private def rand_bits(rng : Random, len : Int32, probability : Float64) : Array(Bool)
  Array(Bool).new(len) { rng.rand < probability }
end

describe Parquet::Arrow::RowSelector do
  it "creates select and skip selectors" do
    RowSelector.select(5).row_count.should eq(5)
    RowSelector.select(5).skip?.should be_false
    RowSelector.skip(3).row_count.should eq(3)
    RowSelector.skip(3).skip?.should be_true
    RowSelector.select(5).should eq(RowSelector.select(5))
    RowSelector.select(5).should_not eq(RowSelector.skip(5))
  end
end

describe Parquet::Arrow::RowSelection do
  it "skips empty selectors and combines adjacent ones" do
    selection = RowSelection.new([
      RowSelector.select(0),
      RowSelector.skip(0),
      RowSelector.select(2),
      RowSelector.select(0),
      RowSelector.skip(1),
    ])
    selectors(selection).should eq([RowSelector.select(2), RowSelector.skip(1)])

    selection = RowSelection.new([
      RowSelector.skip(0),
      RowSelector.select(2),
      RowSelector.skip(0),
      RowSelector.select(2),
    ])
    selectors(selection).should eq([RowSelector.select(4)])

    selection = RowSelection.new([
      RowSelector.select(0),
      RowSelector.skip(2),
      RowSelector.select(0),
      RowSelector.skip(2),
    ])
    selectors(selection).should eq([RowSelector.skip(4)])
  end

  it "starts from and stays empty" do
    RowSelection.new([] of RowSelector).to_a.should be_empty
    RowSelection.empty.to_a.should be_empty
  end

  it "combines selectors like upstream from_iter" do
    a = [RowSelector.skip(3), RowSelector.skip(3), RowSelector.select(10), RowSelector.skip(4)]
    b = a + [RowSelector.skip(0)]
    c = [
      RowSelector.skip(2),
      RowSelector.skip(4),
      RowSelector.select(3),
      RowSelector.select(3),
      RowSelector.select(4),
      RowSelector.skip(3),
      RowSelector.skip(1),
      RowSelector.skip(0),
    ]
    expected = RowSelection.new([
      RowSelector.skip(6),
      RowSelector.select(10),
      RowSelector.skip(4),
    ])

    RowSelection.new(a).should eq(expected)
    RowSelection.new(b).should eq(expected)
    RowSelection.new(c).should eq(expected)

    selectors(RowSelection.new([RowSelector.select(10), RowSelector.select(5)]))
      .should eq([RowSelector.select(15)])
    selectors(RowSelection.new([RowSelector.select(10), RowSelector.skip(5)]))
      .should eq([RowSelector.select(10), RowSelector.skip(5)])
    selectors(RowSelection.new([RowSelector.skip(10), RowSelector.select(5)]))
      .should eq([RowSelector.skip(10), RowSelector.select(5)])
    selectors(RowSelection.new([RowSelector.skip(10), RowSelector.skip(5)]))
      .should eq([RowSelector.skip(15)])
  end

  it "builds selections from consecutive ranges" do
    selection = RowSelection.from_consecutive_ranges([1...3, 4...6, 6...6, 8...8, 9...10], 10)
    selectors(selection).should eq([
      RowSelector.skip(1),
      RowSelector.select(2),
      RowSelector.skip(1),
      RowSelector.select(2),
      RowSelector.skip(3),
      RowSelector.select(1),
    ])

    # Select everything.
    selection = RowSelection.from_consecutive_ranges([0...10], 10)
    selectors(selection).should eq([RowSelector.select(10)])

    # Out-of-order ranges are rejected.
    expect_raises(Parquet::ParquetError, /out of order/) do
      RowSelection.from_consecutive_ranges([1...3, 8...10, 4...7], 10)
    end
  end

  it "reports row counts" do
    selection = RowSelection.new([RowSelector.select(10), RowSelector.skip(20), RowSelector.select(5)])
    selection.row_count.should eq(15)
    selection.skipped_row_count.should eq(20)
    selection.total_row_count.should eq(35)
    selection.selects_any?.should be_true
    RowSelection.new([RowSelector.skip(4)]).selects_any?.should be_false
  end

  it "applies an offset" do
    selection = RowSelection.new([
      RowSelector.select(5),
      RowSelector.skip(23),
      RowSelector.select(7),
      RowSelector.skip(33),
      RowSelector.select(6),
    ])

    selection = selection.offset(2)
    selectors(selection).should eq([
      RowSelector.skip(2),
      RowSelector.select(3),
      RowSelector.skip(23),
      RowSelector.select(7),
      RowSelector.skip(33),
      RowSelector.select(6),
    ])

    selection = selection.offset(5)
    selectors(selection).should eq([
      RowSelector.skip(30),
      RowSelector.select(5),
      RowSelector.skip(33),
      RowSelector.select(6),
    ])

    selection = selection.offset(3)
    selectors(selection).should eq([
      RowSelector.skip(33),
      RowSelector.select(2),
      RowSelector.skip(33),
      RowSelector.select(6),
    ])

    selection = selection.offset(2)
    selectors(selection).should eq([RowSelector.skip(68), RowSelector.select(6)])

    selection = selection.offset(3)
    selectors(selection).should eq([RowSelector.skip(71), RowSelector.select(3)])
  end

  it "applies a limit" do
    # A limit equal to the selection is a no-op (trailing skips are dropped).
    selection = RowSelection.new([RowSelector.select(10), RowSelector.skip(90)])
    selectors(selection.limit(10)).should eq([RowSelector.select(10)])

    selection = RowSelection.new([
      RowSelector.select(10),
      RowSelector.skip(10),
      RowSelector.select(10),
      RowSelector.skip(10),
      RowSelector.select(10),
    ])

    selectors(selection.limit(5)).should eq([RowSelector.select(5)])
    selectors(selection.limit(15)).should eq([
      RowSelector.select(10),
      RowSelector.skip(10),
      RowSelector.select(5),
    ])
    selectors(selection.limit(0)).should be_empty
    selectors(selection.limit(30)).should eq([
      RowSelector.select(10),
      RowSelector.skip(10),
      RowSelector.select(10),
      RowSelector.skip(10),
      RowSelector.select(10),
    ])
    selectors(selection.limit(100)).should eq(selectors(selection))
  end

  it "splits off prefixes" do
    selection = RowSelection.new([
      RowSelector.skip(34),
      RowSelector.select(12),
      RowSelector.skip(3),
      RowSelector.select(35),
    ])

    split = selection.split_off(34)
    selectors(split).should eq([RowSelector.skip(34)])
    selectors(selection).should eq([
      RowSelector.select(12),
      RowSelector.skip(3),
      RowSelector.select(35),
    ])

    split = selection.split_off(5)
    selectors(split).should eq([RowSelector.select(5)])
    selectors(selection).should eq([
      RowSelector.select(7),
      RowSelector.skip(3),
      RowSelector.select(35),
    ])

    split = selection.split_off(8)
    selectors(split).should eq([RowSelector.select(7), RowSelector.skip(1)])
    selectors(selection).should eq([RowSelector.skip(2), RowSelector.select(35)])

    split = selection.split_off(200)
    selectors(split).should eq([RowSelector.skip(2), RowSelector.select(35)])
    selectors(selection).should be_empty
  end

  it "trims trailing skips" do
    selection = RowSelection.new([
      RowSelector.skip(34),
      RowSelector.select(12),
      RowSelector.skip(3),
      RowSelector.select(35),
    ])
    selectors(selection.trim).should eq([
      RowSelector.skip(34),
      RowSelector.select(12),
      RowSelector.skip(3),
      RowSelector.select(35),
    ])

    selection = RowSelection.new([
      RowSelector.skip(34),
      RowSelector.select(12),
      RowSelector.skip(3),
    ])
    selectors(selection.trim).should eq([RowSelector.skip(34), RowSelector.select(12)])
  end
end

# ---------------------------------------------------------------------------
# Bitmap backing (`selection/boolean.rs`)
# ---------------------------------------------------------------------------

private def interleaved_mask : ::Arrow::BooleanBuffer
  mask_of(Array(Bool).new(256) { |i| i % 3 == 0 })
end

describe "bitmap-backed RowSelection (selection/boolean.rs)" do
  it "iterates mask runs as selectors (upstream test_mask_iter_yields_borrowed_selectors)" do
    selection = RowSelection.from_boolean_buffer(mask_of([
      false, false, true, true, false, true, false, false,
    ]))

    selectors(selection).should eq([
      RowSelector.skip(2),
      RowSelector.select(2),
      RowSelector.skip(1),
      RowSelector.select(1),
      RowSelector.skip(2),
    ])
  end

  it "drops the selector cache on clone (upstream test_mask_iter_clone_drops_cache)" do
    selection = RowSelection.from_boolean_buffer(mask_of([
      false, false, true, true, false, true, false, false,
    ]))

    selectors(selection)
    selection.inner.as(RowSelectionInner::Mask).mask.selectors_cached?.should be_true

    cloned = selection.dup
    mask = cloned.inner.as(RowSelectionInner::Mask).mask
    mask.selectors_cached?.should be_false

    selectors(cloned).should eq([
      RowSelector.skip(2),
      RowSelector.select(2),
      RowSelector.skip(1),
      RowSelector.select(1),
      RowSelector.skip(2),
    ])
  end

  it "takes the selector cache when converting (upstream test_into_selectors_takes_the_iter_cache)" do
    selection = RowSelection.from_boolean_buffer(interleaved_mask)
    expected = selectors(selection)
    selection.inner.as(RowSelectionInner::Mask).mask.selectors_cached?.should be_true

    selection.into_selectors_vec.should eq(expected)
  end

  it "converts without a cache when none was populated (upstream test_into_selectors_without_cache_still_converts)" do
    selection = RowSelection.from_boolean_buffer(interleaved_mask)
    selection.inner.as(RowSelectionInner::Mask).mask.selectors_cached?.should be_false

    selection.into_selectors_vec.should eq(selectors(RowSelection.from_boolean_buffer(interleaved_mask)))
  end

  it "reuses the cache without populating it (upstream test_borrowed_selectors_reuses_cache_without_populating_it)" do
    selection = RowSelection.from_boolean_buffer(interleaved_mask)
    mask = selection.inner.as(RowSelectionInner::Mask).mask

    # Uncached: converts into a temporary, leaving the cache empty.
    mask.borrowed_selectors.should eq(PArrow::SelectionMasks.to_selectors(mask.mask))
    mask.selectors_cached?.should be_false

    expected = selectors(selection)
    mask = selection.inner.as(RowSelectionInner::Mask).mask
    mask.borrowed_selectors.same?(mask.selectors).should be_true
    mask.borrowed_selectors.should eq(expected)
  end

  it "agrees on set algebra whether or not the cache is populated (upstream test_set_algebra_agrees_whether_or_not_the_cache_is_populated)" do
    bits = Array(Bool).new(256) { |i| i % 3 == 0 }
    other = RowSelection.from_filters([
      ::Arrow::BooleanArray.from_values(Array(Bool).new(256) { |i| i % 5 != 0 }),
    ])

    cold = RowSelection.from_boolean_buffer(mask_of(bits))
    warm = RowSelection.from_boolean_buffer(mask_of(bits))
    selectors(warm)

    cold.intersection(other).should eq(warm.intersection(other))
    other.intersection(cold).should eq(other.intersection(warm))
    cold.union(other).should eq(warm.union(other))
    other.union(cold).should eq(other.union(warm))
  end

  it "streams mask runs without the cache (upstream test_mask_run_iter_streams_without_cache)" do
    selection = RowSelection.from_boolean_buffer(mask_of([
      false, false, true, true, false, true, false, false,
    ]))
    iter = PArrow::MaskRunIter.new(mask_of(selection))

    iter.next.should eq(RowSelector.skip(2))
    iter.next.should eq(RowSelector.select(2))
    iter.next.should eq(RowSelector.skip(1))
    iter.next.should eq(RowSelector.select(1))
    iter.next.should eq(RowSelector.skip(2))
    iter.next.should eq(Iterator::Stop::INSTANCE)
    iter.next.should eq(Iterator::Stop::INSTANCE)

    selection = RowSelection.from_boolean_buffer(mask_of([true, true, false]))
    iter = PArrow::MaskRunIter.new(mask_of(selection))
    iter.next.should eq(RowSelector.select(2))
    iter.next.should eq(RowSelector.skip(1))
    iter.next.should eq(Iterator::Stop::INSTANCE)
  end

  it "builds from a boolean buffer (upstream test_from_boolean_buffer)" do
    bits = [
      false, false, true, true, false, true, false, false,
      true, false, false, false, false, false, false, true,
    ]
    selection = RowSelection.from_boolean_buffer(mask_of(bits))

    selection.as_mask.should_not be_nil
    selection.row_count.should eq(5)
    selection.skipped_row_count.should eq(11)
    selection.selects_any?.should be_true

    from_filters = RowSelection.from_filters([::Arrow::BooleanArray.from_values(bits)])
    selection.should eq(from_filters)

    bits_tail = [true, false, true, false, false, false]
    trimmed = RowSelection.from_boolean_buffer(mask_of(bits_tail)).trim
    trimmed.as_mask.should_not be_nil
    mask_of(trimmed).len.should eq(3)
  end

  it "handles an empty bitmap (upstream test_from_boolean_buffer_empty)" do
    empty = RowSelection.from_boolean_buffer(mask_of([] of Bool))
    empty.as_mask.should_not be_nil
    empty.row_count.should eq(0)
    empty.skipped_row_count.should eq(0)
    empty.selects_any?.should be_false
    selectors(empty).should be_empty
  end

  it "does not select when every bit is unset (upstream test_from_boolean_buffer_all_unset_does_not_select)" do
    all_zero = RowSelection.from_boolean_buffer(::Arrow::BooleanBuffer.new_unset(1024))
    all_zero.as_mask.should_not be_nil
    all_zero.selects_any?.should be_false
    all_zero.row_count.should eq(0)
    all_zero.skipped_row_count.should eq(1024)
  end

  it "converts a bitmap through RowSelection.from (upstream test_from_boolean_buffer_via_from_impl)" do
    buffer = mask_of([true, false, true, true])
    a = RowSelection.from(buffer)
    b = RowSelection.from_boolean_buffer(buffer)
    a.should eq(b)
    a.as_mask.should_not be_nil
  end

  it "preserves the backing on clone (upstream test_mask_backing_clone_preserves_backing)" do
    original = RowSelection.from_boolean_buffer(mask_of([true, false, true]))
    cloned = original.dup
    cloned.as_mask.should_not be_nil
    original.should eq(cloned)
  end

  it "keeps mask and selector transforms equivalent (upstream test_mask_backing_mutation_equivalence)" do
    bits = [true, true, false, false, true, false, true, true]

    from_mask = RowSelection.from_boolean_buffer(mask_of(bits))
    mask_split = from_mask.split_off(3)
    mask_tail = from_mask

    from_selectors = RowSelection.from_filters([::Arrow::BooleanArray.from_values(bits)])
    selector_split = from_selectors.split_off(3)
    selector_tail = from_selectors

    mask_split.should eq(selector_split)
    mask_tail.should eq(selector_tail)
    mask_split.as_mask.should_not be_nil
    mask_tail.as_mask.should_not be_nil

    limited_mask = RowSelection.from_boolean_buffer(mask_of(bits)).limit(3)
    limited_selectors = RowSelection.from_filters([::Arrow::BooleanArray.from_values(bits)]).limit(3)
    limited_mask.as_mask.should_not be_nil
    limited_mask.should eq(limited_selectors)

    offset_mask = RowSelection.from_boolean_buffer(mask_of(bits)).offset(2)
    offset_selectors = RowSelection.from_filters([::Arrow::BooleanArray.from_values(bits)]).offset(2)
    offset_mask.as_mask.should_not be_nil
    offset_mask.should eq(offset_selectors)
  end

  it "keeps fuzzed mask and selector selections equivalent (upstream test_mask_backing_fuzz_equivalence)" do
    rng = Random.new(42)
    100.times do
      len = rng.rand(200)
      bits = rand_bits(rng, len, 0.35)

      from_mask = RowSelection.from_boolean_buffer(mask_of(bits))
      from_filters = RowSelection.from_filters([::Arrow::BooleanArray.from_values(bits)])

      from_mask.should eq(from_filters)
      from_mask.row_count.should eq(from_filters.row_count)
      from_mask.skipped_row_count.should eq(from_filters.skipped_row_count)
      from_mask.selects_any?.should eq(from_filters.selects_any?)

      inner_len = bits.count(true)
      inner_bits = rand_bits(rng, inner_len, 0.7)
      inner = RowSelection.from_filters([::Arrow::BooleanArray.from_values(inner_bits)])
      inner_mask = RowSelection.from_boolean_buffer(mask_of(inner_bits))
      and_then_mask = from_mask.and_then(inner)
      and_then_both_masks = from_mask.and_then(inner_mask)
      and_then_mask.as_mask.should_not be_nil
      and_then_both_masks.as_mask.should_not be_nil
      and_then_mask.should eq(from_filters.and_then(inner))
      and_then_both_masks.should eq(and_then_mask)
    end
  end

  it "keeps an empty mask backing when offsetting past the end (upstream test_mask_offset_past_end_preserves_empty_mask_backing)" do
    selection = RowSelection.from_boolean_buffer(mask_of([true, false, true])).offset(2)
    selection.as_mask.should_not be_nil
    mask_of(selection).len.should eq(0)
    selection.row_count.should eq(0)
    selection.skipped_row_count.should eq(0)
  end

  it "limits at the nth selected row (upstream test_mask_limit_truncates_at_nth_selected_row)" do
    selection = RowSelection.from_boolean_buffer(mask_of([
      false, true, false, true, false, true, false,
    ])).limit(2)

    mask = mask_of(selection)
    mask.len.should eq(4)
    mask_bits(mask).should eq([false, true, false, true])
  end

  it "preserves the backing when splitting (upstream test_mask_split_off_preserves_backing)" do
    bits = Array(Bool).new(40) { |i| i % 3 == 0 }
    selection = RowSelection.from_boolean_buffer(mask_of(bits))
    head = selection.split_off(15)

    head.as_mask.should_not be_nil
    selection.as_mask.should_not be_nil

    head_selectors = RowSelection.from_filters([::Arrow::BooleanArray.from_values(bits[0, 15])])
    tail_selectors = RowSelection.from_filters([::Arrow::BooleanArray.from_values(bits[15, 25])])
    head.should eq(head_selectors)
    selection.should eq(tail_selectors)
  end

  it "returns the whole mask when splitting past the end (upstream test_mask_split_off_past_end_returns_whole)" do
    bits = [true, false, true]
    selection = RowSelection.from_boolean_buffer(mask_of(bits))
    head = selection.split_off(100)

    head.as_mask.should_not be_nil
    mask_of(head).len.should eq(3)
    selection.as_mask.should_not be_nil
    mask_of(selection).len.should eq(0)
    selection.row_count.should eq(0)
    selection.skipped_row_count.should eq(0)
  end

  it "returns an empty selection when the offset exceeds the selected rows (upstream test_mask_offset_exceeds_selected_returns_empty)" do
    selection = RowSelection.from_boolean_buffer(mask_of([true, true, false, true]))
    result = selection.offset(10)
    result.row_count.should eq(0)
    result.skipped_row_count.should eq(0)

    from_selectors = RowSelection.from_filters([
      ::Arrow::BooleanArray.from_values([true, true, false, true]),
    ]).offset(10)
    result.should eq(from_selectors)
  end

  it "returns every selected row when the limit exceeds them (upstream test_mask_limit_exceeds_selected_returns_all)" do
    bits = [true, true, false, true]
    selection = RowSelection.from_boolean_buffer(mask_of(bits))
    result = selection.limit(10)
    result.row_count.should eq(3)

    from_selectors = RowSelection.from_filters([::Arrow::BooleanArray.from_values(bits)]).limit(10)
    result.should eq(from_selectors)
  end

  it "collapses an all-unset mask when trimmed (upstream test_mask_trim_all_zero_collapses_to_empty)" do
    selection = RowSelection.from_boolean_buffer(::Arrow::BooleanBuffer.new_unset(128))
    trimmed = selection.trim
    trimmed.as_mask.should_not be_nil
    mask_of(trimmed).len.should eq(0)
  end

  it "builds bitmaps from selectors (upstream test_boolean_mask_from_selectors_fuzz_equivalence)" do
    rng = Random.new(7)
    200.times do
      n_selectors = rng.rand(30)
      runs = Array(RowSelector).new(n_selectors) do
        RowSelector.new(rng.rand(40), rng.rand(2) == 0)
      end

      expected = ::Arrow::BooleanBufferBuilder.new(runs.sum(0, &.row_count)).tap do |builder|
        runs.each { |selector| builder.append_n(selector.row_count, !selector.skip?) }
      end.finish

      PArrow::SelectionMasks.from_selectors(runs).should eq(expected)
    end
  end

  it "reports whether a mask contains a run count (upstream test_mask_has_at_least_runs)" do
    assert_run_count = ->(bits : Array(Bool), expected_runs : Int32) do
      mask = mask_of(bits)
      (0..(expected_runs + 2)).each do |min_runs|
        PArrow::SelectionMasks.has_at_least_runs?(mask, min_runs).should eq(expected_runs >= min_runs)
      end
    end

    assert_run_count.call([] of Bool, 0)
    assert_run_count.call(Array(Bool).new(8, false), 1)
    assert_run_count.call(Array(Bool).new(8, true), 1)
    assert_run_count.call([false, false, true, true, false], 3)
    assert_run_count.call([true, false, true, false, true, false], 6)

    # Exercise the unaligned iterator path, as mask-backed selections can be
    # slices.
    mask = mask_of([true, false, false, true, true, false, true, true]).slice(1, 6)
    (0..6).each do |min_runs|
      PArrow::SelectionMasks.has_at_least_runs?(mask, min_runs).should eq(4 >= min_runs)
    end
  end

  it "trims masks equivalently for every offset and length (upstream test_trim_mask_fuzz_equivalence)" do
    rng = Random.new(11)
    200.times do
      len = rng.rand(200)
      bits = rand_bits(rng, len, 0.3)
      full = mask_of(bits)
      start = rng.rand(len + 1)
      slice_len = rng.rand(len - start + 1)
      mask = full.slice(start, slice_len)

      expected_len = 0
      (slice_len - 1).downto(0) do |pos|
        if bits[start + pos]
          expected_len = pos + 1
          break
        end
      end

      case trimmed = PArrow::SelectionMasks.trim(mask)
      when ::Arrow::BooleanBuffer
        expected_len.should_not eq(mask.len)
        trimmed.len.should eq(expected_len)
        trimmed.should eq(mask.slice(0, expected_len))
      else
        expected_len.should eq(mask.len)
      end
    end
  end

  it "propagates the cached count through split_off (upstream test_split_off_propagates_cached_count)" do
    bits = [true, false, true, true, false, false, true, false]
    selection = RowSelection.from_boolean_buffer(mask_of(bits))
    selection.row_count.should eq(4)

    head = selection.split_off(3)
    head.row_count.should eq(2)
    selection.row_count.should eq(2)

    tail_fresh = RowSelection.from_boolean_buffer(mask_of([
      true, false, false, true, false,
    ]))
    selection.should eq(tail_fresh)

    head = selection.split_off(100)
    head.row_count.should eq(2)
    selection.row_count.should eq(0)
  end

  it "preserves the cached count through trim/offset/limit (upstream test_trim_and_offset_and_limit_preserve_cached_count)" do
    bits = [true, true, false, true, false, false]
    selection = RowSelection.from_boolean_buffer(mask_of(bits))
    selection.row_count.should eq(3)

    trimmed = selection.trim
    trimmed.as_mask.should_not be_nil
    mask_of(trimmed).len.should eq(4)
    trimmed.row_count.should eq(3)

    offset = trimmed.dup.offset(1)
    offset.row_count.should eq(2)

    limited = trimmed.limit(2)
    limited.row_count.should eq(2)
  end
end

# ---------------------------------------------------------------------------
# Mixed-backing parts of `selection/mod.rs`
# ---------------------------------------------------------------------------

describe "mixed-backing RowSelection (selection/mod.rs)" do
  it "counts rows across backings (upstream test_total_row_count)" do
    selection = RowSelection.new([RowSelector.skip(5), RowSelector.select(3)])
    selection.total_row_count.should eq(8)
    selection.row_count.should eq(3)
    selection.skipped_row_count.should eq(5)

    selection = RowSelection.from_boolean_buffer(mask_of([true, false, true]))
    selection.total_row_count.should eq(3)
    selection.row_count.should eq(2)
    selection.skipped_row_count.should eq(1)

    RowSelection.new([] of RowSelector).total_row_count.should eq(0)
  end

  it "builds from boolean array filters (upstream test_from_filters)" do
    filters = [
      ::Arrow::BooleanArray.from_values([false, false, false, true, true, true, true]),
      ::Arrow::BooleanArray.from_values([true, true, false, false, true, true, true]),
      ::Arrow::BooleanArray.from_values([false, false, false, false]),
      ::Arrow::BooleanArray.from_values([] of Bool),
    ]

    selection = RowSelection.from_filters(filters[0, 1])
    selection.selects_any?.should be_true
    selectors(selection).should eq([RowSelector.skip(3), RowSelector.select(4)])

    selection = RowSelection.from_filters(filters[0, 2])
    selection.selects_any?.should be_true
    selectors(selection).should eq([
      RowSelector.skip(3),
      RowSelector.select(6),
      RowSelector.skip(2),
      RowSelector.select(3),
    ])

    selection = RowSelection.from_filters(filters)
    selection.selects_any?.should be_true
    selectors(selection).should eq([
      RowSelector.skip(3),
      RowSelector.select(6),
      RowSelector.skip(2),
      RowSelector.select(3),
      RowSelector.skip(4),
    ])

    selection = RowSelection.from_filters(filters[2, 1])
    selection.selects_any?.should be_false
    selectors(selection).should eq([RowSelector.skip(4)])
  end

  it "rejects filters containing nulls" do
    filter = ::Arrow::BooleanArray.new(::Arrow::BooleanBuffer.from_slice([true, false]), ::Arrow::NullBuffer.new_unchecked(::Arrow::BooleanBuffer.from_slice([true, false]), 1))

    expect_raises(Parquet::GeneralError, /filter contains nulls/) do
      RowSelection.from_filters([filter])
    end
  end

  it "compares selections across backings (upstream test_mixed_backing_equality_mismatches)" do
    mask = RowSelection.from_boolean_buffer(mask_of([true, false, true, true]))

    # Total row counts differ
    longer = RowSelection.new([
      RowSelector.select(1),
      RowSelector.skip(1),
      RowSelector.select(2),
      RowSelector.skip(1),
    ])
    mask.should_not eq(longer)
    longer.should_not eq(mask)

    # A selected bit falls inside a skip run
    skip_overlap = RowSelection.new([RowSelector.skip(2), RowSelector.select(2)])
    mask.should_not eq(skip_overlap)

    # Select run boundaries do not line up
    misaligned = RowSelection.new([
      RowSelector.select(2),
      RowSelector.skip(1),
      RowSelector.select(1),
    ])
    mask.should_not eq(misaligned)

    equal = RowSelection.new([
      RowSelector.select(1),
      RowSelector.skip(1),
      RowSelector.select(2),
    ])
    mask.should eq(equal)
    equal.should eq(mask)
  end

  it "concatenates mask-backed selections into a mask (upstream test_from_iter_all_mask_preserves_mask_backing)" do
    a_bits = [true, false, true, true]
    b_bits = [false, true, false]
    c_bits = [true, true, false, false, true]

    collected = RowSelection.concat([
      RowSelection.from_boolean_buffer(mask_of(a_bits)),
      RowSelection.from_boolean_buffer(mask_of(b_bits)),
      RowSelection.from_boolean_buffer(mask_of(c_bits)),
    ])

    combined = a_bits + b_bits + c_bits
    expected = RowSelection.from_filters([::Arrow::BooleanArray.from_values(combined)])

    collected.as_mask.should_not be_nil
    collected.should eq(expected)
  end

  it "falls back to selectors when concatenating mixed backings (upstream test_from_iter_mixed_backing_falls_back_to_selectors)" do
    a_bits = [true, false, true]
    b_selectors = [RowSelector.skip(2), RowSelector.select(3)]
    c_bits = [false, true]

    collected = RowSelection.concat([
      RowSelection.from_boolean_buffer(mask_of(a_bits)),
      RowSelection.new(b_selectors),
      RowSelection.from_boolean_buffer(mask_of(c_bits)),
    ])

    collected.as_mask.should be_nil

    combined_bits = [true, false, true, false, false, true, true, true, false, true]
    expected = RowSelection.from_filters([::Arrow::BooleanArray.from_values(combined_bits)])
    collected.should eq(expected)
  end

  it "concatenates nothing into an empty mask (upstream test_from_iter_empty_yields_empty_selection)" do
    collected = RowSelection.concat([] of RowSelection)
    collected.should eq(RowSelection.empty)
    collected.as_mask.should_not be_nil
    mask_of(collected).len.should eq(0)
  end

  it "treats a zero offset and zero batch expand as identities (upstream test_offset_zero_and_zero_batch_expand_are_identity)" do
    selection = RowSelection.from_boolean_buffer(mask_of([true, false, true]))
    selection.dup.offset(0).should eq(selection)
  end
end

# ---------------------------------------------------------------------------
# Set algebra (`selection/algebra.rs`)
# ---------------------------------------------------------------------------

describe "RowSelection set algebra (selection/algebra.rs)" do
  it "applies and_then (upstream test_and)" do
    a = RowSelection.new([
      RowSelector.skip(12),
      RowSelector.select(23),
      RowSelector.skip(3),
      RowSelector.select(5),
    ])

    b = RowSelection.new([
      RowSelector.select(5),
      RowSelector.skip(4),
      RowSelector.select(15),
      RowSelector.skip(4),
    ])

    expected = RowSelection.new([
      RowSelector.skip(12),
      RowSelector.select(5),
      RowSelector.skip(4),
      RowSelector.select(14),
      RowSelector.skip(3),
      RowSelector.select(1),
      RowSelector.skip(4),
    ])

    a.and_then(b).should eq(expected)

    a.split_off(7)
    expected.split_off(7)
    a.and_then(b).should eq(expected)

    a = RowSelection.new([RowSelector.select(5), RowSelector.skip(3)])
    b = RowSelection.new([
      RowSelector.select(2),
      RowSelector.skip(1),
      RowSelector.select(1),
      RowSelector.skip(1),
    ])

    selectors(a.and_then(b)).should eq([
      RowSelector.select(2),
      RowSelector.skip(1),
      RowSelector.select(1),
      RowSelector.skip(4),
    ])
  end

  it "rejects a longer inner selection (upstream test_and_longer)" do
    a = RowSelection.new([
      RowSelector.select(3),
      RowSelector.skip(33),
      RowSelector.select(3),
      RowSelector.skip(33),
    ])
    b = RowSelection.new([RowSelector.select(36)])

    expect_raises(Parquet::GeneralError, /selection exceeds the number of selected rows/) do
      a.and_then(b)
    end
  end

  it "rejects a shorter inner selection (upstream test_and_shorter)" do
    a = RowSelection.new([
      RowSelector.select(3),
      RowSelector.skip(33),
      RowSelector.select(3),
      RowSelector.skip(33),
    ])
    b = RowSelection.new([RowSelector.select(3)])

    expect_raises(Parquet::GeneralError, /selection contains less than the number of selected rows/) do
      a.and_then(b)
    end
  end

  it "intersects and combines row selections (upstream test_intersect_row_selection_and_combine)" do
    a = [RowSelector.select(5), RowSelector.skip(4), RowSelector.select(1)]
    b = [RowSelector.select(8), RowSelector.skip(1), RowSelector.select(1)]

    PArrow::RowSelectionAlgebra.intersect_row_selections(a, b).should eq(
      RowSelection.new([RowSelector.select(5), RowSelector.skip(4), RowSelector.select(1)])
    )

    a = [
      RowSelector.select(3),
      RowSelector.skip(33),
      RowSelector.select(3),
      RowSelector.skip(33),
    ]
    b = [RowSelector.select(36), RowSelector.skip(36)]
    PArrow::RowSelectionAlgebra.intersect_row_selections(a, b).should eq(
      RowSelection.new([RowSelector.select(3), RowSelector.skip(69)])
    )

    a = [RowSelector.select(3), RowSelector.skip(7)]
    b = [
      RowSelector.select(2),
      RowSelector.skip(2),
      RowSelector.select(2),
      RowSelector.skip(2),
      RowSelector.select(2),
    ]
    PArrow::RowSelectionAlgebra.intersect_row_selections(a, b).should eq(
      RowSelection.new([RowSelector.select(2), RowSelector.skip(8)])
    )
  end

  it "applies and_then over fuzzed filter pairs (upstream test_and_fuzz)" do
    rng = Random.new(3)
    100.times do
      a_len = rng.rand(10..100)
      a_bools = rand_bits(rng, a_len, 0.2)
      a = RowSelection.from_filters([::Arrow::BooleanArray.from_values(a_bools)])

      b_len = a_bools.count(true)
      b_bools = rand_bits(rng, b_len, 0.8)
      b = RowSelection.from_filters([::Arrow::BooleanArray.from_values(b_bools)])

      expected_bools = Array(Bool).new(a_len, false)
      b_idx = 0
      a_bools.each_with_index do |a_bit, idx|
        next unless a_bit
        expected_bools[idx] = true if b_bools[b_idx]
        b_idx += 1
      end

      expected = RowSelection.from_filters([::Arrow::BooleanArray.from_values(expected_bools)])

      a_len.should eq(expected.total_row_count)
      a.and_then(b).should eq(expected)
    end
  end

  it "intersects row selections (upstream test_intersection)" do
    selection = RowSelection.new([RowSelector.select(1048576)])
    selection.intersection(selection).should eq(selection)

    a = RowSelection.new([
      RowSelector.skip(10),
      RowSelector.select(10),
      RowSelector.skip(10),
      RowSelector.select(20),
    ])

    b = RowSelection.new([
      RowSelector.skip(20),
      RowSelector.select(20),
      RowSelector.skip(10),
    ])

    selectors(a.intersection(b)).should eq([
      RowSelector.skip(30),
      RowSelector.select(10),
      RowSelector.skip(10),
    ])
  end

  it "unions row selections (upstream test_union)" do
    selection = RowSelection.new([RowSelector.select(1048576)])
    selection.union(selection).should eq(selection)

    # NYNYY
    a = RowSelection.new([
      RowSelector.skip(10),
      RowSelector.select(10),
      RowSelector.skip(10),
      RowSelector.select(20),
    ])

    # NNYYNYN
    b = RowSelection.new([
      RowSelector.skip(20),
      RowSelector.select(20),
      RowSelector.skip(10),
      RowSelector.select(10),
      RowSelector.skip(10),
    ])

    # NYYYYYN
    selectors(a.union(b)).should eq([
      RowSelector.skip(10),
      RowSelector.select(50),
      RowSelector.skip(10),
    ])
  end

  it "preserves the mask backing when and_then-ing filters (upstream test_mask_and_then_preserves_backing)" do
    outer_bits = [false, true, true, false, true, false, true]
    inner_bits = [true, false, true, false]
    outer_mask = RowSelection.from_boolean_buffer(mask_of(outer_bits))
    inner = RowSelection.from_filters([::Arrow::BooleanArray.from_values(inner_bits)])

    result = outer_mask.and_then(inner)
    result.as_mask.should_not be_nil

    outer_selectors = RowSelection.from_filters([::Arrow::BooleanArray.from_values(outer_bits)])
    result.should eq(outer_selectors.and_then(inner))
    mask_bits(mask_of(result)).should eq([false, true, false, false, true, false, false])
  end

  it "preserves the mask backing when and_then-ing masks (upstream test_mask_and_then_mask_preserves_backing)" do
    outer_bits = [false, true, true, false, true, false, true, true]
    inner_bits = [false, true, false, true, false]
    outer_mask = RowSelection.from_boolean_buffer(mask_of(outer_bits))
    inner_mask = RowSelection.from_boolean_buffer(mask_of(inner_bits))

    result = outer_mask.and_then(inner_mask)
    result.as_mask.should_not be_nil

    outer_selectors = RowSelection.from_filters([::Arrow::BooleanArray.from_values(outer_bits)])
    inner_selectors = RowSelection.from_filters([::Arrow::BooleanArray.from_values([
      false, true, false, true, false,
    ])])
    result.should eq(outer_selectors.and_then(inner_selectors))
    mask_bits(mask_of(result)).should eq([false, false, true, false, false, false, true, false])
  end

  it "keeps the selector backing when and_then-ing a mask (upstream test_selector_and_then_mask)" do
    outer = RowSelection.from_filters([
      ::Arrow::BooleanArray.from_values([false, true, true, false, true]),
    ])
    inner = RowSelection.from_boolean_buffer(mask_of([true, false, true]))

    result = outer.and_then(inner)
    result.as_mask.should be_nil
    result.should eq(
      RowSelection.from_filters([
        ::Arrow::BooleanArray.from_values([false, true, false, false, true]),
      ])
    )
  end

  it "returns an all-unset mask when nothing is selected (upstream test_mask_and_then_none_selected_returns_all_unset)" do
    outer = RowSelection.from_boolean_buffer(mask_of([
      false, true, true, false, true,
    ]))
    inner = RowSelection.from_boolean_buffer(mask_of([false, false, false]))

    mask = mask_of(outer.and_then(inner))
    mask.len.should eq(5)
    mask.count_set_bits.should eq(0)
  end

  it "intersects masks bitwise (upstream test_mask_intersection_uses_bitwise)" do
    a_bits = [true, true, false, true, false, true]
    b_bits = [true, false, true, true, true, false]
    a = RowSelection.from_boolean_buffer(mask_of(a_bits))
    b = RowSelection.from_boolean_buffer(mask_of(b_bits))

    result = a.intersection(b)
    result.as_mask.should_not be_nil

    expected = a_bits.zip(b_bits).map { |(x, y)| x && y }
    result.should eq(RowSelection.from_filters([::Arrow::BooleanArray.from_values(expected)]))
  end

  it "unions masks bitwise (upstream test_mask_union_uses_bitwise)" do
    a_bits = [true, false, false, true, false, false]
    b_bits = [false, true, false, false, true, false]
    a = RowSelection.from_boolean_buffer(mask_of(a_bits))
    b = RowSelection.from_boolean_buffer(mask_of(b_bits))

    result = a.union(b)
    result.as_mask.should_not be_nil

    expected = a_bits.zip(b_bits).map { |(x, y)| x || y }
    result.should eq(RowSelection.from_filters([::Arrow::BooleanArray.from_values(expected)]))
  end

  it "intersects and unions mixed backings (upstream test_mixed_mask_selector_intersection_and_union)" do
    mask_bits = [true, false, true, false, true, false]
    selector_bits = [false, true, true, false, false, true]
    mask = RowSelection.from_boolean_buffer(mask_of(mask_bits))
    selectors_selection = RowSelection.from_filters([
      ::Arrow::BooleanArray.from_values(selector_bits),
    ])

    intersection_bits = mask_bits.zip(selector_bits).map { |(x, y)| x && y }
    expected_intersection = RowSelection.from_filters([
      ::Arrow::BooleanArray.from_values(intersection_bits),
    ])
    mask.intersection(selectors_selection).should eq(expected_intersection)
    selectors_selection.intersection(mask).should eq(expected_intersection)

    union_bits = mask_bits.zip(selector_bits).map { |(x, y)| x || y }
    expected_union = RowSelection.from_filters([::Arrow::BooleanArray.from_values(union_bits)])
    mask.union(selectors_selection).should eq(expected_union)
    selectors_selection.union(mask).should eq(expected_union)
  end

  it "passes the longer mask's tail through (upstream test_mask_intersection_uneven_passes_tail_through)" do
    a = RowSelection.from_boolean_buffer(mask_of([true, true, true, true, true]))
    b = RowSelection.from_boolean_buffer(mask_of([true, false, true]))

    mask = mask_of(a.intersection(b))
    mask.len.should eq(5)
    mask_bits(mask).should eq([true, false, true, true, true])

    # Swapped operands: the right side is longer and its tail passes through.
    a = RowSelection.from_boolean_buffer(mask_of([true, false, true]))
    b = RowSelection.from_boolean_buffer(mask_of([true, true, true, false, true]))

    mask = mask_of(a.intersection(b))
    mask.len.should eq(5)
    mask_bits(mask).should eq([true, false, true, false, true])
  end

  it "passes the longer mask's tail through when unioning (upstream test_mask_union_uneven_passes_tail_through)" do
    a = RowSelection.from_boolean_buffer(mask_of([true, false, true]))
    b = RowSelection.from_boolean_buffer(mask_of([false, true, false, true, false]))

    mask = mask_of(a.union(b))
    mask.len.should eq(5)
    mask_bits(mask).should eq([true, true, true, true, false])

    a = RowSelection.from_boolean_buffer(mask_of([false, true, false, false, true]))
    b = RowSelection.from_boolean_buffer(mask_of([true, false, false]))

    mask = mask_of(a.union(b))
    mask_bits(mask).should eq([true, true, false, false, true])
  end

  it "intersects and unions selector runs like a bitmask (characterization)" do
    # Upstream's selector-based intersection/union also combine the common
    # prefix and pass the longer side's tail through, so the mask oracle applies
    # to both backings.
    rng = Random.new(13)
    100.times do
      a = random_selector_selection(rng)
      b = random_selector_selection(rng)
      a_bits = bits_of(a)
      b_bits = bits_of(b)

      intersection = expected_combined(a_bits, b_bits) { |x, y| x && y }
      union = expected_combined(a_bits, b_bits) { |x, y| x || y }

      bits_of(a.intersection(b)).should eq(intersection)
      bits_of(a.union(b)).should eq(union)
    end
  end

  it "combines masks across bit offsets and lengths (upstream test_mask_algebra_with_offsets)" do
    base_bits = Array(Bool).new(600) { |i| i % 7 == 0 || i % 3 == 1 }
    other_bits = Array(Bool).new(600) { |i| i % 5 == 2 || i % 11 == 4 }
    base = mask_of(base_bits)
    other = mask_of(other_bits)

    {0, 1, 5, 8, 13, 64, 67}.each do |l_offset|
      {0, 1, 3, 8, 60, 64, 70}.each do |r_offset|
        {
          {0, 9}, {9, 0}, {1, 200}, {200, 1}, {63, 130}, {321, 65},
          {0, 0}, {1, 1}, {63, 63}, {64, 64}, {200, 200}, {321, 321},
        }.each do |(l_len, r_len)|
          l = base.slice(l_offset, l_len)
          r = other.slice(r_offset, r_len)
          l_bits = mask_bits(l)
          r_bits = mask_bits(r)

          expected_intersection = expected_combined(l_bits, r_bits) { |x, y| x && y }
          expected_union = expected_combined(l_bits, r_bits) { |x, y| x || y }

          PArrow::RowSelectionAlgebra.intersection(
            RowSelection.from_boolean_buffer(l),
            RowSelection.from_boolean_buffer(r),
          ).should eq(RowSelection.from_filters([
            ::Arrow::BooleanArray.from_values(expected_intersection),
          ]))

          PArrow::RowSelectionAlgebra.union(
            RowSelection.from_boolean_buffer(l),
            RowSelection.from_boolean_buffer(r),
          ).should eq(RowSelection.from_filters([
            ::Arrow::BooleanArray.from_values(expected_union),
          ]))
        end
      end
    end
  end

  it "combines fuzzed masks (upstream test_mask_algebra_fuzz)" do
    rng = Random.new(5)
    200.times do
      l_offset = rng.rand(70)
      r_offset = rng.rand(70)
      l_len = rng.rand(300)
      # Bias towards equal lengths so that path is hit often
      r_len = rng.rand < 0.25 ? l_len : rng.rand(300)

      l_bits = rand_bits(rng, l_offset + l_len, 0.5)
      r_bits = rand_bits(rng, r_offset + r_len, 0.5)
      l = mask_of(l_bits).slice(l_offset, l_len)
      r = mask_of(r_bits).slice(r_offset, r_len)
      l_bits = mask_bits(l)
      r_bits = mask_bits(r)

      expected_intersection = expected_combined(l_bits, r_bits) { |x, y| x && y }
      expected_union = expected_combined(l_bits, r_bits) { |x, y| x || y }

      PArrow::RowSelectionAlgebra.intersection(
        RowSelection.from_boolean_buffer(l),
        RowSelection.from_boolean_buffer(r),
      ).should eq(RowSelection.from_filters([
        ::Arrow::BooleanArray.from_values(expected_intersection),
      ]))

      PArrow::RowSelectionAlgebra.union(
        RowSelection.from_boolean_buffer(l),
        RowSelection.from_boolean_buffer(r),
      ).should eq(RowSelection.from_filters([
        ::Arrow::BooleanArray.from_values(expected_union),
      ]))
    end
  end
end

private def random_selector_selection(rng : Random) : RowSelection
  runs = rng.rand(6)
  RowSelection.new(Array(RowSelector).new(runs) do
    RowSelector.new(rng.rand(1..5), rng.rand(2) == 0)
  end)
end

private def bits_of(selection : RowSelection) : Array(Bool)
  mask_bits(PArrow::SelectionMasks.from_selectors(selection.to_a))
end

# Expected result of combining two masks of possibly differing lengths: the
# block over the common prefix, then the longer side's tail unchanged.
private def expected_combined(l : Array(Bool), r : Array(Bool), & : Bool, Bool -> Bool) : Array(Bool)
  common = Math.min(l.size, r.size)
  longer = l.size > r.size ? l : r
  result = Array(Bool).new(common) { |i| yield l[i], r[i] }
  result.concat(longer[common, longer.size - common])
  result
end
