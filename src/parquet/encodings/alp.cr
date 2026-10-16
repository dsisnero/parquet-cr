require "../util/bit_util"

# Ported from `parquet/src/encodings/alp.rs`,
# `parquet/src/encodings/encoding/alp_encoder.rs`, and
# `parquet/src/encodings/decoding/alp_decoder.rs`
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Ports the ALP (Adaptive Lossless floating-Point) codec for f32/f64.
#
# Divergence: Rust's `AlpFloat`/`AlpExact` traits and associated types are
# replaced by `F.class`-dispatched module methods on `Alp`; the exact integer
# types (`u32`/`u64`) are carried uniformly as `UInt64` with width-masked
# wrapping arithmetic (`exact_wrapping_add`/`_sub`) so f32 wraps at 32 bits;
# `AlpHeader::serialize` uses a `Bytes` return instead of `[u8; 7]`;
# `Scratch`/`StreamingPage` use element counts (no `Array` capacity) for
# `estimated_memory_size`; `RleValueDecoder`-style scratch decoding is not
# needed because `BitReader#get_batch` accepts both `Array` and `Slice`.
module Parquet
  module Encodings
    # ALP constants, metadata/float/exact helpers, and the encode/decode
    # algorithm (upstream `encodings/alp.rs` + the free functions of the
    # encoder/decoder modules).
    module Alp
      ALP_HEADER_SIZE                   =     7
      ALP_COMPRESSION_MODE              =  0_u8
      ALP_INTEGER_ENCODING_FOR_BIT_PACK =  0_u8
      ALP_MIN_LOG_VECTOR_SIZE           =  3_u8
      ALP_MAX_LOG_VECTOR_SIZE           = 15_u8
      ALP_DEFAULT_LOG_VECTOR_SIZE       = 10_u8
      ALP_MAX_EXPONENT_F32              = 10_u8
      ALP_MAX_EXPONENT_F64              = 18_u8

      # Vectors are written at the spec's default size, the ALP paper's 1024.
      VECTOR_SIZE = 1 << ALP_DEFAULT_LOG_VECTOR_SIZE

      # Values sampled from a vector when estimating a candidate's encoded size.
      SAMPLES_PER_VECTOR = 256

      # Vectors sampled from the first page to build the candidate set.
      SAMPLE_VECTORS = 8

      # Candidate `(exponent, factor)` pairs carried forward from sampling.
      MAX_COMBINATIONS = 5

      # Consecutive non-improving candidates after which selection stops.
      SAMPLING_EARLY_EXIT_THRESHOLD = 4

      # Largest slice decoded in one unpack-then-decode pass.
      DECODE_TILE_CAP = 1 << ALP_DEFAULT_LOG_VECTOR_SIZE

      ALP_POW10_F32 = [
        1.0_f32, 10.0_f32, 100.0_f32, 1000.0_f32, 10000.0_f32, 100000.0_f32,
        1000000.0_f32, 10000000.0_f32, 100000000.0_f32, 1000000000.0_f32, 10000000000.0_f32,
      ]

      ALP_POW10_F64 = [
        1.0_f64, 10.0_f64, 100.0_f64, 1000.0_f64, 10000.0_f64, 100000.0_f64,
        1000000.0_f64, 10000000.0_f64, 100000000.0_f64, 1000000000.0_f64, 10000000000.0_f64,
        100000000000.0_f64, 1000000000000.0_f64, 10000000000000.0_f64, 100000000000000.0_f64,
        1000000000000000.0_f64, 10000000000000000.0_f64, 100000000000000000.0_f64,
        1000000000000000000.0_f64,
      ]

      ALP_NEG_POW10_F32 = [
        1.0_f32, 0.1_f32, 0.01_f32, 0.001_f32, 0.0001_f32, 0.00001_f32,
        0.000001_f32, 0.0000001_f32, 0.00000001_f32, 0.000000001_f32, 0.0000000001_f32,
      ]

      ALP_NEG_POW10_F64 = [
        1.0_f64, 0.1_f64, 0.01_f64, 0.001_f64, 0.0001_f64, 0.00001_f64,
        0.000001_f64, 0.0000001_f64, 0.00000001_f64, 0.000000001_f64, 0.0000000001_f64,
        0.00000000001_f64, 0.000000000001_f64, 0.0000000000001_f64, 0.00000000000001_f64,
        0.000000000000001_f64, 0.0000000000000001_f64, 0.00000000000000001_f64,
        0.000000000000000001_f64,
      ]

      # One ALP decimal-encoding candidate: `encoded = round(value * 10^e * 10^-f)`.
      record ExponentAndFactor, exponent : UInt8, factor : UInt8

      # A candidate together with how often it won across the sampled vectors.
      class Combination
        property params : ExponentAndFactor
        property num_appearances : UInt64
        property estimated_size_bits : UInt64

        def initialize(@params, @num_appearances, @estimated_size_bits)
        end
      end

      # Sort key ranking candidates, larger is better.
      def self.rank(combination : Combination) : Tuple(UInt64, Int64, UInt8, UInt8)
        {
          combination.num_appearances,
          -combination.estimated_size_bits.to_i64,
          combination.params.exponent,
          combination.params.factor,
        }
      end

      def self.better?(c1 : Combination, c2 : Combination) : Bool
        rank(c1) > rank(c2)
      end

      def self.div_ceil(value : Int32, divisor : Int32) : Int32
        (value + divisor - 1) // divisor
      end

      def self.trailing_zeros(value : Int32) : Int32
        return 32 if value == 0

        count = 0
        v = value
        while (v & 1) == 0
          count += 1
          v >>= 1
        end
        count
      end

      def self.power_of_two?(value : Int32) : Bool
        value > 0 && (value & (value - 1)) == 0
      end

      # ---- AlpExact (integer side), dispatched on the float type ----

      def self.exact_width(type : Float32.class) : Int32
        4
      end

      def self.exact_width(type : Float64.class) : Int32
        8
      end

      def self.exact_from_le_slice(type : Float32.class, slice : Bytes) : UInt64
        IO::Memory.new(slice[0, 4]).read_bytes(UInt32, IO::ByteFormat::LittleEndian).to_u64
      end

      def self.exact_from_le_slice(type : Float64.class, slice : Bytes) : UInt64
        IO::Memory.new(slice[0, 8]).read_bytes(UInt64, IO::ByteFormat::LittleEndian)
      end

      def self.exact_reinterpret_as_signed(type : Float32.class, value : UInt64) : Int64
        value.to_u32!.unsafe_as(Int32).to_i64
      end

      def self.exact_reinterpret_as_signed(type : Float64.class, value : UInt64) : Int64
        value.unsafe_as(Int64)
      end

      def self.exact_reinterpret_from_signed(type : Float32.class, signed : Int64) : UInt64
        signed.to_i32!.unsafe_as(UInt32).to_u64
      end

      def self.exact_reinterpret_from_signed(type : Float64.class, signed : Int64) : UInt64
        signed.unsafe_as(UInt64)
      end

      def self.exact_wrapping_add(type : Float32.class, left : UInt64, right : UInt64) : UInt64
        (left &+ right) & 0xFFFF_FFFF_u64
      end

      def self.exact_wrapping_add(type : Float64.class, left : UInt64, right : UInt64) : UInt64
        left &+ right
      end

      def self.exact_wrapping_sub(type : Float32.class, left : UInt64, right : UInt64) : UInt64
        (left &- right) & 0xFFFF_FFFF_u64
      end

      def self.exact_wrapping_sub(type : Float64.class, left : UInt64, right : UInt64) : UInt64
        left &- right
      end

      def self.exact_extend_le_bytes(type : Float32.class, value : UInt64, dest : Array(UInt8)) : Nil
        4.times { |i| dest << ((value >> (8 * i)) & 0xFF).to_u8! }
      end

      def self.exact_extend_le_bytes(type : Float64.class, value : UInt64, dest : Array(UInt8)) : Nil
        8.times { |i| dest << ((value >> (8 * i)) & 0xFF).to_u8! }
      end

      def self.exception_bits(type : F.class) : UInt64 forall F
        (exact_width(F).to_u64 * 8) + 16
      end

      # ---- AlpFloat (float side), dispatched on the float type ----

      def self.max_exponent(type : Float32.class) : UInt8
        ALP_MAX_EXPONENT_F32
      end

      def self.max_exponent(type : Float64.class) : UInt8
        ALP_MAX_EXPONENT_F64
      end

      def self.magic(type : Float32.class) : Float32
        12582912.0_f32
      end

      def self.magic(type : Float64.class) : Float64
        6755399441055744.0_f64
      end

      def self.encoding_lower_limit(type : Float32.class) : Float32
        -2147483500.0_f32
      end

      def self.encoding_lower_limit(type : Float64.class) : Float64
        -9223372036854775000.0_f64
      end

      def self.encoding_upper_limit(type : Float32.class) : Float32
        2147483500.0_f32
      end

      def self.encoding_upper_limit(type : Float64.class) : Float64
        9223372036854775000.0_f64
      end

      def self.encoding_sentinel(type : Float32.class) : Int64
        2147483520_i64
      end

      def self.encoding_sentinel(type : Float64.class) : Int64
        9223372036854774784_i64
      end

      def self.decode_scale(type : Float32.class, exponent : UInt8, factor : UInt8) : Tuple(Float32, Float32)
        {ALP_POW10_F32[factor], ALP_NEG_POW10_F32[exponent]}
      end

      def self.decode_scale(type : Float64.class, exponent : UInt8, factor : UInt8) : Tuple(Float64, Float64)
        {ALP_POW10_F64[factor], ALP_NEG_POW10_F64[exponent]}
      end

      def self.encode_scale(type : Float32.class, exponent : UInt8, factor : UInt8) : Tuple(Float32, Float32)
        {ALP_POW10_F32[exponent], ALP_NEG_POW10_F32[factor]}
      end

      def self.encode_scale(type : Float64.class, exponent : UInt8, factor : UInt8) : Tuple(Float64, Float64)
        {ALP_POW10_F64[exponent], ALP_NEG_POW10_F64[factor]}
      end

      def self.apply_scale(value : Float32, scale : Tuple(Float32, Float32)) : Float32
        (value * scale[0]) * scale[1]
      end

      def self.apply_scale(value : Float64, scale : Tuple(Float64, Float64)) : Float64
        (value * scale[0]) * scale[1]
      end

      def self.decode_value(type : Float32.class, signed : Int64, scale : Tuple(Float32, Float32)) : Float32
        (signed.to_f32 * scale[0]) * scale[1]
      end

      def self.decode_value(type : Float64.class, signed : Int64, scale : Tuple(Float64, Float64)) : Float64
        (signed.to_f64 * scale[0]) * scale[1]
      end

      def self.from_exact_bits(type : Float32.class, bits : UInt64) : Float32
        bits.to_u32!.unsafe_as(Float32)
      end

      def self.from_exact_bits(type : Float64.class, bits : UInt64) : Float64
        bits.unsafe_as(Float64)
      end

      def self.to_exact_bits(value : Float32) : UInt64
        value.unsafe_as(UInt32).to_u64
      end

      def self.to_exact_bits(value : Float64) : UInt64
        value.unsafe_as(UInt64)
      end

      def self.impossible_to_encode?(type : Float32.class, value : Float32) : Bool
        value.nan? ||
          value < encoding_lower_limit(Float32) ||
          value > encoding_upper_limit(Float32) ||
          (value == 0.0_f32 && (1.0_f32 / value) < 0)
      end

      def self.impossible_to_encode?(type : Float64.class, value : Float64) : Bool
        value.nan? ||
          value < encoding_lower_limit(Float64) ||
          value > encoding_upper_limit(Float64) ||
          (value == 0.0_f64 && (1.0_f64 / value) < 0)
      end

      def self.fast_round(type : Float32.class, value : Float32) : Int64
        ((value + magic(Float32)) - magic(Float32)).to_i32.to_i64
      end

      def self.fast_round(type : Float64.class, value : Float64) : Int64
        ((value + magic(Float64)) - magic(Float64)).to_i64
      end

      def self.encode_value(type : Float32.class, value : Float32, scale : Tuple(Float32, Float32)) : Int64
        scaled = apply_scale(value, scale)
        return encoding_sentinel(Float32) if impossible_to_encode?(Float32, scaled)

        fast_round(Float32, scaled)
      end

      def self.encode_value(type : Float64.class, value : Float64, scale : Tuple(Float64, Float64)) : Int64
        scaled = apply_scale(value, scale)
        return encoding_sentinel(Float64) if impossible_to_encode?(Float64, scaled)

        fast_round(Float64, scaled)
      end

      # ---- Sampling and candidate selection ----

      # Estimate, in bits, what `params` would cost on `sample`.
      def self.estimate_size_bits(
        type : F.class,
        sample : Array(F),
        params : ExponentAndFactor,
        penalize_exceptions : Bool,
      ) : UInt64? forall F
        encode_scale = encode_scale(F, params.exponent, params.factor)
        decode_scale = decode_scale(F, params.exponent, params.factor)

        num_exceptions = 0_u64
        min = nil.as(Int64?)
        max = nil.as(Int64?)

        sample.each do |value|
          encoded = encode_value(F, value, encode_scale)
          if decode_value(F, encoded, decode_scale) == value
            if current_min = min
              min = Math.min(current_min, encoded)
            else
              min = encoded
            end
            if current_max = max
              max = Math.max(current_max, encoded)
            else
              max = encoded
            end
          else
            num_exceptions += 1
          end
        end

        num_values = sample.size.to_u64
        num_non_exceptions = num_values - num_exceptions
        return nil if penalize_exceptions && num_non_exceptions < 2

        mn = min
        mx = max
        if mn.nil? || mx.nil?
          return num_values &* exception_bits(F)
        end

        range = exact_wrapping_sub(
          F,
          exact_reinterpret_from_signed(F, mx),
          exact_reinterpret_from_signed(F, mn),
        )
        bit_width = Util::BitUtil.num_required_bits(range).to_u64

        num_values &* bit_width &+ num_exceptions &* exception_bits(F)
      end

      # Sample every `n`th value so the whole span is represented.
      def self.sample_values(type : F.class, values : Array(F), dest : Array(F)) : Nil forall F
        dest.clear
        stride = Math.max(div_ceil(values.size, SAMPLES_PER_VECTOR), 1)
        i = 0
        while i < values.size
          dest << values[i]
          i += stride
        end
      end

      # Build the column chunk's candidate set.
      def self.build_preset(type : F.class, values : Array(F)) : Array(ExponentAndFactor) forall F
        num_vectors = div_ceil(values.size, VECTOR_SIZE)
        vector_stride = Math.max(div_ceil(num_vectors, SAMPLE_VECTORS), 1)

        sample = [] of F
        tally = [] of Combination

        vi = 0
        while vi < num_vectors
          vector = values[vi * VECTOR_SIZE, Math.min(VECTOR_SIZE, values.size - vi * VECTOR_SIZE)]
          sample_values(F, vector, sample)

          max_exponent = Alp.max_exponent(F)
          best = Combination.new(
            ExponentAndFactor.new(max_exponent, max_exponent),
            0_u64,
            sample.size.to_u64 &* (exception_bits(F) &+ (exact_width(F).to_u64 &* 8)),
          )

          (0_u8..max_exponent).each do |exponent|
            (0_u8..exponent).each do |factor|
              params = ExponentAndFactor.new(exponent, factor)
              if estimated_size_bits = estimate_size_bits(F, sample, params, true)
                candidate = Combination.new(params, 0_u64, estimated_size_bits)
                best = candidate if better?(candidate, best)
              end
            end
          end

          existing = tally.find { |combination| combination.params == best.params }
          if existing
            existing.num_appearances += 1
          else
            tally << Combination.new(best.params, 1_u64, best.estimated_size_bits)
          end
          vi += vector_stride
        end

        tally.each(&.estimated_size_bits=(0_u64))
        tally.sort_by! { |combination| rank(combination) }
        tally.reverse!
        tally = tally[0, MAX_COMBINATIONS] if tally.size > MAX_COMBINATIONS

        return [ExponentAndFactor.new(0_u8, 0_u8)] if tally.empty?

        tally.map(&.params)
      end

      # Pick the candidate that encodes `vector` smallest.
      def self.select_params(
        type : F.class,
        vector : Array(F),
        preset : Array(ExponentAndFactor),
        sample : Array(F),
      ) : ExponentAndFactor forall F
        return preset[0] if preset.size == 1

        sample_values(F, vector, sample)

        best = preset[0]
        best_size_bits = UInt64::MAX
        worse_in_a_row = 0

        preset.each do |params|
          size_bits = estimate_size_bits(F, sample, params, false)
          next if size_bits.nil?

          if size_bits >= best_size_bits
            worse_in_a_row += 1
            break if worse_in_a_row == SAMPLING_EARLY_EXIT_THRESHOLD

            next
          end
          best = params
          best_size_bits = size_bits
          worse_in_a_row = 0
        end
        best
      end

      # Reusable per-vector buffers.
      class Scratch(F)
        property encoded : Array(Int64)
        property exc_mask : Array(UInt8)
        property exception_positions : Array(Int32)
        property exception_values : Array(F)
        property sample : Array(F)

        def initialize
          @encoded = [] of Int64
          @exc_mask = [] of UInt8
          @exception_positions = [] of Int32
          @exception_values = [] of F
          @sample = [] of F
        end

        def estimated_memory_size : Int32
          @encoded.size * 8 + @exc_mask.size + @exception_positions.size * 4 +
            @exception_values.size * sizeof(F) + @sample.size * sizeof(F)
        end
      end

      def self.first_non_exception_value(encoded : Array(Int64), exception_positions : Array(Int32)) : Int64
        candidate = 0
        exception_positions.each do |position|
          break if position != candidate

          candidate += 1
        end
        encoded[candidate]? || 0_i64
      end

      def self.write_u16_le(dest : Array(UInt8), value : UInt16) : Nil
        dest << (value & 0xFF).to_u8!
        dest << ((value >> 8) & 0xFF).to_u8!
      end

      def self.write_u32_le(dest : Array(UInt8), value : UInt32) : Nil
        4.times { |i| dest << ((value >> (8 * i)) & 0xFF).to_u8! }
      end

      # Encode one vector and append it to `out`.
      def self.encode_vector(
        type : F.class,
        values : Array(F),
        params : ExponentAndFactor,
        scratch : Scratch(F),
        dest : Array(UInt8),
      ) : Nil forall F
        encode_scale = encode_scale(F, params.exponent, params.factor)
        decode_scale = decode_scale(F, params.exponent, params.factor)

        n_values = values.size
        encoded = Array(Int64).new(n_values, 0_i64)
        exc_mask = Array(UInt8).new(n_values, 0_u8)

        n_values.times do |idx|
          value = values[idx]
          encoded_value = encode_value(F, value, encode_scale)
          encoded[idx] = encoded_value
          exc_mask[idx] = decode_value(F, encoded_value, decode_scale) == value ? 0_u8 : 1_u8
        end

        scratch.encoded = encoded
        scratch.exc_mask = exc_mask

        # Branchless compaction: always write the index, advance only when flagged.
        positions = Array(Int32).new(n_values, 0)
        num_exceptions = 0
        exc_mask.each_with_index do |is_exception, idx|
          positions[num_exceptions] = idx
          num_exceptions += 1 if is_exception == 1
        end
        positions = positions[0, num_exceptions]
        scratch.exception_positions = positions

        if num_exceptions > UInt16::MAX
          raise general_err!("Invalid ALP vector: #{num_exceptions} exceptions exceeds u16::MAX")
        end

        # Fill each exception's packed slot with a real value so the FOR range
        # stays tight; the true value goes in the exception section.
        placeholder = first_non_exception_value(encoded, positions)
        exception_values = [] of F
        positions.each do |position|
          exception_values << values[position]
          encoded[position] = placeholder
        end
        scratch.exception_values = exception_values

        min = encoded.min
        max = encoded.max

        frame_of_reference = exact_reinterpret_from_signed(F, min)
        range = exact_wrapping_sub(F, exact_reinterpret_from_signed(F, max), frame_of_reference)
        bit_width = Util::BitUtil.num_required_bits(range)

        alp_info = AlpInfo.new(params.exponent, params.factor, num_exceptions.to_u16!)
        for_info = ForInfo(F).new(frame_of_reference, bit_width)
        alp_info.extend_serialized(dest)
        for_info.extend_serialized(dest)

        if bit_width > 0
          writer = Util::BitWriter.new_from_buf(dest)
          encoded.each do |encoded_value|
            delta = exact_wrapping_sub(F, exact_reinterpret_from_signed(F, encoded_value), frame_of_reference)
            writer.put_value(delta, bit_width.to_i32)
          end
          writer.consume
        end

        positions.each { |position| write_u16_le(dest, position.to_u16!) }
        exception_values.each { |value| exact_extend_le_bytes(F, to_exact_bits(value), dest) }
      end

      # Encode `values` as one ALP page.
      def self.encode_page(
        type : F.class,
        values : Array(F),
        preset : Array(ExponentAndFactor),
        scratch : Scratch(F),
      ) : Bytes forall F
        header = AlpHeader.new(ALP_COMPRESSION_MODE, ALP_INTEGER_ENCODING_FOR_BIT_PACK, VECTOR_SIZE, values.size.to_i64)
        num_vectors = header.num_vectors

        page = [] of UInt8
        page.concat(header.serialize.to_a)
        offsets_start = page.size
        page.concat(Array(UInt8).new(num_vectors * 4, 0_u8))

        vi = 0
        while vi < num_vectors
          vector = values[vi * VECTOR_SIZE, Math.min(VECTOR_SIZE, values.size - vi * VECTOR_SIZE)]

          offset = page.size - ALP_HEADER_SIZE
          if offset > UInt32::MAX
            raise general_err!("Invalid ALP page: body exceeds u32 offset range")
          end
          offset_at = offsets_start + vi * 4
          page[offset_at] = (offset & 0xFF).to_u8!
          page[offset_at + 1] = ((offset >> 8) & 0xFF).to_u8!
          page[offset_at + 2] = ((offset >> 16) & 0xFF).to_u8!
          page[offset_at + 3] = ((offset >> 24) & 0xFF).to_u8!

          params = select_params(F, vector, preset, scratch.sample)
          encode_vector(F, vector, params, scratch, page)
          vi += 1
        end

        Slice.new(page.to_unsafe, page.size).dup
      end

      # A page encoded incrementally, one vector at a time.
      class StreamingPage(F)
        property body : Array(UInt8)
        property vector_offsets : Array(UInt32)
        property carry : Array(F)
        property count : Int64

        def initialize
          @body = [] of UInt8
          @vector_offsets = [] of UInt32
          @carry = [] of F
          @count = 0_i64
        end

        def estimated_memory_size : Int32
          @body.size + @vector_offsets.size * 4 + @carry.size * sizeof(F)
        end

        def push_vector(vector : Array(F), preset : Array(ExponentAndFactor), scratch : Scratch(F)) : Nil
          body_start = @body.size.to_u32!
          @vector_offsets << body_start
          params = Alp.select_params(F, vector, preset, scratch.sample)
          Alp.encode_vector(F, vector, params, scratch, @body)
        end

        def put(values : Array(F), preset : Array(ExponentAndFactor), scratch : Scratch(F)) : Nil
          @count += values.size
          remaining = values

          unless @carry.empty?
            need = Alp::VECTOR_SIZE - @carry.size
            if remaining.size < need
              @carry.concat(remaining)
              return
            end
            head = remaining[0, need]
            remaining = remaining[need, remaining.size - need]
            @carry.concat(head)
            vector = @carry
            @carry = [] of F
            push_vector(vector, preset, scratch)
          end

          i = 0
          while i + Alp::VECTOR_SIZE <= remaining.size
            push_vector(remaining[i, Alp::VECTOR_SIZE], preset, scratch)
            i += Alp::VECTOR_SIZE
          end
          @carry.concat(remaining[i, remaining.size - i])
        end

        def finish(preset : Array(ExponentAndFactor), scratch : Scratch(F)) : Bytes
          unless @carry.empty?
            vector = @carry
            @carry = [] of F
            push_vector(vector, preset, scratch)
          end

          num_vectors = @vector_offsets.size
          offsets_section = num_vectors * 4
          header = AlpHeader.new(ALP_COMPRESSION_MODE, ALP_INTEGER_ENCODING_FOR_BIT_PACK, Alp::VECTOR_SIZE, @count)

          page = [] of UInt8
          page.concat(header.serialize.to_a)
          @vector_offsets.each do |body_start|
            page_offset = offsets_section + body_start
            if page_offset > UInt32::MAX
              raise general_err!("Invalid ALP page: body exceeds u32 offset range")
            end
            Alp.write_u32_le(page, page_offset.to_u32!)
          end
          page.concat(@body)

          @body.clear
          @vector_offsets.clear
          @count = 0_i64
          Slice.new(page.to_unsafe, page.size).dup
        end
      end

      # ---- Page/vector parsing (decoder side) ----

      def self.parse_alp_page_header(data : Bytes) : AlpHeader
        header = AlpHeader.deserialize(data)

        if header.compression_mode != ALP_COMPRESSION_MODE
          raise general_err!("Invalid ALP page: unsupported compression mode #{header.compression_mode}")
        end
        if header.integer_encoding != ALP_INTEGER_ENCODING_FOR_BIT_PACK
          raise general_err!("Invalid ALP page: unsupported integer encoding #{header.integer_encoding}")
        end
        if header.vector_size < (1 << ALP_MIN_LOG_VECTOR_SIZE)
          raise general_err!(
            "Invalid ALP page: log_vector_size #{trailing_zeros(header.vector_size)} below min #{ALP_MIN_LOG_VECTOR_SIZE}"
          )
        end
        if header.vector_size > (1 << ALP_MAX_LOG_VECTOR_SIZE)
          raise general_err!(
            "Invalid ALP page: log_vector_size #{trailing_zeros(header.vector_size)} exceeds max #{ALP_MAX_LOG_VECTOR_SIZE}"
          )
        end

        header
      end

      def self.read_offset(body : Bytes, idx : Int32) : Int32
        start = idx * 4
        if start + 4 > body.size
          raise general_err!("Invalid ALP page: offset index #{idx} out of bounds")
        end
        (body[start].to_u32! |
          (body[start + 1].to_u32! << 8) |
          (body[start + 2].to_u32! << 16) |
          (body[start + 3].to_u32! << 24)).to_i32!
      end

      def self.parse_vector_view(
        type : F.class,
        body : Bytes,
        vector_start : Int32,
        vector_end : Int32,
        num_elements : Int32,
      ) : AlpEncodedVectorView(F) forall F
        vector_bytes = body[vector_start, vector_end - vector_start]

        metadata_size = AlpInfo::STORED_SIZE + ForInfo(F).stored_size
        if vector_bytes.size < metadata_size
          raise general_err!(
            "Invalid ALP page: vector metadata too short, expected at least #{metadata_size} bytes, got #{vector_bytes.size}"
          )
        end

        alp_info = AlpInfo.new(
          vector_bytes[0],
          vector_bytes[1],
          (vector_bytes[2].to_u16! | (vector_bytes[3].to_u16! << 8)),
        )

        max_exponent = Alp.max_exponent(F)
        if alp_info.exponent > max_exponent
          raise general_err!("Invalid ALP page: exponent #{alp_info.exponent} exceeds max #{max_exponent}")
        end
        if alp_info.factor > alp_info.exponent
          raise general_err!("Invalid ALP page: factor #{alp_info.factor} exceeds exponent #{alp_info.exponent}")
        end
        if alp_info.num_exceptions > num_elements
          raise general_err!(
            "Invalid ALP page: num_exceptions #{alp_info.num_exceptions} exceeds vector num_elements #{num_elements}"
          )
        end

        width = Alp.exact_width(F)
        for_start = AlpInfo::STORED_SIZE
        frame_of_reference = exact_from_le_slice(F, vector_bytes[for_start, width])
        bit_width = vector_bytes[for_start + width]

        if bit_width.to_i32 > width * 8
          raise general_err!("Invalid ALP page: bit width #{bit_width} exceeds #{width * 8}")
        end

        for_info = ForInfo(F).new(frame_of_reference, bit_width)
        data_size = for_info.get_data_stored_size(num_elements, alp_info.num_exceptions)
        expected_size = metadata_size + data_size
        if vector_bytes.size < expected_size
          raise general_err!(
            "Invalid ALP page: vector data too short, expected at least #{expected_size} bytes, got #{vector_bytes.size}"
          )
        end
        if vector_bytes.size > expected_size
          raise general_err!(
            "Invalid ALP page: vector data too long, expected #{expected_size} bytes, got #{vector_bytes.size}"
          )
        end

        packed_size = for_info.get_bit_packed_size(num_elements)
        positions_size = alp_info.num_exceptions.to_i32 * 2
        positions_start = packed_size
        values_start = positions_start + positions_size

        idx = positions_start
        while idx < values_start
          position = vector_bytes[metadata_size + idx].to_i32 | (vector_bytes[metadata_size + idx + 1].to_i32 << 8)
          if position >= num_elements
            raise general_err!(
              "Invalid ALP page: exception position #{position} out of bounds for vector length #{num_elements}"
            )
          end
          idx += 2
        end

        data_start = vector_start + metadata_size
        AlpEncodedVectorView(F).new(
          num_elements,
          alp_info,
          for_info,
          data_start,
          data_start + positions_start,
          data_start + values_start,
        )
      end

      # Live decode state for the one vector currently being consumed.
      class CurrentVector(F)
        property reader : Util::BitReader
        property bit_width : UInt8
        property frame_of_reference : UInt64
        property scale : Tuple(F, F)
        property remaining : Int32
        property delivered : Int32
        property exception_positions : Bytes
        property exception_values : Bytes

        def initialize(
          @reader,
          @bit_width,
          @frame_of_reference,
          @scale,
          @remaining,
          @delivered,
          @exception_positions,
          @exception_values,
        )
        end
      end

      # Decode the next `out.size` elements of the current vector into `out`.
      def self.decode_range(type : F.class, cur : CurrentVector(F), scratch : Array(UInt64), dest : Slice(F)) : Nil forall F
        frame_of_reference = cur.frame_of_reference
        if cur.bit_width == 0
          signed = exact_reinterpret_as_signed(F, frame_of_reference)
          decoded = decode_value(F, signed, cur.scale)
          dest.size.times { |i| dest[i] = decoded }
        else
          bit_width = cur.bit_width.to_i32
          scale = cur.scale
          pos = 0
          while pos < dest.size
            chunk = Math.min(scratch.size, dest.size - pos)
            deltas = Slice.new(scratch.to_unsafe, scratch.size)[0, chunk]
            unpacked = cur.reader.get_batch(UInt64, deltas, bit_width)
            if unpacked != chunk
              raise general_err!("Invalid ALP page: not enough packed bits to decode vector")
            end
            chunk.times do |i|
              signed = exact_reinterpret_as_signed(F, exact_wrapping_add(F, scratch[i], frame_of_reference))
              dest[pos + i] = decode_value(F, signed, scale)
            end
            pos += chunk
          end
        end

        lo = cur.delivered
        hi = lo + dest.size
        num_exceptions = cur.exception_positions.size // 2
        idx = 0
        while idx < num_exceptions
          position =
            cur.exception_positions[idx * 2].to_i32 | (cur.exception_positions[idx * 2 + 1].to_i32 << 8)
          if position >= lo && position < hi
            value = exact_from_le_slice(F, cur.exception_values[idx * exact_width(F), exact_width(F)])
            dest[position - lo] = from_exact_bits(F, value)
          end
          idx += 1
        end

        cur.delivered = hi
        cur.remaining -= dest.size
      end
    end

    # Page-level ALP header (7 bytes).
    struct AlpHeader
      getter compression_mode : UInt8
      getter integer_encoding : UInt8
      getter vector_size : Int32
      getter num_elements : Int64

      def initialize(@compression_mode, @integer_encoding, @vector_size, @num_elements)
      end

      def self.deserialize(bytes : Bytes) : AlpHeader
        if bytes.size < Alp::ALP_HEADER_SIZE
          raise general_err!(
            "Invalid ALP page: expected at least #{Alp::ALP_HEADER_SIZE} bytes for header, got #{bytes.size}"
          )
        end

        log_vector_size = bytes[2]
        if log_vector_size >= 64
          raise general_err!(
            "Invalid ALP page: log_vector_size #{log_vector_size} too large to represent a vector size"
          )
        end
        vector_size = (1_u64 << log_vector_size).to_i32!

        num_elements_i32 = IO::Memory.new(bytes[3, 4]).read_bytes(Int32, IO::ByteFormat::LittleEndian)
        if num_elements_i32 < 0
          raise general_err!("Invalid ALP page: num_elements #{num_elements_i32} must be >= 0")
        end

        new(bytes[0], bytes[1], vector_size, num_elements_i32.to_i64)
      end

      def serialize : Bytes
        unless Alp.power_of_two?(@vector_size)
          raise general_err!("Invalid ALP page: vector_size #{@vector_size} is not a power of two")
        end

        if @num_elements > Int32::MAX
          raise general_err!("Invalid ALP page: num_elements #{@num_elements} exceeds i32::MAX")
        end

        dest = Bytes.new(Alp::ALP_HEADER_SIZE)
        dest[0] = @compression_mode
        dest[1] = @integer_encoding
        dest[2] = Alp.trailing_zeros(@vector_size).to_u8!
        IO::Memory.new(dest[3, 4]).write_bytes(@num_elements.to_i32, IO::ByteFormat::LittleEndian)
        dest
      end

      def num_vectors : Int32
        ((@num_elements + @vector_size - 1) >> Alp.trailing_zeros(@vector_size)).to_i32
      end

      def vector_num_elements(vector_index : Int32) : Int32
        start = vector_index.to_i64 * @vector_size
        remaining = @num_elements - start
        remaining = 0_i64 if remaining < 0
        Math.min(remaining, @vector_size.to_i64).to_i32
      end
    end

    # Per-vector ALP metadata (4 bytes).
    struct AlpInfo
      STORED_SIZE = 4

      getter exponent : UInt8
      getter factor : UInt8
      getter num_exceptions : UInt16

      def initialize(@exponent, @factor, @num_exceptions)
      end

      def extend_serialized(dest : Array(UInt8)) : Nil
        dest << @exponent
        dest << @factor
        dest << (@num_exceptions & 0xFF).to_u8!
        dest << ((@num_exceptions >> 8) & 0xFF).to_u8!
      end
    end

    # Per-vector FOR metadata: 5 bytes for f32, 9 for f64.
    class ForInfo(F)
      getter frame_of_reference : UInt64
      getter bit_width : UInt8

      def initialize(@frame_of_reference, @bit_width)
      end

      def self.stored_size : Int32
        Alp.exact_width(F) + 1
      end

      def extend_serialized(dest : Array(UInt8)) : Nil
        Alp.exact_extend_le_bytes(F, @frame_of_reference, dest)
        dest << @bit_width
      end

      def get_bit_packed_size(num_elements : Int32) : Int32
        (@bit_width.to_i32 * num_elements + 7) // 8
      end

      def get_data_stored_size(num_elements : Int32, num_exceptions : UInt16) : Int32
        get_bit_packed_size(num_elements) +
          num_exceptions.to_i32 * 2 +
          num_exceptions.to_i32 * Alp.exact_width(F)
      end
    end

    # Parsed view of one vector's metadata and data sections.
    class AlpEncodedVectorView(F)
      getter num_elements : Int32
      getter alp_info : AlpInfo
      getter for_info : ForInfo(F)
      getter packed_values : Int32
      getter exception_positions : Int32
      getter exception_values : Int32

      def initialize(@num_elements, @alp_info, @for_info, @packed_values, @exception_positions, @exception_values)
      end

      def expected_stored_size : Int32
        AlpInfo::STORED_SIZE +
          ForInfo(F).stored_size +
          @for_info.get_data_stored_size(@num_elements, @alp_info.num_exceptions)
      end
    end

    # Encoder for ALP-encoded floating-point pages (f32/f64).
    class AlpEncoder(F) < Encoder(F)
      def initialize
        @values = [] of F
        @preset = nil.as(Array(Alp::ExponentAndFactor)?)
        @scratch = Alp::Scratch(F).new
        @streaming = Alp::StreamingPage(F).new
      end

      def put(values : Array(F)) : Nil
        if preset = @preset
          @streaming.put(values, preset, @scratch)
        else
          @values.concat(values)
        end
      end

      def encoding : Encoding
        Encoding::ALP
      end

      def estimated_data_encoded_size : Int32
        len = current_page_len
        num_vectors = Alp.div_ceil(len, Alp::VECTOR_SIZE)
        Alp::ALP_HEADER_SIZE +
          num_vectors * (4 + AlpInfo::STORED_SIZE + ForInfo(F).stored_size) +
          len * (2 * Alp.exact_width(F) + 2)
      end

      def estimated_memory_size : Int32
        @values.size * sizeof(F) +
          (@preset.try(&.size) || 0) * sizeof(Alp::ExponentAndFactor) +
          @scratch.estimated_memory_size +
          @streaming.estimated_memory_size
      end

      def flush_buffer : Bytes
        if preset = @preset
          @streaming.finish(preset, @scratch)
        else
          built = Alp.build_preset(F, @values)
          page = Alp.encode_page(F, @values, built, @scratch)
          @values.clear
          @preset = built
          page
        end
      end

      private def current_page_len : Int32
        @preset.nil? ? @values.size : @streaming.count.to_i32
      end
    end

    # Decoder for ALP-encoded floating-point pages (f32/f64).
    class AlpDecoder(F) < Decoder(F)
      def initialize
        @header = AlpHeader.new(0_u8, 0_u8, 1, 0_i64)
        @body = Bytes.new(0)
        @next_vector_idx = 0
        @expected_next_offset = 0
        @current = nil.as(Alp::CurrentVector(F)?)
        @scratch = [] of UInt64
        @num_values = 0
      end

      def set_data(data : Bytes, num_values : Int32) : Nil
        header = Alp.parse_alp_page_header(data)

        if header.num_elements > num_values
          raise general_err!(
            "Invalid ALP page: header num_elements #{header.num_elements} exceeds page num_values #{num_values}"
          )
        end

        num_values = header.num_elements.to_i32
        offsets_section_size = header.num_vectors * 4
        body = data[Alp::ALP_HEADER_SIZE, data.size - Alp::ALP_HEADER_SIZE]

        if body.size < offsets_section_size
          raise general_err!(
            "Invalid ALP page: expected at least #{offsets_section_size} bytes for #{header.num_vectors} offsets, got #{body.size}"
          )
        end

        tile = Math.max(Math.min(Math.min(header.vector_size, Alp::DECODE_TILE_CAP), num_values), 1)
        @scratch = Array(UInt64).new(tile, 0_u64)

        @header = header
        @body = body
        @next_vector_idx = 0
        @expected_next_offset = offsets_section_size
        @current = nil
        @num_values = num_values
      end

      def get(buffer : Array(F)) : Int32
        target = Math.min(buffer.size, @num_values)
        return 0 if target == 0

        written = 0
        while written < target
          current = @current
          if current.nil? || current.remaining == 0
            load_current_vector
            current = @current
          end
          raise general_err!("Invalid ALP page: no vector available") if current.nil?

          n = Math.min(current.remaining, target - written)
          Alp.decode_range(F, current, @scratch, Slice.new(buffer.to_unsafe + written, n))
          written += n
          @num_values -= n
        end
        target
      end

      def values_left : Int32
        @num_values
      end

      def encoding : Encoding
        Encoding::ALP
      end

      def skip(num_values : Int32) : Int32
        to_skip = Math.min(num_values, @num_values)
        return 0 if to_skip == 0

        left = to_skip

        if current = @current
          within = Math.min(left, current.remaining)
          if current.bit_width != 0
            current.reader.skip(within, current.bit_width.to_i32)
          end
          current.delivered += within
          current.remaining -= within
          @num_values -= within
          left -= within
        end
        return to_skip if left == 0

        vector_size = @header.vector_size
        num_elements = @header.num_elements.to_i32
        target = (num_elements - @num_values) + left

        @current = nil
        @num_values = num_elements - target

        if target == num_elements
          @next_vector_idx = @header.num_vectors
          return to_skip
        end

        landing = target >> Alp.trailing_zeros(vector_size)
        within = target & (vector_size - 1)

        @next_vector_idx = landing
        @expected_next_offset = Alp.read_offset(@body, landing)

        if within > 0
          load_current_vector
          current = @current
          raise general_err!("Invalid ALP page: no vector available") if current.nil?

          current.reader.skip(within, current.bit_width.to_i32) if current.bit_width != 0
          current.delivered = within
          current.remaining -= within
        end

        to_skip
      end

      private def load_current_vector : Nil
        idx = @next_vector_idx
        num_vectors = @header.num_vectors
        body_ref = @body

        start = Alp.read_offset(body_ref, idx)
        if start != @expected_next_offset
          raise general_err!(
            "Invalid ALP page: vector offset #{start} at index #{idx} does not match expected #{@expected_next_offset}"
          )
        end

        end_offset = idx + 1 < num_vectors ? Alp.read_offset(body_ref, idx + 1) : body_ref.size
        if end_offset < start || end_offset > body_ref.size
          raise general_err!("Invalid ALP page: vector offset #{end_offset} out of bounds at index #{idx}")
        end

        count = @header.vector_num_elements(idx)
        view = Alp.parse_vector_view(F, body_ref, start, end_offset, count)

        @expected_next_offset = start + view.expected_stored_size

        packed_len = view.for_info.get_bit_packed_size(view.num_elements)
        packed = body_ref[view.packed_values, packed_len]
        positions_len = view.alp_info.num_exceptions.to_i32 * 2
        exception_positions = body_ref[view.exception_positions, positions_len]
        values_len = view.alp_info.num_exceptions.to_i32 * Alp.exact_width(F)
        exception_values = body_ref[view.exception_values, values_len]

        @current = Alp::CurrentVector(F).new(
          Util::BitReader.new(packed),
          view.for_info.bit_width,
          view.for_info.frame_of_reference,
          Alp.decode_scale(F, view.alp_info.exponent, view.alp_info.factor),
          count,
          0,
          exception_positions,
          exception_values,
        )
        @next_vector_idx += 1
      end
    end
  end
end
