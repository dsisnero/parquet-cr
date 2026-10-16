require "./wkb"

# Ported from `parquet-geospatial/src/testing.rs` (apache/arrow-rs 60.0.0,
# ef1fa157977633f0ba21aa921f9ef3d5c669235b).
#
# Testing utilities for geospatial Parquet types.
module ParquetGeospatial
  # Testing helpers for building WKB buffers.
  module Testing
    private LE = IO::ByteFormat::LittleEndian

    # Builds well-known binary representing a point with the given XY coordinate.
    def self.wkb_point_xy(x : Float64, y : Float64) : Bytes
      io = IO::Memory.new(21)
      io.write_byte(0x01_u8)
      io.write_bytes(1_u32, LE)
      io.write_bytes(x, LE)
      io.write_bytes(y, LE)
      io.to_slice
    end

    # Builds well-known binary representing a point with the given XYZM
    # coordinate.
    def self.wkb_point_xyzm(x : Float64, y : Float64, z : Float64, m : Float64) : Bytes
      io = IO::Memory.new(37)
      io.write_byte(0x01_u8)
      io.write_bytes(3001_u32, LE)
      io.write_bytes(x, LE)
      io.write_bytes(y, LE)
      io.write_bytes(z, LE)
      io.write_bytes(m, LE)
      io.to_slice
    end
  end
end
