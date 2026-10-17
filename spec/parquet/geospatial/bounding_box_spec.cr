require "../../spec_helper"

# Ported from `parquet/src/geospatial/bounding_box.rs` test module
# (apache/arrow-rs 60.0.0, ef1fa157977633f0ba21aa921f9ef3d5c669235b).

private alias BBox = Parquet::Geospatial::BoundingBox

describe Parquet::Geospatial::BoundingBox do
  # Upstream `test_bounding_box`.
  it "exposes x/y ranges and optional z/m ranges" do
    bbox = BBox.new(0.0, 0.0, 10.0, 10.0)
    bbox.xmin.should eq(0.0)
    bbox.xmax.should eq(0.0)
    bbox.ymin.should eq(10.0)
    bbox.ymax.should eq(10.0)
    bbox.zmin.should be_nil
    bbox.zmax.should be_nil
    bbox.mmin.should be_nil
    bbox.mmax.should be_nil
    bbox.is_z_valid.should be_false
    bbox.is_m_valid.should be_false

    # test with zrange
    bbox_z = BBox.new(0.0, 0.0, 10.0, 10.0).with_zrange(5.0, 15.0)
    bbox_z.zmin.should eq(5.0)
    bbox_z.zmax.should eq(15.0)
    bbox_z.is_z_valid.should be_true
    bbox_z.is_m_valid.should be_false

    # test with mrange
    bbox_m = BBox.new(0.0, 0.0, 10.0, 10.0).with_mrange(10.0, 20.0)
    bbox_m.mmin.should eq(10.0)
    bbox_m.mmax.should eq(20.0)
    bbox_m.is_z_valid.should be_false
    bbox_m.is_m_valid.should be_true

    # test with zrange and mrange
    bbox_zm = BBox.new(0.0, 0.0, 10.0, 10.0)
      .with_zrange(5.0, 15.0)
      .with_mrange(10.0, 20.0)
    bbox_zm.zmin.should eq(5.0)
    bbox_zm.zmax.should eq(15.0)
    bbox_zm.mmin.should eq(10.0)
    bbox_zm.mmax.should eq(20.0)
    bbox_zm.is_z_valid.should be_true
    bbox_zm.is_m_valid.should be_true
  end

  # Upstream `with_xrange`/`with_yrange` (exercised by the doc examples).
  it "updates the x/y ranges" do
    bbox = BBox.new(0.0, 0.0, 0.0, 0.0).with_xrange(1.0, 2.0).with_yrange(3.0, 4.0)
    bbox.xmin.should eq(1.0)
    bbox.xmax.should eq(2.0)
    bbox.ymin.should eq(3.0)
    bbox.ymax.should eq(4.0)
  end
end
