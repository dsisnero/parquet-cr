require "./spec_helper"

describe Parquet do
  it "exposes a version" do
    Parquet::VERSION.should eq("0.1.0")
  end
end
