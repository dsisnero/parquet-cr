# Architecture

`parquet-cr` is a Crystal port of the `parquet` crate from
[apache/arrow-rs](https://github.com/apache/arrow-rs). The upstream crate is the
source of truth for behavior; this project follows its module layout closely so
that files can be compared one-to-one.

## Module layout

Upstream `parquet/src` maps to Crystal as follows:

| Upstream (Rust)      | Crystal                     | Responsibility                          |
| -------------------- | --------------------------- | --------------------------------------- |
| `errors.rs`          | `src/parquet/errors.cr`     | Error and result types                  |
| `basic.rs`           | `src/parquet/basic.cr`      | Logical/physical types, statistics      |
| `data_type.rs`       | `src/parquet/data_type.cr`  | Column data type model                  |
| `schema/`            | `src/parquet/schema/`       | Schema descriptors and parser           |
| `encodings/`         | `src/parquet/encodings/`    | RLE, dictionary, level, and ALP codecs  |
| `column/`            | `src/parquet/column/`       | Column reader/writer and descriptors    |
| `file/`              | `src/parquet/file/`         | File metadata, reader, writer           |
| `record/`            | `src/parquet/record/`       | Record (row) reader and writer API      |
| `geospatial/`        | `src/parquet/geospatial/`   | Geospatial statistics (bbox/accumulator) |
| `parquet-geospatial/` (crate) | `src/parquet_geospatial/` | WKB reader, intervals, geometry bounder |
| `arrow/`             | `src/parquet/arrow/`        | Arrow interoperability                  |
| `parquet_thrift.rs`  | `src/parquet/format/`       | Thrift-encoded Parquet metadata structs |

## Layering

The port preserves the upstream dependency direction:

1. `format` (Thrift structs) and `errors` have no internal dependencies.
2. `basic`, `data_type`, and `schema` depend only on the two above.
3. `encodings` build on `basic` and byte buffers.
4. `column` and `file` build on encodings and schema.
5. `record` and `arrow` are the highest-level APIs.

Do not introduce a dependency that reverses this direction.
