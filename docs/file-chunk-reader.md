# Streaming file chunk readers

`Parquet::File::FileChunkReader#get_read` follows the pinned upstream
`impl ChunkReader for File` in `vendor/arrow-rs/parquet/src/file/reader.rs`:
duplicate the open handle, seek to the requested offset, then return a buffered
sequential reader. It does not allocate or read the whole remaining file.
Offsets remain UInt64 until the native seek; no file-tail length is narrowed to
Int32. Native file seeks use signed 64-bit offsets, so offsets outside that
range retain native conversion/seek errors.

Crystal's public `IO::FileDescriptor#reopen` duplicates the source handle into
an unused descriptor opened from `File::NULL`. The returned `File` supplies
bounded buffering. The source pathname is never reopened, so renamed or
unlinked files remain readable through their existing handles. Closing a
returned reader closes its duplicate, not the source handle.

Prefer the block form for automatic cleanup:

```crystal
source.get_read(offset) do |input|
  input.read_fully(buffer)
end
```

The block's return value is preserved and an `ensure` closes the reader even
when parsing raises. The non-block overload transfers cleanup responsibility to
the caller. Internal page-header parsing and column splicing use the block form;
`append_column_from_read` still borrows its caller-supplied IO.

## Upstream behavior and the reported proposal

The reported overflow is reproducible: the previous implementation converted
`file.size - start` to Int32 and allocated the entire tail. A sparse file with a
marker beyond Int32::MAX fails at offset zero before the fix. The new reader
reads both the first byte and the marker without that allocation.

The report's positioned `read_at` proposal also avoids whole-tail allocation,
but differs from upstream in two observable ways:

- File handle clones share the OS seek position. Reads and seeks through other
  handles can affect a reader's subsequent unbuffered reads. Callers must avoid
  interleaving/concurrent operations that depend on independent positions;
  prefetched bytes remain local to each reader's buffer.
- Seeking at or beyond EOF is allowed for files and subsequent reads return EOF.
  The explicit bounds error belongs to upstream's in-memory `Bytes` reader,
  which the port preserves separately.

The fix uses upstream's file semantics rather than the positioned proposal.
`get_bytes` retains the existing bounded `File#read_at` implementation; it does
not change the shared seek position. The upstream trait permits, but does not
require, that operation to affect existing readers.

## Verification

`spec/parquet/file/chunk_reader_spec.cr` characterizes the pinned file logic,
which has no dedicated upstream large-file unit test. It covers sparse-file
reads beyond 2 GiB, successive buffered reads, empty reads, EOF/beyond-EOF,
shared seek effects, independent handle lifetimes, unlinked source files, and
block cleanup on success and exceptions. Integration checks verify page-header
cleanup across read/peek/skip paths and parsing failures, plus column-splice
cleanup on success and failure.

Sparse allocation depends on the filesystem; run these specs with temporary
storage that supports sparse files. The external report's 9.2 GB catalog scan
and application workaround are not repository fixtures and were not rerun here.
The verified upstream pin is apache/arrow-rs 60.0.0
(`ef1fa157977633f0ba21aa921f9ef3d5c669235b`).
