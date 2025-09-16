> [!CAUTION]
> **Alpha-level software.** Development is actively ongoing. API will most likely change. **Use with caution.**

[![main](https://github.com/flowerinthenight/luna/actions/workflows/main.yml/badge.svg)](https://github.com/flowerinthenight/luna/actions/workflows/main.yml)

## Overview

**Luna** is an open-source, **in-memory** (supports spilling to disk for larger-than-memory workloads) SQL interface to object storage data. Built on top of [DuckDB](https://duckdb.org/) and [Apache Arrow](https://arrow.apache.org/), it supports  [S3](https://aws.amazon.com/s3/), [GCS](https://cloud.google.com/storage?hl=en), and local filesystem data sources; and `CSV`, `JSON`, and `PARQUET` data types.

> [!NOTE]
> - Luna's development is supported (and funded) by [Alphaus, Inc.](https://www.alphaus.cloud/en/) as it's also being used internally.
> - Luna is tested (and expected) to run on a single machine, although a private fork is being developed to support distributed clusters in the future.

## API Specs

A `luna` process maintains a single, in-memory database that can be configured through its TCP-based API which is exposed, by default, at port `7688` (can be changed through the `--api-host-port` flag). Requests use a variant of Redis' [RESP](https://redis.io/docs/latest/develop/reference/protocol-spec/) spec, specifically, the [Bulk strings](https://redis.io/docs/latest/develop/reference/protocol-spec/#bulk-strings) representation. Responses, on the other hand, consist of a stream of `RecordBatch` messages (schema included), utilizing Arrow's [IPC format](https://arrow.apache.org/docs/format/Columnar.html#format-ipc). This also applies to error messages.

Details to be added.

## Build

```sh
# Build binary:
$ cargo build

# Sample run:
$ RUST_LOG=info ./target/debug/luna
```

See [lunactl](https://github.com/flowerinthenight/lunactl/) for the cmdline.
