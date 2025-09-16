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

Requests are encoded as follows:

```
$<length>\r\n<data>\r\n
```

- The dollar sign ($) as the first byte.
- One or more decimal digits (0..9) as the string's length, in bytes, as an unsigned, base-10 value.
- The CRLF terminator.
- The data.
- A final CRLF.

The `<data>` section is further broken down as follows:

- First 2 bytes - prefix as command type. It can either be `x:` for execute, and `q:` for query statements.
- Remaining bytes - the actual command, mostly in SQL form.

For example, to load CSV files from GCS, we will have the following requests:

```sql
-- Setup credentials for GCS access:
$79\n\nx:CREATE OR REPLACE SECRET (TYPE gcs, KEY_ID 'some-key', SECRET 'some-secret');\n\n

-- Then import some CSV files to a table:
$138\n\nx:CREATE TABLE tmpcur AS FROM read_csv('gs://bucket/987368816909_2025-08*.csv',
header = true,
union_by_name = true,
files_to_sniff = -1);\n\n

-- Describe the created table:
$18\n\nq:DESCRIBE tmpcur;\n\n

-- Query data:
$39\n\nq:SELECT uuid, date, payer FROM tmpcur;\n\n
```

## Build

```sh
# Build binary:
$ cargo build

# Sample run:
$ RUST_LOG=info ./target/debug/luna
```

See [lunactl](https://github.com/flowerinthenight/lunactl/) for the cmdline.
