> [!CAUTION]
> Alpha-level software. Development is actively ongoing. APIs most likely will change.

[![main](https://github.com/flowerinthenight/luna/actions/workflows/main.yml/badge.svg)](https://github.com/flowerinthenight/luna/actions/workflows/main.yml)

## Overview

`luna` is an open-source, **in-memory** SQL interface to your object storage data. Built on top of [DuckDB](https://duckdb.org/) and [Apache Arrow](https://arrow.apache.org/), it supports  [S3](https://aws.amazon.com/s3/), [GCS](https://cloud.google.com/storage?hl=en), and local filesystem data sources; and `CSV`, `JSON`, and `PARQUET` data types.

> [!NOTE]
> Development is supported (and funded) by [Alphaus, Inc.](https://www.alphaus.cloud/en/) as it's also being used internally.

```sh
# Sample run:
$ RUST_LOG=info ./target/debug/luna
```

See [lunactl](https://github.com/flowerinthenight/lunactl/) for the cmdline.
