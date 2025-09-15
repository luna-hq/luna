> [!CAUTION]
> Alpha-level software. Development is actively ongoing. APIs most likely will change.

[![main](https://github.com/flowerinthenight/luna/actions/workflows/main.yml/badge.svg)](https://github.com/flowerinthenight/luna/actions/workflows/main.yml)

## Overview

`luna` is an open-source, in-memory SQL interface to your object storage data. Think of it as an alternative to Amazon Athena. Built on top of [DuckDB](https://duckdb.org/) and [Apache Arrow](https://arrow.apache.org/), it supports data sources such as [S3](https://aws.amazon.com/s3/), [GCS](https://cloud.google.com/storage?hl=en), and local filesystem; and data types such as `CSV`, `JSON`, `PARQUET`, etc.

```sh
# Sample run:
$ RUST_LOG=info ./target/debug/luna
```

See [lunactl](https://github.com/flowerinthenight/lunactl/) for the cmdline.
