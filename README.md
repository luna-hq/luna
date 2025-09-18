> [!CAUTION]
> **Alpha-level software.** Development is actively ongoing. API will most likely change.

[![main](https://github.com/flowerinthenight/luna/actions/workflows/main.yml/badge.svg)](https://github.com/flowerinthenight/luna/actions/workflows/main.yml)

## Overview

**Luna** is an open-source, **in-memory** (OLAP) SQL server for object storage data. Built on [DuckDB](https://duckdb.org/) and [Apache Arrow](https://arrow.apache.org/), it supports  [S3](https://aws.amazon.com/s3/), [GCS](https://cloud.google.com/storage?hl=en), and local filesystem data sources; and `CSV`, `JSON`, and `PARQUET` data types. It's in-memory by default but it supports spilling to disk for larger-than-memory workloads.

> [!NOTE]
> - Luna's development is supported (and funded) by [Alphaus, Inc.](https://www.alphaus.cloud/en/) as it's also being used internally.
> - At the moment, Luna is tested (and expected) to run on a single machine, although support for distributed clusters is part of the goal in the (near) future.

## API specs

A Luna process maintains a single, in-memory database that can be configured through its TCP-based API which is exposed, by default, at port `7688` (can be changed through the `--api-host-port` flag). Requests use a variant of Redis' [RESP](https://redis.io/docs/latest/develop/reference/protocol-spec/) spec, specifically, the [Bulk strings](https://redis.io/docs/latest/develop/reference/protocol-spec/#bulk-strings) representation. Responses, on the other hand, consist of a stream of `RecordBatch` messages, utilizing Arrow's [IPC format](https://arrow.apache.org/docs/format/Columnar.html#format-ipc). This also applies to error messages.

#### [Request]

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

- First 2 bytes - prefix as command type. It can either be `x:` for execute, or `q:` for query statements.
- Remaining bytes - the actual command, mostly in SQL form.

For example, to load CSV files from cloud storage, we will have the following requests:

```sql
-- Setup credentials for GCS access:
$79\n\nx:CREATE OR REPLACE SECRET (TYPE gcs, KEY_ID 'some-key', SECRET 'some-secret');\n\n

-- or S3 access:
$116\n\nx:CREATE OR REPLACE SECRET (
TYPE s3, PROVIDER config, KEY_ID 'some-key', SECRET 'some-secret', REGION 'us-east-1');\n\n

-- Then import some CSV files from GCS:
$138\n\nx:CREATE TABLE tmpcur AS FROM read_csv('gs://bucket/987368816909_2025-08*.csv',
header = true,
union_by_name = true,
files_to_sniff = -1);\n\n

-- or from S3:
$138\n\nx:CREATE TABLE tmpcur AS FROM read_csv('s3://bucket/987368816909_2025-08*.csv',
header = true,
union_by_name = true,
files_to_sniff = -1);\n\n

-- Describe the created table:
$18\n\nq:DESCRIBE tmpcur;\n\n

-- Query data:
$39\n\nq:SELECT uuid, date, payer FROM tmpcur;\n\n
```

#### [Response]

- Error messages will have two fields/columns: `input`, and `error`, with row values containing the input and the resulting error.

```
schema:
  fields: 2
    - input: type=utf8
    - error: type=utf8

record:
  schema:
  fields: 2
    - input: type=utf8
    - error: type=utf8
  rows: 1
  col[0][input]: ["DESCRIBE me;"]
  col[1][error]: ["Catalog Error: Table with name me does not exist!..."]

+--------------+--------------------------------------------------------+
| input        | error                                                  |
+--------------+--------------------------------------------------------+
| DESCRIBE me; | Catalog Error: Table with name me does not exist!      |
|              | Did you mean \"pg_namespace\"?\n\nLINE 1: describe me; |
+--------------+--------------------------------------------------------+
```

- Success messages use the same format with `OK` as the value.

```
+---------------------------------------------------------------+-------+
| input                                                         | error |
+---------------------------------------------------------------+-------+
| CREATE TABLE customers AS FROM read_csv('s3://bucket/custo... | OK    |
+---------------------------------------------------------------+-------+
```

- Otherwise, the result will depend on the query result itself.

```
SELECT CustomerId, Email FROM customers;

schema:
  fields: 2
    - CustomerId: type=utf8, nullable
    - Email: type=utf8, nullable

record:
  schema:
  fields: 2
    - CustomerId: type=utf8, nullable
    - Email: type=utf8, nullable
  rows: 1000
  col[0][CustomerId]: ["dE014d010c7ab0c" "d794Dd48988d2ac" ... ]
  col[1][Email]: ["marieyates@gomez-spencer.info" "justincurtis@pierce.org" ...]

+-----------------+-------------------------------+
| CustomerId      | Email                         |
+-----------------+-------------------------------+
| dE014d010c7ab0c | marieyates@gomez-spencer.info |
| d794Dd48988d2ac | justincurtis@pierce.org       |
| ...             | ...                           |
+-----------------+-------------------------------+
```

## Build

The following guide uses [`lunactl`](https://github.com/flowerinthenight/lunactl/), a test cmdline for Luna. A [sample CSV](./testdata/) (copied from [sample-csv-files](https://github.com/datablist/sample-csv-files)) is also included to test filesystem-based data imports.

```sh
# Build binary:
$ cargo build

# Run on default port 7688:
$ RUST_LOG=info ./target/debug/luna

# Install lunactl for testing:
$ brew install flowerinthenight/tap/lunactl

# Import local CSV to luna (newline is for readability only):
$ lunactl -type 'x:' -p "CREATE TABLE customers AS FROM read_csv(
'/path/to/luna/testdata/customers-1000.csv', header = true, files_to_sniff = -1);"

# Describe our newly-created table:
$ lunactl -p "DESCRIBE customers;"

# Query some data (#1):
$ lunactl -p "SELECT CustomerId, Email FROM customers LIMIT 5;"

# Query some data (#2):
$ lunactl -p "SELECT count(Index) FROM customers;" 
```

## Runing on a GCP MIG

A sample cloud-init [startup script](./startup-gcp-mig.sh) is provided for spinning up a [Managed Instance Group](https://cloud.google.com/compute/docs/instance-groups#managed_instance_groups) with Luna running as a systemd service.

```bash
# Create a launch template. Update --service-account with
# your own service account. You can update the --machine-type.
$ gcloud compute instance-templates create luna-tmpl \
  --machine-type e2-micro \
  --service-account=name@project.iam.gserviceaccount.com \
  --scopes=cloud-platform \
  --metadata=startup-script=''"$(cat startup-gcp-mig.sh)"''

# Create the MIG. Update {target-region} with actual value.
$ gcloud compute instance-groups managed create luna-mig \
  --template luna-tmpl --size 1 --region {target-region}

# Let's use 'https://github.com/flowerinthenight/g-ssh-cmd'
# to tail the VM logs. This is optional.
$ brew install flowerinthenight/tap/g-ssh-cmd

# Assuming your 'gcloud' cmdline is configured properly.
$ g-ssh-cmd mig luna-mig 'journalctl -f' --project {project}
```

## Runing on an AWS ASG

A sample cloud-init [startup script](./startup-aws-asg.sh) is provided for spinning up an [Auto Scaling Group](https://docs.aws.amazon.com/autoscaling/ec2/userguide/auto-scaling-groups.html) with Luna running as a systemd service.

```sh
# Create a launch template. ImageId here is Amazon Linux, default VPC.
# (Added newlines for readability. Might not run when copied as is.)
# Replace 'keyName' with your own keypair; update 'InstanceType'.
$ aws ec2 create-launch-template \
  --launch-template-name luna-lt \
  --version-description version1 \
  --launch-template-data '
  {
    "UserData":"'"$(cat startup-aws-asg.sh | base64 -w 0)"'",
    "ImageId":"ami-08f0737412a47a5ed",
    "InstanceType":"t2.micro",
    "KeyName":"keyName"
  }'

# Create the single-zone ASG; update {target-zone} with actual value.
$ aws autoscaling create-auto-scaling-group \
  --auto-scaling-group-name luna-asg \
  --launch-template LaunchTemplateName=luna-lt,Version='1' \
  --min-size 1 \
  --max-size 1 \
  --tags Key=Name,Value=luna-asg \
  --availability-zones {target-zone}

# Let's use [https://github.com/flowerinthenight/g-ssh-cmd]
# to tail the VM logs. This is optional.
$ brew install flowerinthenight/tap/g-ssh-cmd

# Assuming your 'aws' cmdline is configured properly.
$ g-ssh-cmd asg luna-asg 'journalctl -f' --key keyName.pem
```

## Todo

- [ ] Client SDK (and samples) for mainstream programming languages
- [ ] Authentication
- [ ] Dedicated documentation site
- [ ] Deployment guides and samples
- [x] Release pipeline
- [ ] Proper testing codes
- [ ] Additional testdata for object storage, local, and different file/db types
- [ ] Distributed/cluster support
