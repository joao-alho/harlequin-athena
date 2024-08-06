# harlequin-athena

This repo provides a Harlequin adapter for Athena.


## Installation

`harlequin-athena` depends on `harlequin`, so installing this package will also install Harlequin.

### Using pip

To install this adapter into an activated virtual environment:

```bash
pip install harlequin-athena
```

### Using poetry
```bash
poetry add harlequin-athena
```

### Using pipx

If you do not already have Harlequin installed:
```bash
pip install harlequin-athena
```

If you would like to add the Athena adapter to an existing Harlequin installation:
```bash
pipx inject harlequin harlequin-athena
```

### As an extra

Alternatively, you can install Harlequin with the `athena` extra:

```bash
pip install harlequin[athena]
```
```bash
poetry add harlequin[athena]
```
```bash
pipx install harlequin[athena]
```

## Usage and configuration

This adapter uses [pyathena](https://github.com/laughingman7743/PyAthena.git) as the client for Athena, it will use PyArrow as the results cursor.

>[!Important]
> There's currently an issue when setting the limit for a query and running a query which returns duplicated columns.
> The duplicated columns will not be properly created, only the last added column will show in the result set.
> This is also an issue when running `fetchall()` however, this has been easily fixed by using `ArrowCursor.as_arrow()`

Config options are:
* `workgroup`: Athena workgroup to run queries on. If this is specified and the workgroup is set to override client settings then `s3_staging_dir` is ignored. This is the prefered option.
* `s3_staging_dir`: The S3 bucket where Athena stores query results and other metadata.
* `result_reuse_enable`: Whether to cache results or not. The query string must match exactly for this to work. Enabled by default.
* `result_reuse_minutes`: How long to cache results for in minutes. 60 minutes by default.
* `unload`: Whether to use the unload option in PyAthena. This will convert `SELECT` statements to unload statements which will persist the results to `s3_staging_dir` as Snappy compressed Parquet files. It will speed up queries, at the cost of higher overal AWS costs.
* `region_name`: AWS region name.
* `aws_access_key_id`: AWS access key id. This is not recommended, use environment variables or credentials file instead.
* `aws_secret_access_key`: AWS secret access key. This is not recommended, use environment variables or credentials file instead.
* `aws_session_token`: AWS session token when using assumed roles. This is not recommended, use environment variables or credentials file instead.

### AWS Credentials

While you can pass credentials on the options when running harlequin with this adapter, you should instead setup credentials in a way that lets boto3's credentials provider chain find them, see the [AWS docs](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html#configuring-credentials)

## Testing

An AWS account is needed to run the tests, the account should have the following AWS resources:
1. A S3 bucket to store Athena query results
2. Optional: an Athena workgroup.

Depends on the following environment variables:

```bash
export AWS_ATHENA_WORKGROUP=my-workgroup
export AWS_ATHENA_S3_STAGING_DIR=s3://your_s3_bucket/path
export AWS_DEFAULT_REGION=eu-central-1
```

Credentials for AWS should also be set as environment variables:

```bash
export AWS_ACCESS_KEY_ID="<your_access_key>"
export AWS_SECRET_ACCESS_KEY="<your_secret_access_key>"
# optionally you can also set a session token
# export AWS_SESSION_TOKEN="<your_token>"
```

