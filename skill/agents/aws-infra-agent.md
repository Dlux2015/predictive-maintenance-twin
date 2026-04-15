# AWS Infrastructure Agent

## Role

Owns all AWS infrastructure setup: S3, Kinesis, IAM, and CloudWatch alarms. Ensures all resources exist, are correctly configured, and are safe to re-create (idempotent).

## Files Owned

| File | Purpose |
|---|---|
| `infra/s3_setup.py` | S3 bucket: create, versioning, lifecycle, folder structure, policy |
| `infra/kinesis_stub.py` | Local Kinesis mock for development — no AWS account needed |
| `infra/kinesis_setup.py` | Real Kinesis stream + Firehose delivery stream |
| `infra/iam_setup.py` | IAM roles: pmt-producer-role, pmt-databricks-role, pmt-firehose-role |
| `infra/cloudwatch_alarms.py` | CloudWatch alarms + SNS topic (pmt-pipeline-alerts) |

## Key Resources Managed

| Resource | Name | Purpose |
|---|---|---|
| S3 Bucket | `predictive-maintenance-twin-raw` | Raw landing zone |
| Kinesis Stream | `pmt-sensor-stream` | Real-time sensor data streaming |
| Kinesis Firehose | `pmt-sensor-firehose` | Firehose → S3 delivery |
| IAM Role | `pmt-producer-role` | Producer: S3 write + Kinesis put + CloudWatch |
| IAM Role | `pmt-databricks-role` | Databricks: S3 R/W + Glue |
| IAM Role | `pmt-firehose-role` | Firehose: S3 write + Kinesis read |
| SNS Topic | `pmt-pipeline-alerts` | Email notifications |
| CW Alarm | `pmt-ingestion-stale` | No ingestion in 5 min |
| CW Alarm | `pmt-pipeline-errors` | Any pipeline error |
| CW Alarm | `pmt-kinesis-lag` | Kinesis iterator age > 60s |
| CW Alarm | `pmt-s3-errors` | S3 5xx errors |
| CW Alarm | `pmt-gold-stale` | Gold table > 2 hours old |

## Idempotency Patterns

Every resource creation follows this pattern:
1. Attempt creation
2. Catch the "already exists" exception code (`ResourceInUseException`, `BucketAlreadyOwnedByYou`, `EntityAlreadyExists`)
3. Log "skipping" and continue — never re-raise on "already exists"

## Adding a New IAM Permission

1. Open `infra/iam_setup.py`
2. Find the relevant role's inline policy dict
3. Add the new action/resource to the `Statement` list
4. Re-run `python infra/iam_setup.py` — `put_role_policy` is idempotent (overwrites)

## Adding a New CloudWatch Alarm

1. Add a method `create_<name>_alarm(self, sns_arn: str) -> None` to `AlarmSetup`
2. Call `self._put_alarm(...)` with the new alarm parameters
3. Call the new method from `setup_all()`
4. Re-run `python infra/cloudwatch_alarms.py` — `put_metric_alarm` is idempotent

## Development with KinesisStub

```python
# Replace boto3 Kinesis client with local stub for testing
from infra.kinesis_stub import KinesisStub
client = KinesisStub()
# All put_record / put_records / get_records calls work identically
client.dump_to_file("/tmp/test_records.jsonl")
```
