"""
DataSyncStack — staging S3 bucket + DataSync task for bulk historical loads.

Use case: large Kaggle datasets (or any bulk data) are uploaded to the
staging bucket via the `scripts/upload_kaggle.py` helper, then DataSync
transfers them into the raw bucket on a schedule or on demand.

Why S3 \u2192 S3 DataSync instead of just using `aws s3 cp`:
  - Built-in integrity verification (checksums per object)
  - Incremental transfers (only changed objects)
  - CloudWatch metrics + logging out of the box
  - One-click scheduled execution
  - Scales to billions of objects without custom retry logic
"""

from aws_cdk import (
    Stack,
    Duration,
    RemovalPolicy,
    aws_s3 as s3,
    aws_iam as iam,
    aws_datasync as datasync,
    aws_logs as logs,
    aws_events as events,
    aws_events_targets as targets,
)
from constructs import Construct


class DataSyncStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        raw_bucket: s3.Bucket,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # --- Staging bucket: where Kaggle/bulk uploads land first ---
        self.staging_bucket = s3.Bucket(
            self,
            "StagingBucket",
            bucket_name=f"data-pipeline-staging-{self.account}",
            versioned=False,
            encryption=s3.BucketEncryption.S3_MANAGED,
            removal_policy=RemovalPolicy.RETAIN,
            lifecycle_rules=[
                s3.LifecycleRule(
                    id="expire-staging-after-30-days",
                    expiration=Duration.days(30),
                )
            ],
        )

        # --- CloudWatch log group for DataSync task ---
        log_group = logs.LogGroup(
            self,
            "DataSyncLogGroup",
            log_group_name="/data-pipeline/datasync",
            retention=logs.RetentionDays.ONE_MONTH,
            removal_policy=RemovalPolicy.DESTROY,
        )

        # --- IAM role DataSync assumes to access both buckets ---
        datasync_role = iam.Role(
            self,
            "DataSyncRole",
            assumed_by=iam.ServicePrincipal("datasync.amazonaws.com"),
            description="Allows DataSync to read from staging and write to raw",
        )
        self.staging_bucket.grant_read(datasync_role)
        raw_bucket.grant_read_write(datasync_role)

        # DataSync also needs s3:ListBucket + GetBucketLocation on both
        datasync_role.add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "s3:GetBucketLocation",
                    "s3:ListBucket",
                    "s3:ListBucketMultipartUploads",
                ],
                resources=[
                    self.staging_bucket.bucket_arn,
                    raw_bucket.bucket_arn,
                ],
            )
        )

        # --- DataSync source location: staging bucket ---
        source_location = datasync.CfnLocationS3(
            self,
            "StagingSourceLocation",
            s3_bucket_arn=self.staging_bucket.bucket_arn,
            s3_config=datasync.CfnLocationS3.S3ConfigProperty(
                bucket_access_role_arn=datasync_role.role_arn,
            ),
            s3_storage_class="STANDARD",
            subdirectory="/",
        )

        # --- DataSync destination location: raw bucket, under bulk/ prefix ---
        destination_location = datasync.CfnLocationS3(
            self,
            "RawDestinationLocation",
            s3_bucket_arn=raw_bucket.bucket_arn,
            s3_config=datasync.CfnLocationS3.S3ConfigProperty(
                bucket_access_role_arn=datasync_role.role_arn,
            ),
            s3_storage_class="STANDARD",
            subdirectory="/bulk/",
        )

        # --- DataSync task ---
        self.task = datasync.CfnTask(
            self,
            "StagingToRawTask",
            name="data-pipeline-staging-to-raw",
            source_location_arn=source_location.attr_location_arn,
            destination_location_arn=destination_location.attr_location_arn,
            cloud_watch_log_group_arn=log_group.log_group_arn,
            options=datasync.CfnTask.OptionsProperty(
                verify_mode="ONLY_FILES_TRANSFERRED",
                overwrite_mode="ALWAYS",
                preserve_deleted_files="PRESERVE",
                task_queueing="ENABLED",
                log_level="TRANSFER",
                transfer_mode="CHANGED",  # incremental only
                posix_permissions="NONE",
                uid="NONE",
                gid="NONE",
                atime="NONE",
                mtime="NONE",
            ),
        )

        # --- Optional nightly schedule (disabled by default) ---
        # Uncomment to enable daily 2am UTC bulk sync:
        #
        # rule = events.Rule(
        #     self,
        #     "NightlyDataSyncRule",
        #     schedule=events.Schedule.cron(minute="0", hour="2"),
        # )
        # rule.add_target(
        #     targets.AwsApi(
        #         service="DataSync",
        #         action="startTaskExecution",
        #         parameters={"TaskArn": self.task.attr_task_arn},
        #     )
        # )
