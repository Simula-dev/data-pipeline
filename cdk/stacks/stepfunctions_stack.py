"""
StepFunctionsStack \u2014 Pipeline orchestration state machine.

Main flow:
  IngestData \u2192 LoadToSnowflake \u2192 RunDbtTransformation
    \u2192 MLEnabled? (Choice)
        \u251c\u2500 true:  ExportMLInput \u2192 RunBatchTransform \u2192 LoadMLPredictions \u2192 DataQualityGate
        \u2514\u2500 false:                                                   \u2192 DataQualityGate
    \u2192 DataQualityGate
    \u2192 QualityPassed? (Choice)
        \u251c\u2500 true:  NotifySuccess
        \u2514\u2500 false: NotifyFailure

Any task error is caught and routed to NotifyFailure.
"""

from aws_cdk import (
    Stack,
    Duration,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    aws_ecs as ecs,
    aws_ec2 as ec2,
    aws_lambda as _lambda,
    aws_logs as logs,
)
from constructs import Construct


class StepFunctionsStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        ingest_function: _lambda.Function,
        load_function: _lambda.Function,
        ml_export_function: _lambda.Function,
        ml_load_function: _lambda.Function,
        quality_gate_function: _lambda.Function,
        notify_function: _lambda.Function,
        dbt_cluster: ecs.Cluster,
        dbt_task_definition: ecs.FargateTaskDefinition,
        raw_bucket_name: str,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # ------------------------------------------------------------------ #
        #  STEP 1 \u2014 Ingest                                                    #
        # ------------------------------------------------------------------ #
        ingest_task = tasks.LambdaInvoke(
            self, "IngestData",
            lambda_function=ingest_function,
            payload=sfn.TaskInput.from_json_path_at("$"),
            result_path="$.ingestResult",
            retry_on_service_exceptions=True,
        )

        # ------------------------------------------------------------------ #
        #  STEP 2 \u2014 Load to Snowflake                                         #
        # ------------------------------------------------------------------ #
        load_task = tasks.LambdaInvoke(
            self, "LoadToSnowflake",
            lambda_function=load_function,
            payload=sfn.TaskInput.from_json_path_at("$"),
            result_path="$.loadResult",
            retry_on_service_exceptions=True,
        )

        # ------------------------------------------------------------------ #
        #  STEP 3 \u2014 dbt build on Fargate                                      #
        # ------------------------------------------------------------------ #
        dbt_task = tasks.EcsRunTask(
            self, "RunDbtTransformation",
            cluster=dbt_cluster,
            task_definition=dbt_task_definition,
            launch_target=tasks.EcsFargateLaunchTarget(
                platform_version=ecs.FargatePlatformVersion.LATEST
            ),
            subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS),
            assign_public_ip=False,
            container_overrides=[
                tasks.ContainerOverride(
                    container_definition=dbt_task_definition.default_container,
                    command=["dbt", "build", "--profiles-dir", "/app/dbt", "--target", "prod"],
                )
            ],
            result_path="$.dbtResult",
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
        )

        # ------------------------------------------------------------------ #
        #  STEP 4 \u2014 ML sub-flow                                               #
        # ------------------------------------------------------------------ #
        # 4a: Unload MARTS.ML_INFERENCE_INPUT to S3 as CSV
        ml_export_task = tasks.LambdaInvoke(
            self, "ExportMLInput",
            lambda_function=ml_export_function,
            payload=sfn.TaskInput.from_json_path_at("$"),
            result_path="$.mlExport",
            retry_on_service_exceptions=True,
        )

        # 4b: SageMaker Batch Transform \u2014 native Step Functions task
        #     Model name comes from $.ml_model_name in the input event (set
        #     it after training your first model; see ml/README.md).
        batch_transform_task = tasks.SageMakerCreateTransformJob(
            self, "RunBatchTransform",
            model_name=sfn.JsonPath.string_at("$.ml_model_name"),
            transform_job_name=sfn.JsonPath.string_at("$$.Execution.Name"),
            transform_input=tasks.TransformInput(
                transform_data_source=tasks.TransformDataSource(
                    s3_data_source=tasks.TransformS3DataSource(
                        s3_uri=sfn.JsonPath.string_at("$.mlExport.Payload.s3InputPrefix"),
                        s3_data_type=tasks.S3DataType.S3_PREFIX,
                    )
                ),
                content_type="text/csv",
                split_type=tasks.SplitType.LINE,
            ),
            transform_output=tasks.TransformOutput(
                s3_output_path=f"s3://{raw_bucket_name}/ml/output/",
                accept="text/csv",
            ),
            transform_resources=tasks.TransformResources(
                instance_count=1,
                instance_type=ec2.InstanceType.of(
                    ec2.InstanceClass.M5, ec2.InstanceSize.LARGE
                ),
            ),
            result_path="$.transformResult",
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
        )

        # 4c: Load predictions CSV back into MARTS.ML_PREDICTIONS
        ml_load_task = tasks.LambdaInvoke(
            self, "LoadMLPredictions",
            lambda_function=ml_load_function,
            payload=sfn.TaskInput.from_json_path_at("$"),
            result_path="$.mlLoadResult",
            retry_on_service_exceptions=True,
        )

        # ------------------------------------------------------------------ #
        #  STEP 5 \u2014 Data quality gate                                         #
        # ------------------------------------------------------------------ #
        quality_gate_task = tasks.LambdaInvoke(
            self, "DataQualityGate",
            lambda_function=quality_gate_function,
            payload=sfn.TaskInput.from_json_path_at("$"),
            result_path="$.qualityResult",
            result_selector={"passed.$": "$.Payload.passed"},
        )

        # ------------------------------------------------------------------ #
        #  STEP 6 \u2014 Notifications (success / failure terminals)               #
        # ------------------------------------------------------------------ #
        # Both terminals invoke the same notify Lambda with a different
        # `status` field. The Lambda formats a human-readable message,
        # publishes to SNS, and emits CloudWatch metrics via EMF.
        notify_success = tasks.LambdaInvoke(
            self, "NotifySuccess",
            lambda_function=notify_function,
            payload=sfn.TaskInput.from_object({
                "status": "SUCCESS",
                "executionId.$": "$$.Execution.Name",
                "executionArn.$": "$$.Execution.Id",
                "startTime.$": "$$.Execution.StartTime",
                "stateMachineName.$": "$$.StateMachine.Name",
                "region": self.region,
                "state.$": "$",
            }),
            result_path=sfn.JsonPath.DISCARD,  # don't mutate state
        )

        notify_failure = tasks.LambdaInvoke(
            self, "NotifyFailure",
            lambda_function=notify_function,
            payload=sfn.TaskInput.from_object({
                "status": "FAILURE",
                "executionId.$": "$$.Execution.Name",
                "executionArn.$": "$$.Execution.Id",
                "startTime.$": "$$.Execution.StartTime",
                "stateMachineName.$": "$$.StateMachine.Name",
                "region": self.region,
                "state.$": "$",
            }),
            result_path=sfn.JsonPath.DISCARD,
        )

        # ------------------------------------------------------------------ #
        #  Wiring order: build from the tails up so states are created       #
        #  before being referenced as transition targets.                     #
        # ------------------------------------------------------------------ #

        # Quality gate flows into the pass/fail choice
        quality_choice = sfn.Choice(self, "QualityPassed?")
        quality_choice.when(
            sfn.Condition.boolean_equals("$.qualityResult.passed", True),
            notify_success,
        )
        quality_choice.otherwise(notify_failure)
        quality_gate_task.next(quality_choice)

        # ML sub-chain terminates at the quality gate
        ml_export_task.next(batch_transform_task).next(ml_load_task).next(quality_gate_task)

        # ML enabled / disabled choice. Both branches converge on quality_gate_task.
        ml_choice = sfn.Choice(self, "MLEnabled?")
        ml_choice.when(
            sfn.Condition.boolean_equals("$.ml_enabled", True),
            ml_export_task,
        )
        ml_choice.otherwise(quality_gate_task)

        # Main chain: ingest \u2192 load \u2192 dbt \u2192 ml_choice
        definition = (
            ingest_task
            .next(load_task)
            .next(dbt_task)
            .next(ml_choice)
        )

        # Error catches on every intermediate task \u2192 failure notification
        for task in (
            ingest_task, load_task, dbt_task,
            ml_export_task, batch_transform_task, ml_load_task,
            quality_gate_task,
        ):
            task.add_catch(
                notify_failure,
                errors=["States.ALL"],
                result_path="$.errorInfo",
            )

        # ------------------------------------------------------------------ #
        #  State machine                                                      #
        # ------------------------------------------------------------------ #
        log_group = logs.LogGroup(
            self, "StateMachineLogGroup",
            log_group_name="/data-pipeline/state-machine",
            retention=logs.RetentionDays.ONE_MONTH,
        )

        self.state_machine = sfn.StateMachine(
            self, "DataPipelineStateMachine",
            state_machine_name="data-pipeline-orchestrator",
            definition_body=sfn.DefinitionBody.from_chainable(definition),
            timeout=Duration.hours(3),
            logs=sfn.LogOptions(
                destination=log_group,
                level=sfn.LogLevel.ALL,
                include_execution_data=True,
            ),
            tracing_enabled=True,
        )
