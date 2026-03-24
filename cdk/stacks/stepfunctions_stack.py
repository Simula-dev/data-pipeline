"""
StepFunctionsStack — Pipeline orchestration state machine.

State machine flow:
  IngestData → LoadToSnowflake → RunDbtTransformation → RunMLInference
  → DataQualityGate → [Pass: NotifySuccess | Fail: NotifyFailure]
"""

from aws_cdk import (
    Stack,
    Duration,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as tasks,
    aws_ecs as ecs,
    aws_lambda as _lambda,
    aws_sns as sns,
    aws_iam as iam,
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
        quality_gate_function: _lambda.Function,
        notify_topic: sns.Topic,
        dbt_cluster: ecs.Cluster,
        dbt_task_definition: ecs.FargateTaskDefinition,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # ------------------------------------------------------------------ #
        #  STEP 1 — Ingest raw data into S3                                   #
        # ------------------------------------------------------------------ #
        ingest_task = tasks.LambdaInvoke(
            self,
            "IngestData",
            lambda_function=ingest_function,
            payload=sfn.TaskInput.from_json_path_at("$"),
            result_path="$.ingestResult",
            retry_on_service_exceptions=True,
        )

        # ------------------------------------------------------------------ #
        #  STEP 2 — Load S3 data into Snowflake raw schema                   #
        # ------------------------------------------------------------------ #
        load_task = tasks.LambdaInvoke(
            self,
            "LoadToSnowflake",
            lambda_function=load_function,
            payload=sfn.TaskInput.from_json_path_at("$"),
            result_path="$.loadResult",
            retry_on_service_exceptions=True,
        )

        # ------------------------------------------------------------------ #
        #  STEP 3 — Run dbt transformations on Fargate                        #
        # ------------------------------------------------------------------ #
        dbt_task = tasks.EcsRunTask(
            self,
            "RunDbtTransformation",
            cluster=dbt_cluster,
            task_definition=dbt_task_definition,
            launch_target=tasks.EcsFargateLaunchTarget(
                platform_version=ecs.FargatePlatformVersion.LATEST
            ),
            # Override command to also run dbt tests after models
            container_overrides=[
                tasks.ContainerOverride(
                    container_definition=dbt_task_definition.default_container,
                    command=["sh", "-c", "dbt run && dbt test"],
                )
            ],
            result_path="$.dbtResult",
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
        )

        # ------------------------------------------------------------------ #
        #  STEP 4 — ML inference via SageMaker endpoint (Lambda wrapper)      #
        # ------------------------------------------------------------------ #
        # Placeholder Lambda invoke — swap for tasks.SageMakerInvokeEndpoint
        # once your endpoint is deployed.
        ml_inference_task = tasks.LambdaInvoke(
            self,
            "RunMLInference",
            lambda_function=self._placeholder_lambda("MLInferencePlaceholder"),
            payload=sfn.TaskInput.from_json_path_at("$"),
            result_path="$.mlResult",
            retry_on_service_exceptions=True,
        )

        # ------------------------------------------------------------------ #
        #  STEP 5 — Data quality gate: checks row counts / null rates         #
        # ------------------------------------------------------------------ #
        quality_gate_task = tasks.LambdaInvoke(
            self,
            "DataQualityGate",
            lambda_function=quality_gate_function,
            payload=sfn.TaskInput.from_json_path_at("$"),
            result_path="$.qualityResult",
            result_selector={"passed.$": "$.Payload.passed"},
        )

        # ------------------------------------------------------------------ #
        #  STEP 6a — Notify success                                           #
        # ------------------------------------------------------------------ #
        notify_success = tasks.SnsPublish(
            self,
            "NotifySuccess",
            topic=notify_topic,
            message=sfn.TaskInput.from_object({
                "status": "SUCCESS",
                "pipeline": "data-pipeline",
                "detail.$": "$",
            }),
        )

        # ------------------------------------------------------------------ #
        #  STEP 6b — Notify failure                                           #
        # ------------------------------------------------------------------ #
        notify_failure = tasks.SnsPublish(
            self,
            "NotifyFailure",
            topic=notify_topic,
            message=sfn.TaskInput.from_object({
                "status": "FAILURE",
                "pipeline": "data-pipeline",
                "detail.$": "$",
            }),
        )

        # ------------------------------------------------------------------ #
        #  Quality gate branch: pass → success, fail → failure notification   #
        # ------------------------------------------------------------------ #
        quality_choice = sfn.Choice(self, "QualityPassed?")
        quality_choice.when(
            sfn.Condition.boolean_equals("$.qualityResult.passed", True),
            notify_success,
        )
        quality_choice.otherwise(notify_failure)

        # ------------------------------------------------------------------ #
        #  Catch any unhandled errors and route to failure notification        #
        # ------------------------------------------------------------------ #
        pipeline_catch = sfn.TaskStateBase.add_catch if False else None  # type: ignore

        # ------------------------------------------------------------------ #
        #  Wire the full state machine chain                                  #
        # ------------------------------------------------------------------ #
        definition = (
            ingest_task
            .next(load_task)
            .next(dbt_task)
            .next(ml_inference_task)
            .next(quality_gate_task)
            .next(quality_choice)
        )

        # Add error catch on each task → route to failure notification
        for task in [ingest_task, load_task, ml_inference_task, quality_gate_task]:
            task.add_catch(
                notify_failure,
                errors=["States.ALL"],
                result_path="$.errorInfo",
            )

        # State machine log group
        log_group = logs.LogGroup(
            self,
            "StateMachineLogGroup",
            log_group_name="/data-pipeline/state-machine",
            retention=logs.RetentionDays.ONE_MONTH,
        )

        self.state_machine = sfn.StateMachine(
            self,
            "DataPipelineStateMachine",
            state_machine_name="data-pipeline-orchestrator",
            definition_body=sfn.DefinitionBody.from_chainable(definition),
            timeout=Duration.hours(2),
            logs=sfn.LogOptions(
                destination=log_group,
                level=sfn.LogLevel.ALL,
                include_execution_data=True,
            ),
            tracing_enabled=True,
        )

    def _placeholder_lambda(self, name: str) -> _lambda.Function:
        """
        Inline placeholder Lambda used for steps not yet implemented.
        Replace with real function references as each step is built out.
        """
        return _lambda.Function(
            self,
            name,
            runtime=_lambda.Runtime.PYTHON_3_12,
            handler="index.handler",
            code=_lambda.Code.from_inline(
                "def handler(event, context):\n"
                "    print('Placeholder — implement me')\n"
                "    return {'statusCode': 200, 'passed': True}\n"
            ),
            timeout=Duration.seconds(30),
        )
