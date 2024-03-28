import json

from aws_cdk import (
    Duration,
    RemovalPolicy,
    Stack,
    aws_scheduler_alpha as scheduler_alpha,  # need aws-cdk.aws-scheduler-alpha installed
    aws_scheduler_targets_alpha as targets_alpha,  # need aws-cdk.aws-scheduler-targets-alpha installed
    aws_iam as iam,
    aws_logs as logs,
    aws_lambda as _lambda,
    aws_pipes as pipes,
    aws_sqs as sqs,
)
from constructs import Construct


class EventbridgePipeAndSchedulerStack(Stack):
    def __init__(
        self, scope: Construct, construct_id: str, environment: dict, **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        self.source_dlq = sqs.Queue(
            self,
            "SourceDlq",
            queue_name="source-dlq",  # hard coded
            removal_policy=RemovalPolicy.DESTROY,
        )
        self.source_queue = sqs.Queue(
            self,
            "SourceQueue",
            queue_name="source-queue",  # hard coded
            visibility_timeout=Duration.seconds(environment["SQS_VISIBILITY_TIME_SECONDS"]),
            dead_letter_queue=sqs.DeadLetterQueue(
                max_receive_count=environment["SQS_MAX_RECEIVE_COUNT"],
                queue=self.source_dlq,
            ),
            removal_policy=RemovalPolicy.DESTROY,
        )
        self.target_queue = sqs.Queue(
            self,
            "TargetQueue",
            queue_name="target-queue",  # hard coded
            removal_policy=RemovalPolicy.DESTROY,
        )

        self.publish_to_sqs_lambda = _lambda.Function(
            self,
            "PublishToSqsLambda",
            function_name="publish-to-sqs",  # hard coded
            handler="handler.lambda_handler",
            memory_size=128,
            timeout=Duration.seconds(60),  # hard coded
            runtime=_lambda.Runtime.PYTHON_3_10,
            code=_lambda.Code.from_asset(
                "lambda_code/publish_to_sqs_lambda",
                exclude=[".venv/*"],
            ),
        )
        self.enrich_pipe_event_lambda = _lambda.Function(
            self,
            "EnrichPipeEventLambda",
            function_name="enrich-pipe-event",  # hard coded
            handler="handler.lambda_handler",
            memory_size=128,
            timeout=Duration.seconds(1),  # should be instantaneous
            runtime=_lambda.Runtime.PYTHON_3_10,
            environment={
                "ENRICHMENT_LAMBDA_FAILURE_RATE": json.dumps(
                    environment["ENRICHMENT_LAMBDA_FAILURE_RATE"]
                )
            },
            code=_lambda.Code.from_asset(
                "lambda_code/enrich_pipe_event_lambda",
                exclude=[".venv/*"],
            ),
        )

        # connect AWS resources together
        self.source_queue.grant_send_messages(self.publish_to_sqs_lambda)
        self.publish_to_sqs_lambda.add_environment(
            key="SQS_NAME", value=self.source_queue.queue_name
        )
        target = targets_alpha.LambdaInvoke(
            func=self.publish_to_sqs_lambda,
            input=scheduler_alpha.ScheduleTargetInput.from_object(
                {"payload": "useful"},
            ),
            # retry_attempts=None, role=None,
        )
        self.schedule = scheduler_alpha.Schedule(
            self,
            "Schedule",
            schedule_name="every-minute",  # hard coded
            schedule=scheduler_alpha.ScheduleExpression.rate(Duration.minutes(1)),
            target=target,
            # group=...,
        )

        self.pipe_role = iam.Role(
            self,
            "pipe-role",
            role_name="pipe-role",  # hard coded
            assumed_by=iam.ServicePrincipal("pipes.amazonaws.com"),
        )
        self.source_queue.grant_consume_messages(self.pipe_role)
        self.enrich_pipe_event_lambda.grant_invoke(self.pipe_role)
        self.target_queue.grant_send_messages(self.pipe_role)
        log_group = logs.LogGroup(
            self,
            "PipesLogGroup",
            log_group_name="PipesLogGroup",
            removal_policy=RemovalPolicy.DESTROY,
        )
        self.pipe = pipes.CfnPipe(
            self,
            "EventbridgePipe",
            name="eventbridge-pipe",
            role_arn=self.pipe_role.role_arn,
            source=self.source_queue.queue_arn,
            source_parameters=pipes.CfnPipe.PipeSourceParametersProperty(
                sqs_queue_parameters=pipes.CfnPipe.PipeSourceSqsQueueParametersProperty(
                    batch_size=environment["EVENTBRIDGE_PIPE_BATCH_SIZE"],
                    maximum_batching_window_in_seconds=environment["EVENTBRIDGE_PIPE_BATCHING_WINDOW_SECONDS"],
                    # https://docs.aws.amazon.com/eventbridge/latest/userguide/eb-pipes-sqs.html
                    # Note: The batching window needs to be shorter than
                    # visibility timeout (and Lambda processing time) as messages
                    # will be pulled from SQS but not delivered to Lambda until
                    # batching window time, so messages will increment receive count
                    # and then fall into DLQ.
                    # Another note: Eventbridge Pipe long polls SQS. It appears that
                    # Eventbridge pipe might wait up to 20 seconds, so batching window
                    # isn't really 0 seconds even if you set it to 0 seconds.
                    # Another note: SQS visibility needs to be at least 6 times the
                    # combined runtime of the pipe enrichment and target components.
                    # Or else messages might inadvertently fall into the DLQ.
                    # Most of the time, when batch size is 1 and batch window is 0 or
                    # 1 seconds, message from SQS to Lambda is instantaneous (ie <1 second),
                    # but occasionally it will take a few seconds (more than 20 seconds)
                    # due to Eventbridge pipe's real batch window.
                ),
                # filter_criteria=...,
            ),
            enrichment=self.enrich_pipe_event_lambda.function_arn,
            target=self.target_queue.queue_arn,
            # target_parameters=...,
            log_configuration=pipes.CfnPipe.PipeLogConfigurationProperty(
                cloudwatch_logs_log_destination=pipes.CfnPipe.CloudwatchLogsLogDestinationProperty(
                    log_group_arn=log_group.log_group_arn
                ),
                level="INFO",
            ),
        )
        self.pipe.apply_removal_policy(RemovalPolicy.DESTROY)

        self.schedule.node.add_dependency(
            self.pipe  # deploy Eventbridge Pipe before Scheduler
        )
