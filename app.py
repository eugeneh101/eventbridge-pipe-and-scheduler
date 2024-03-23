import aws_cdk as cdk

from eventbridge_pipe_and_scheduler import EventbridgePipeAndSchedulerStack


app = cdk.App()
environment = app.node.try_get_context("environment")
env = cdk.Environment(region=environment["AWS_REGION"])
EventbridgePipeAndSchedulerStack(
    app,
    "EventbridgePipeAndSchedulerStack",
    environment=environment,
    env=env,
)
app.synth()
