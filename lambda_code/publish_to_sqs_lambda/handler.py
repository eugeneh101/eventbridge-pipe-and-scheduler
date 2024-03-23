import json
import os
import time
from datetime import datetime

import boto3


SQS_NAME = os.environ["SQS_NAME"]
SQS_RESOURCE = boto3.resource("sqs")
SQS_QUEUE = SQS_RESOURCE.get_queue_by_name(QueueName=SQS_NAME)


def lambda_handler(event, context) -> None:
    print("event", event)
    start_time = time.time()
    for i in reversed(range(60)):
        payload = {"creation_time": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")}
        SQS_QUEUE.send_message(MessageBody=json.dumps(payload))
        time.sleep(
            (start_time + 60 - i - time.time())  # right on the second
            - 0.1  # a little less for Lambda to finish in under 1 minute
        )
