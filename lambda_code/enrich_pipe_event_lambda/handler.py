import hashlib
import json
import os
import random
from datetime import datetime
from typing import Any

ENRICHMENT_LAMBDA_FAILURE_RATE = json.loads(
    os.environ["ENRICHMENT_LAMBDA_FAILURE_RATE"]
)


def lambda_handler(event, context) -> list[dict]:
    """Enrichment Lambda can return 1, many, or 0 elements for each input element.
    It is a FlatMap operation.
    """
    print("len(event)", len(event))
    # print("event", event)
    if random.random() <= ENRICHMENT_LAMBDA_FAILURE_RATE:
        message_hashes = [
            hashlib.md5(json.dumps(one_event["body"]).encode("utf-8")).hexdigest()
            for one_event in event
        ]
        raise RuntimeError(
            f"Failing with probability {ENRICHMENT_LAMBDA_FAILURE_RATE}. "
            f"Message hash {message_hashes}"
        )
    else:
        payloads = []
        for one_event in event:
            payload = json.loads(one_event["body"])  # only care about the SQS body
            creation_time = datetime.strptime(
                payload["creation_time"], "%Y-%m-%d %H:%M:%S.%f"
            )
            enrichment_time = datetime.utcnow()
            payload["enrichment_time"] = enrichment_time.strftime(
                "%Y-%m-%d %H:%M:%S.%f"
            )
            payload["time_delta"] = (enrichment_time - creation_time).total_seconds()
            payload["batch_size"] = len(event)
            payload["ApproximateReceiveCount"] = one_event["attributes"][
                "ApproximateReceiveCount"
            ]
            payloads.append(payload)
        print("len(payloads)", len(payloads))
        return payloads
