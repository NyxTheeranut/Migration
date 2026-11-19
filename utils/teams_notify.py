"""Post a message to a Microsoft Teams channel through an incoming webhook.

Each channel's webhook URL comes from an environment variable, for example
TEAMS_WEBHOOK_JOB_FAIL for channel "job-fail". If it is not set, the message is
only printed to the task log, so the DAG still works on a laptop.
"""

import json
import os
import urllib.request


def send_teams_message(message, channel):
    env_name = "TEAMS_WEBHOOK_" + channel.upper().replace("-", "_")
    url = os.environ.get(env_name)
    if not url:
        print(f"[teams:{channel}] {message}  ({env_name} not set, not sent)")
        return

    request = urllib.request.Request(
        url,
        data=json.dumps({"text": message}).encode(),
        headers={"Content-Type": "application/json"},
    )
    try:
        urllib.request.urlopen(request, timeout=10)
    except Exception as error:
        # A failed alert must never fail or hide the real task result.
        print(f"[teams:{channel}] could not send: {error}")
