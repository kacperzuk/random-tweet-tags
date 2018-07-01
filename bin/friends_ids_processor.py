#!/usr/bin/env python3

import time
import json
import os
import itertools
import sys
import logging
from itertools import zip_longest
from collections import defaultdict

parent_path = os.path.realpath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(parent_path)

from common import get_response, ack_response, nack_response, get_raw, command, get_raw_nb

while True:
    logging.info("Waiting for data from 'friends_ids' queue...")
    meta, resp = get_response("friends_ids")

    if resp["result"] != {}:
        logging.info("Got friends ids for %s requesting tweets...", resp["metadata"]["parent"])

        for user_id in resp["result"]["ids"]:
            params = { "user_id": user_id, "count": 200, "trim_user": "true", "include_rts": "false"}
            metadata = { "params": params, "collected": 0}
            command("get", "statuses/user_timeline", params, "user_tweets", metadata=metadata)

            if resp["metadata"]["parent_level"] <= 1:
                params = { "user_id": user_id, "count": 5000, "stringify_ids": True }
                metadata = { "params": params, "parent": user_id, "parent_level": resp["metadata"]["parent_level"] + 1}
                command("get", "friends/ids", params, "friends_ids", metadata=metadata)

        if resp["result"]["next_cursor_str"] != "0":
            resp["metadata"]["params"]["cursor"] = resp["result"]["next_cursor_str"]
            command("get", "friends/ids", resp["metadata"]["params"], "friends_ids", metadata=resp["metadata"])

    ack_response(meta)
