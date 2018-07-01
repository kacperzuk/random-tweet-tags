#!/usr/bin/env python3

import time
import os
import json
import itertools
import sys
import logging
from collections import defaultdict

parent_path = os.path.realpath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(parent_path)

from common import get_response, ack_response, nack_response, get_raw, command, get_raw_nb

while True:
    logging.info("Waiting for requests in 'users_to_download' queue...")
    meta, screen_name = get_raw("users_to_download")
    logging.info("Got request for user=%s, requesting info from Twitter...", screen_name)
    
    # start job for downloading user tweets
    params = { "screen_name": screen_name, "count": 200, "trim_user": "true", "include_rts": "false" }
    metadata = { "params": params, "collected": 0 }
    command("get", "statuses/user_timeline", params, "user_tweets", metadata=metadata)

    # start job for downloading friends
    params = { "screen_name": screen_name, "count": 5000, "stringify_ids": True }
    metadata = { "params": params, "parent": screen_name, "parent_level": 0}
    command("get", "friends/ids", params, "friends_ids", metadata=metadata)

    # remove this user from queue of users to download
    ack_response(meta)
    logging.info("Processed screen name %s", screen_name)
