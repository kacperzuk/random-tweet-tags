#!/usr/bin/env python3

import time
import os
import json
import itertools
import sys
import statsd
import logging
from collections import defaultdict

parent_path = os.path.realpath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(parent_path)

from common import conn, cur, get_response, ack_response, nack_response, get_raw, command, get_raw_nb

c = statsd.StatsClient('localhost', 8125, prefix='user_processor')

while True:
    logging.info("Waiting for requests in 'users_to_download' queue...")
    meta, screen_name = get_raw("users_to_download")
    with c.timer("valid_jobs"):
        logging.info("Got request for user=%s, requesting info from Twitter...", screen_name)

        params = { "screen_name": screen_name }
        q = command("get", "users/show", params, "user_details")
        meta_user, resp = get_response("user_details")
        twid = resp["result"]["id_str"]
        cur.execute("insert into users (twid, screen_name) values (%s, %s) returning id", (twid, screen_name))
        uid = cur.fetchone()[0]
        conn.commit()

        ack_response(meta_user)
        
        # start job for downloading user tweets
        params = { "user_id": twid, "count": 200, "trim_user": "true", "include_rts": "false" }
        metadata = { "user_id": uid, "params": params, "collected": 0, "hashtags": {} }
        command("get", "statuses/user_timeline", params, "user_tweets", metadata=metadata)

        # start job for downloading friends
        params = { "user_id": twid, "count": 5000, "stringify_ids": True }
        metadata = { "params": params, "parent": twid, "parent_level": 0}
        command("get", "friends/ids", params, "friends_ids", metadata=metadata)

        # remove this user from queue of users to download
        ack_response(meta)
        logging.info("Processed screen name %s", screen_name)
