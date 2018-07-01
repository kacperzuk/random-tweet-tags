#!/usr/bin/env python3

import time
import os
import json
import sys
from collections import defaultdict

parent_path = os.path.realpath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(parent_path)

from common import conn, cur, get_response, ack_response, nack_response, get_raw, command, get_raw_nb

while True:
    meta_seed, seed = get_raw("seeds")
    sys.stdout.flush()

    max_id = None
    N = 3000
    meta_tweets, resp = get_raw_nb("seed_"+seed)
    if not meta_tweets:
        metadata = {
            "collected": 0,
            "ht_counters": {}
        }
        params = {"q": seed, "result_type": "recent", "count": 100}
        if max_id:
            params["max_id"] = max_id
        command("get", "search/tweets", params, "seed_"+seed, metadata=metadata)
        meta_tweets, resp = get_raw("seed_"+seed)
    metadata = resp["metadata"]
    while True:
        prev_max_id = max_id
        for t in resp["result"]["statuses"]:
            twid = t["id_str"]
            if twid == prev_max_id:
                continue
            metadata["collected"] += 1
            hashtags = [ v["text"].lower() for v in t["entities"]["hashtags"] ]
            fav_count = t["favorite_count"]
            rt_count = t["retweet_count"]
            for h in hashtags:
                if h not in metadata["ht_counters"]:
                    metadata["ht_counters"][h] = 0
                metadata["ht_counters"][h] += 1
            if not max_id or t["id_str"] < max_id:
                max_id = t["id_str"]
        print("Seed '%s': %d out of %d" % (seed, metadata["collected"], N))
        ack_response(meta_tweets)
        if metadata["collected"] >= N:
            break
        params = {"q": seed, "result_type": "recent", "count": 100}
        if max_id:
            params["max_id"] = max_id
        command("get", "search/tweets", params, "seed_"+seed, metadata=metadata)
        meta_tweets, resp = get_raw("seed_"+seed)
        metadata = resp["metadata"]
    top_hashtags = sorted(metadata["ht_counters"].items(), reverse=True, key=lambda x: x[1])[:1000]
    for (hashtag, t) in top_hashtags:
        params = {"q": "#"+hashtag, "result_type": "recent", "count": 100}
        command("get", "search/tweets", params, "tweets", metadata={"collected": 0, "params": params})
    cur.execute("update seeds set processed = 't' where seed = %s", (seed,))
    conn.commit()
    ack_response(meta_seed)
    print("Finished seed '%s'" % (seed,))
