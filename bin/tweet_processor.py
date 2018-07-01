#!/usr/bin/env python3

import time
import os
import psycopg2
import json
import itertools
import sys
from collections import defaultdict

parent_path = os.path.realpath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(parent_path)

from common import conn, cur, get_response, ack_response, nack_response, get_raw, command, get_raw_nb

while True:
    N = 1000

    meta, resp = get_response("tweets")
    metadata = resp["metadata"]
    if "max_id" not in metadata:
        metadata["max_id"] = None
    cmd_max_id = metadata["max_id"]

    to_update = defaultdict(lambda: {
        "favs": 0,
        "rts": 0,
        "count": 0,
        "combinations": defaultdict(lambda: 0)
    })

    for t in resp["result"]["statuses"]:
        if cmd_max_id == t["id_str"]:
            continue
        if not metadata["max_id"] or t["id_str"] < metadata["max_id"]:
            metadata["max_id"] = t["id_str"]
        metadata["collected"] += 1

        hashtags = set([ v["text"].lower() for v in t["entities"]["hashtags"] ])
        fav_count = t["favorite_count"]
        rt_count = t["retweet_count"]
        cur.execute("insert into number_of_hashtags_tmp (number) values (%s)", (len(hashtags),))
        for hashtag in hashtags:
            to_update[hashtag]["favs"] += fav_count
            to_update[hashtag]["rts"] += rt_count
            to_update[hashtag]["count"] += 1

        for (h1, h2) in itertools.combinations(hashtags, 2):
            to_update[min(h1,h2)]["combinations"][max(h1,h2)] += 1

    for hashtag, var in ( (k, to_update[k]) for k in sorted(to_update.keys()) ):
        cur.execute("""
            insert into hashtags (hashtag, total_tweets, total_favs, total_rts) values
                (%(hashtag)s, 1, %(favs)s, %(rts)s)
            on conflict (hashtag) do update set
                total_tweets = hashtags.total_tweets + %(count)s,
                total_favs = hashtags.total_favs + %(favs)s,
                total_rts = hashtags.total_rts + %(rts)s
            """,
            {
                "hashtag": hashtag,
                "count": var["count"],
                "favs": var["favs"],
                "rts": var["rts"]
            })

    for h1, var in ( (k, to_update[k]) for k in sorted(to_update.keys()) ):
        for h2, c in var["combinations"].items():
            cur.execute("""
                insert into hashtags_relations (h1, h2, tweets_with_both)
                    values (
                        least(
                            (select id from hashtags where hashtag = %(h1)s),
                            (select id from hashtags where hashtag = %(h2)s)),
                        greatest(
                            (select id from hashtags where hashtag = %(h1)s),
                            (select id from hashtags where hashtag = %(h2)s)),
                        1)
                on conflict (h1, h2) do update set
                    tweets_with_both = hashtags_relations.tweets_with_both + %(c)s
            """, {"h1": h1, "h2": h2, "c": c})

    if metadata["collected"] < N and len(resp["result"]["statuses"]) > 1:
        params = {"max_id": metadata["max_id"]}
        params.update(metadata["params"])
        command("get", "search/tweets", params, "tweets", metadata=metadata)

    conn.commit()
    ack_response(meta)
    print("Processed for q = %s (+%s, %s / %s)" % (metadata["params"]["q"], len(resp["result"]["statuses"])-1, metadata["collected"], N))