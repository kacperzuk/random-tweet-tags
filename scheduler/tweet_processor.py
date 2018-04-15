import time
import psycopg2
import json
import itertools
import sys
from collections import defaultdict

from common import conn, cur, get_response, ack_response, nack_response, s, get_raw, command, get_raw_nb
from process_user import process_user

while True:
    N = 1000

    meta, resp = get_response("tweets")
    metadata = resp["metadata"]
    if "max_id" not in metadata:
        metadata["max_id"] = None
    cmd_max_id = metadata["max_id"]

    for t in resp["result"]["statuses"]:
        if cmd_max_id == t["id_str"]:
            continue
        if not metadata["max_id"] or t["id_str"] < metadata["max_id"]:
            metadata["max_id"] = t["id_str"]
        metadata["collected"] += 1
        hashtags = set([ v["text"].lower() for v in t["entities"]["hashtags"] ])
        fav_count = t["favorite_count"]
        rt_count = t["retweet_count"]
        for hashtag in hashtags:
            cur.execute("""
                insert into hashtags (hashtag, total_tweets, total_favs, total_rts) values
                    (%(hashtag)s, 1, %(favs)s, %(rts)s)
                on conflict (hashtag) do update set
                    total_tweets = hashtags.total_tweets + 1,
                    total_favs = hashtags.total_favs + %(favs)s,
                    total_rts = hashtags.total_rts + %(rts)s
                """,
                {
                    "hashtag": hashtag,
                    "favs": fav_count,
                    "rts": rt_count
                })
        for (h1, h2) in itertools.combinations(hashtags, 2):
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
                    tweets_with_both = hashtags_relations.tweets_with_both + 1
            """, {"h1": h1, "h2": h2})

    if metadata["collected"] < N:
        params = {"max_id": metadata["max_id"]}
        params.update(metadata["params"])
        command("get", "search/tweets", params, "tweets", metadata=metadata)

    conn.commit()
    ack_response(meta)
    print("Processed for q = %s (%s / %s)" % (metadata["params"]["q"], metadata["collected"], N))
