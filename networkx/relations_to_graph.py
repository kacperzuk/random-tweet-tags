#!/usr/bin/env python3

import time
import os
import networkx as nx
import sys

parent_path = os.path.realpath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(parent_path)


from common import cur

print("Initializing graph...")
G = nx.Graph()

print("Max hashtag...")
cur.execute("select max(total_tweets) from hashtags");

print("Requesting hashtags...")
cur.execute("select id, hashtag, total_tweets/%s from hashtags where id in (select h1 from hashtags_relations order by tweets_with_both desc limit 500) or id in (select h2 from hashtags_relations order by tweets_with_both desc limit 500)", (float(cur.fetchone()[0]),))

print("Transforming hashtags into nodes...")
G.add_nodes_from(
        (
            (
                v[0],
                {
                    "label": v[1],
                    "weight": v[2]
                }
            )
        for v in cur
        )
    )

print("Max relation...")
cur.execute("select max(tweets_with_both) from hashtags_relations");

print("Requesting relations...")
cur.execute("select h1, h2, tweets_with_both/%s from hashtags_relations order by tweets_with_both desc limit 500", (float(cur.fetchone()[0]),));

print("Transforming relations into edges...")
G.add_edges_from(
        (
            (
                v[0],
                v[1],
                { "weight": 1/v[2], "label": int(v[2]*1000) }
            )
        for v in cur
        )
    )

#print("Storing dot...")
#nx.drawing.nx_agraph.write_dot(G, "graph.dot")
#
#print("Storing GEXF...")
#nx.write_gexf(G, "graph.gexf.gz")

print("Cleaning components <4")
for component in list(nx.connected_components(G)):
    if len(component) < 4:
        G.remove_nodes_from(component)

print("Creating agraph...")
G.overlap = "scale"
A = nx.drawing.nx_agraph.to_agraph(G)

print("Store dot...")
nx.drawing.nx_agraph.write_dot(G, 'graph.dot')

#print("Layout...")
#A.layout()
#
#print("Store png...")
#A.draw('graph.png')
