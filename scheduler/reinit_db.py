import psycopg2
import os

conn = psycopg2.connect("dbname=%s user=%s host=%s password=%s" % (
    os.getenv("PGDATABASE"),
    os.getenv("PGUSER"),
    os.getenv("PGHOST"),
    os.getenv("PGPASSWORD")))

cur = conn.cursor()
cur.execute("drop table if exists hashtags_relations")
cur.execute("drop table if exists hashtags")
cur.execute("drop table if exists seeds")
cur.execute("""
create table seeds (
    seed text not null unique,
    processed boolean not null default 'f'
)""")
cur.execute("""
create table hashtags (
    id serial primary key,
    hashtag text not null unique,
    total_tweets integer not null default 0 check(total_tweets >= 0),
    total_favs integer not null default 0 check(total_favs >= 0),
    total_rts integer not null default 0 check(total_rts >= 0)
)""")
cur.execute("""
create table hashtags_relations (
    h1 integer not null references hashtags(id),
    h2 integer not null references hashtags(id),
    tweets_with_both integer not null default 0 check(tweets_with_both >= 0),
    check(h1 < h2),
    primary key(h1, h2)
)""")
conn.commit()