#!/usr/bin/env python3

import psycopg2
import os

conn = psycopg2.connect("dbname=%s user=%s host=%s password=%s" % (
    os.getenv("PGDATABASE"),
    os.getenv("PGUSER"),
    os.getenv("PGHOST"),
    os.getenv("PGPASSWORD")))

cur = conn.cursor()
cur.execute("drop table if exists user_hashtags")
cur.execute("drop table if exists users")
cur.execute("drop table if exists number_of_hashtags_tmp")
cur.execute("drop table if exists hashtags_relations")
cur.execute("drop table if exists hashtags")
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
cur.execute("""
create table number_of_hashtags_tmp (
    number int not null
);
""")
cur.execute("""
create table users (
    id serial primary key,
    twid text,
    screen_name text,
    finished_tweets boolean default 'f'
);
""")
cur.execute("""
create table user_hashtags (
    uid integer not null references users(id),
    hashtag integer not null references hashtags(id),
    number_of_tweets bigint,
    primary key (uid, hashtag)
);
""")
conn.commit()
