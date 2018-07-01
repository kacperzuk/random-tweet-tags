import psycopg2
import json
import pika
import time
import logging
import os

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(name)s.%(funcName)s +%(lineno)s: %(levelname)-8s [%(process)d] %(message)s',
                    )

params = pika.URLParameters(os.getenv("AMQP_CONNECTION_STRING"))
connection = pika.BlockingConnection(params)
jobs = connection.channel() # start a channel
responses = connection.channel()
raw = connection.channel()

conn = psycopg2.connect("dbname=%s user=%s host=%s password=%s" % (
    os.getenv("PGDATABASE"),
    os.getenv("PGUSER"),
    os.getenv("PGHOST"),
    os.getenv("PGPASSWORD")))
cur = conn.cursor()

def command(method, path, params, reply_to, metadata=None):
    global jobs
    cmd = json.dumps({
        "method": method,
        "path": path,
        "params": params,
        "metadata": metadata,
        "reply_to": reply_to
    })
    responses.queue_declare(queue=reply_to, durable=True, auto_delete=False)
    jobs.queue_declare(queue='twitter_jobs:'+path, durable=True, auto_delete=False) # Declare a queue
    jobs.basic_publish(exchange='',
            routing_key='twitter_jobs:'+path,
            body=cmd,
            properties=pika.BasicProperties(delivery_mode=2))

def get_response(queue):
    return get_raw(queue)

def get_raw_nb(queue):
    responses.queue_declare(queue=queue, durable=True, auto_delete=False)
    isok, properties, resp = responses.basic_get(queue)
    if isok:
        return isok, json.loads(resp)
    return False, None

def get_raw(queue):
    raw.queue_declare(queue=queue, durable=True, auto_delete=False)
    while True:
        isok, properties, resp = responses.basic_get(queue)
        if isok:
            return isok, json.loads(resp)
        time.sleep(0.01)

def ack_response(meta):
    responses.basic_ack(meta.delivery_tag)

def nack_response(meta):
    responses.basic_nack(meta.delivery_tag)

def download_user(screen_name):
    raw.queue_declare(queue='users_to_download', durable=True, auto_delete=False)
    raw.basic_publish(exchange='',
            routing_key='users_to_download',
            body=json.dumps(screen_name),
            properties=pika.BasicProperties(delivery_mode=2))
