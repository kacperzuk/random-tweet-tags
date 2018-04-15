import sys
import json

from common import conn, cur, raw

def log_in_db(pgconn, seed):
    cur = pgconn.cursor()
    cur.execute("insert into seeds (seed) values (%s)", (seed,))
    pgconn.commit()

def insert_to_queue(amqp_conn, seed):
    amqp_conn.queue_declare(queue='seeds', durable=True, auto_delete=False)
    amqp_conn.basic_publish(exchange='',
            routing_key='seeds',
            body=json.dumps(seed))

if len(sys.argv) > 2:
    print("Only one cmdline argument is allowed. Quote if you need to pass spaces.")
    sys.exit(1)

seed = sys.argv[1]
log_in_db(conn, seed)
insert_to_queue(raw, seed)
