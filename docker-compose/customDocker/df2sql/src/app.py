import pika
import pandas as pd
from sqlalchemy import create_engine
from os import environ
import json
from time import sleep

PUBLISHCHANNELNAME = "pandasdfs"

# create psql connector

engine = create_engine('postgresql://postgres:%s@%s:5432/postgres'%(environ["POSTGRES_PASSWORD"],environ["POSTGRES_HOST"]))

# pika rabbitmq conenctor
try:
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=environ["RABBITMQHOST"]))
except Exception as e:
    # give it some time
    sleep(10)
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=environ["RABBITMQHOST"]))

channel = connection.channel()
queue = channel.queue_declare(queue=PUBLISHCHANNELNAME, durable=True)


def callback(ch, method, properties, body):
    body = json.loads(body)
    df = pd.DataFrame.from_dict(body) 
    # convert unix timestamp to datetime
    df["timestamp"] = pd.to_datetime(df['timestamp'],unit='ms')
    df.to_sql('asx_data', engine,if_exists='append', index=False)
    # kam an, send ok
    ch.basic_ack(delivery_tag = method.delivery_tag)
    #  print(df.head())




# channel.basic_qos(prefetch_count=1)
print("nr of messages in queue: ",queue.method.message_count)

channel.basic_consume(PUBLISHCHANNELNAME,callback)
channel.start_consuming()