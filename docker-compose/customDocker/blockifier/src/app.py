import pika
import json
from time import sleep
from os import environ
from datetime import datetime


CHANNELNAME = "ingestormessages"

try:
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=environ["RABBITMQHOST"]))
except Exception as e:
    # give it some time
    sleep(10)
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=environ["RABBITMQHOST"]))

channel = connection.channel()

queue = channel.queue_declare(queue=CHANNELNAME, durable=True)


TMPDICTSTORE = []
lastTimestamp = None

def callback(ch, method, properties, body):
    global TMPDICTSTORE, lastTimestamp
    body = json.loads(body)
    # convert first to datetime
    try:
        body["TS"] = datetime.strptime(body["TS"], "%Y-%m-%d %H:%M:%S.%f") # default pandas
    except Exception as e:
        print(e)
        print(body["TS"])
    ch.basic_ack(delivery_tag = method.delivery_tag)
    # next if else time difference big enough execute script
    if not lastTimestamp: # if None
        # set it to current stamp
        lastTimestamp = body["TS"]
    if (body["TS"] - lastTimestamp).seconds > 5:
        print("yooo 5 sec durch du spasst",body["TS"])
        lastTimestamp = body["TS"]
    else:
        TMPDICTSTORE.append(body)
    

# channel.basic_qos(prefetch_count=1)
print("nr of messages in queue: ",queue.method.message_count)

channel.basic_consume(CHANNELNAME,callback)
channel.start_consuming()