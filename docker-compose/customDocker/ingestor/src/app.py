import telnetlib
from os import environ
import pika
import json
from time import sleep
from datetime import datetime

tn_ip = environ["TELNETSERVER"]
tn_port = str(environ["TELNETPORT"])

CHANNELNAME = "ingestormessages"

# get stocks file to array to limit ingestion to these stocks
with open('stocks.txt') as f:
    validstocks = [line.rstrip() for line in f]

try:
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=environ["RABBITMQHOST"]))
except Exception as e:
    # give it some time
    sleep(10)
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=environ["RABBITMQHOST"]))

channel = connection.channel()
channel.queue_declare(queue=CHANNELNAME, durable=True)


def string2Dict(message):
    msg_dict = dict()
    message = message.split(",")
    for msg in message:
        key,val = msg.split("=")
        msg_dict.update({key:val})
    return msg_dict

def telnet():
    try:
        tn = telnetlib.Telnet(tn_ip, tn_port, 15)
    except:
        raise
    # tn.set_debuglevel(100)
    while True:
        msg = tn.read_until(b"|")
        msg = msg.decode("ascii")[:-1] # split the | off
        # print(msg)
        # send to rabbitmq
        msg_dict = string2Dict(msg)
        if msg_dict.get("S") in validstocks:
            # add current timestamp if not exists -> it exists if we are using the fake data generator
            if msg_dict.get("TS") is None:
                msg_dict.update({"TS":str(datetime.now())}) # no utc as we want the australian time
            # simple mechanism to only submit data after 10.15, bc before that it is just a mess
            if int(msg_dict["TS"][11:13]) > 10: # little bit tricky, but this is how we get the hour as it is a string
                channel.basic_publish(exchange='',
                    routing_key=CHANNELNAME,
                    body=json.dumps(msg_dict),
                    properties=pika.BasicProperties(
                        delivery_mode = 2, # make message persistent
                    ))
        else:
            # print("stock not in selection: ",str(msg_dict["S"]))
            pass
        
telnet()