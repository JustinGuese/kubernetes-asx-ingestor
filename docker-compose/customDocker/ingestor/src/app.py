import telnetlib
from os import environ
import pika
import json
from time import sleep

tn_ip = environ["TELNETSERVER"]
tn_port = str(environ["TELNETPORT"])

CHANNELNAME = "ingestormessages"

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
        channel.basic_publish(exchange='',
            routing_key=CHANNELNAME,
            body=json.dumps(msg_dict),
            properties=pika.BasicProperties(
                delivery_mode = 2, # make message persistent
            ))
        
telnet()