import pika
import json
from time import sleep
from os import environ
from datetime import datetime
import pandas as pd


CHANNELNAME = "ingestormessages"
PUBLISHCHANNELNAME = "pandasdfs"
BLOCKTHRESHOLD = 5 # in seconds

try:
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=environ["RABBITMQHOST"]))
except Exception as e:
    # give it some time
    sleep(5)
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=environ["RABBITMQHOST"]))

channel = connection.channel()
channel2 = connection.channel()
channel2.queue_declare(queue=PUBLISHCHANNELNAME, durable=True)

queue = channel.queue_declare(queue=CHANNELNAME, durable=True)


TMPDICTSTORE = []
lastTimestamp = None

def translateEntries(dictionary):
    # edits and translates dictionary entries
    keyz = dictionary.keys()
    # TS should already be translated
    if "P" in keyz:
        dictionary["P"] = float(dictionary["P"])/100 # should be price divided by 1000
    if "Q" in keyz:
        dictionary["Q"] = int(dictionary["Q"]) # quantity
    
    ## build the dict containing only the info we want
    newdict = {
        "timestamp" : dictionary["TS"],
        "symbol" : dictionary["S"],
        "price" : dictionary["P"],
        "quantity" : dictionary["Q"],
        "priceTimesQuantity" : round(dictionary["P"]*dictionary["Q"])
    }
    return newdict
        
        
def tick2Block(df):
    # takes pandas dataframe containing tick data, and somehow calculates per stock smart values
    # for symbol in df.symbol.unique:
        # timestamp -> first timestamp of array
        # stock -> just take stockname of first
        # price -> mean(sub)
    pass

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
    if (body["TS"] - lastTimestamp).seconds > BLOCKTHRESHOLD:
        df = pd.DataFrame(TMPDICTSTORE)
        print("%s, yooo 5 sec durch du spasst. shape df: %s"%(str(body["TS"]),str(df.shape)))
        # send newly created df to queue
        out = df.to_json()
        channel.basic_publish(exchange='',
                routing_key=PUBLISHCHANNELNAME,
                body=out)
        TMPDICTSTORE = []
        lastTimestamp = body["TS"]
    else:
        body = translateEntries(body)
        TMPDICTSTORE.append(body)
    

# channel.basic_qos(prefetch_count=1)
print("nr of messages in queue: ",queue.method.message_count)

channel.basic_consume(CHANNELNAME,callback)
channel.start_consuming()