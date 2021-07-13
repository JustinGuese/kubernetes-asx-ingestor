import pika
import json
from time import sleep
from os import environ
from datetime import datetime,timedelta
from sqlalchemy import create_engine,text,exc
import pandas as pd
import numpy as np 


CHANNELNAME = "ingestormessages"
PUBLISHCHANNELNAME = "pandasdfs"
BLOCKTHRESHOLD = 5 # in seconds


# create psql connector

engine = create_engine('postgresql://postgres:%s@%s:5432/postgres'%(environ["POSTGRES_PASSWORD"],environ["POSTGRES_HOST"]))

# grab selection file with marketcaps
with open("asxmarketcaps.json", "r") as file:
    ASXMARKETCAPS = json.loads(file.read())

try:
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=environ["RABBITMQHOST"]))
except Exception as e:
    # give it some time
    sleep(10)
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=environ["RABBITMQHOST"]))

channel = connection.channel()
channel2 = connection.channel()
channel2.queue_declare(queue=PUBLISHCHANNELNAME, durable=True)

queue = channel.queue_declare(queue=CHANNELNAME, durable=True)


TMPDICTSTORE = []
lastTimestamp = None

### RUNNING VARIABLES ###
TODAYDEBUG = True
if TODAYDEBUG:
    OPENINGPRICES = {"currentdate":datetime.strptime('2021-01-24',"%Y-%m-%d").date()}
else:
    OPENINGPRICES = {"currentdate":datetime.now().date()}

# In here variables keeping track of opening price etc are created. They die as soon as the container crashes, and should update on a new day
CURRENTDAY = datetime.now().date()
OPENPRICE = 0.0 # for price increase since open
### END RUNNING VARIABLES ###

def maintainOpeningPrices(df,symbols):
    global OPENINGPRICES, TODAYDEBUG
    # first check if we have new stocks, and if so query new OpeningPrices
    # first check if we have a new day
    if TODAYDEBUG:
        TODAY = datetime.strptime('2021-01-24',"%Y-%m-%d").date()
        TOMORROW = datetime.strptime('2021-01-25',"%Y-%m-%d").date()
    else:
        TODAY = datetime.now().date()
        TOMORROW = TODAY + timedelta(days=1)
    
    # print("TODAY: %s, currentdateinfdf: %s"%(str(TODAY), str(OPENINGPRICES.get("currentdate"))))
    if TODAY != OPENINGPRICES.get("currentdate"):
        # reset it to new day
        OPENINGPRICES = {"currentdate":TODAY}
    else: # if still same day
        # check if entry exists
        for symbol in symbols:
            if symbol in OPENINGPRICES.keys():
                # if we already have an entry do not update
                pass
            else:
                # if we do not yet have an entry
                # make db call and set it to that value
                
                startingPrice = None
                try:
                    with engine.connect() as connection:
                        query = """SELECT *
                                    FROM asx_data
                                    where symbol IN ('%s') and "timestamp" between '%s' and '%s'
                                    order by "timestamp" asc
                                    limit 1;""" % (symbol, TODAY.strftime("%Y-%m-%d"), TOMORROW.strftime("%Y-%m-%d"))
                        result = connection.execute(text(query))
                        valarray = []
                        for row in result:
                            valarray.append(row[2])
                        if len(valarray) > 1: 
                            raise Exception("query returned more than one entry, can't be!!!!!")
                        elif len(valarray) == 0:
                            startingPrice = None
                        else:
                            startingPrice = valarray[0] # should be position?
                            OPENINGPRICES[symbol] = float(startingPrice)
                except exc.SQLAlchemyError as e:
                    print(repr(e))
                    print("entry not found yet, which should be ok if you just started, but not later!!!!")
                
                # if startingprice None then it means we had an error or no response from sql
                if startingPrice is None:
                    # usually bc no entry yet in db, 
                    # if db call fails set it to current value (as this might be the first of the day)
                    subset = df[df["symbol"]==symbol]
                    OPENINGPRICES[symbol] = np.median(subset["price"])

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
    global ASXMARKETCAPS, OPENINGPRICES
    combined = []
    # takes pandas dataframe containing tick data, and somehow calculates per stock smart values
    # for symbol in df.symbol.unique:
        # timestamp -> first timestamp of array
        # stock -> just take stockname of first
        # price -> mean(sub)
    symbols = df.get("symbol")
    if symbols is not None:
        symbols = symbols.unique()
        if len(symbols) > 0:
            # first check and set opening prices
            maintainOpeningPrices(df,symbols)
            # now loop through everything to get matching values
            for symbol in symbols:
                subset = df[df["symbol"]==symbol]
                if len(subset) == 0:
                    print("!!!!!!!!!!!!! length subset:", len(subset))
                # build array
                
                timestamp = subset["timestamp"].values[0] # just first entry as timestamp
                # symbol already there
                price = np.median(subset["price"]) # median price
                # price % gain since open calculated with OPENINGPRICES
                if float(price) == float(OPENINGPRICES[symbol]) or OPENINGPRICES[symbol] == 0.:
                    # avoid zero division 
                    pricePctGainSinceOpenTimesHundred = 0.
                else:
                    pricePctGainSinceOpenTimesHundred = ((float(price)-OPENINGPRICES[symbol])/OPENINGPRICES[symbol])*100 * 100 # bc should be times 100
                quantity = np.median(subset["quantity"]) # median quantity
                volume = np.sum(subset["quantity"]) # volume equals sum of quantity
                noOrders = len(subset) # should be amount of trades in this timeframe
                priceTimesQuantity = np.median(subset["priceTimesQuantity"]) # median priceTimesQuantity
                totalPriceTimesQuantity = np.sum(subset["priceTimesQuantity"]) # median
                windowsSize = BLOCKTHRESHOLD
                # get marketcap
                marketcap = ASXMARKETCAPS.get(symbol)
                # get turnover as pct of marketcap
                if marketcap is None:
                    # print("OHOH! We don't have a marketcap for: ",symbol)
                    turnoverPctOfMarketcap = -1.0
                else:
                    turnoverPctOfMarketcap = (100/marketcap) * priceTimesQuantity
                ## jim signals
                # turnover since open calculated in gui
                # trades since open as well in gui sum
                # turnover per trade
                tunoverPerTrade = totalPriceTimesQuantity / noOrders

                # TODO: track averages and set this in comparison, price since start etc
                column_names = ["timestamp","symbol","price","pricePctGainSinceOpenTimesHundred","quantity","volume",'noOrders',"turnover-priceTimesQuantity","turnoverPctOfMarketcap","totalPriceTimesQuantity","tunoverPerTrade","windowsSize"]
                columns = [timestamp,symbol,price,pricePctGainSinceOpenTimesHundred,quantity,volume,noOrders,priceTimesQuantity,turnoverPctOfMarketcap,totalPriceTimesQuantity,tunoverPerTrade,windowsSize]
                combined.append(columns)
            # if all symbols processed put them together into one huge df
            combinedDf = pd.DataFrame(combined, columns=column_names)
            # add the two combined values and calculate the algorithm1
            averageTurnoverOfAllStocks = np.mean( combinedDf["turnover-priceTimesQuantity"]  ) # should be one value?
            averagePriceGainOfAllStocks = np.mean( combinedDf["pricePctGainSinceOpenTimesHundred"]  )
            # function apply or can we solve this as simple pandas statement?
            combinedDf["algorithm1"] = (df["pricePctGainSinceOpenTimesHundred"] + ((averageTurnoverOfAllStocks / averagePriceGainOfAllStocks)*10) ) ** 3

            return combinedDf
    else:
        print("! invalid dataframe?",df.head())
        return None

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
        # apply additional signals, shrink to x second window
        df = tick2Block(df)
        # is df if valid, None if invalid
        if isinstance(df, pd.DataFrame):
            #  print("%s, yooo 5 sec durch du spasst. shape df: %s"%(str(body["TS"]),str(df.shape)))
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