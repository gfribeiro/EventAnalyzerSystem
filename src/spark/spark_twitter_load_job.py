import time
import json
import datetime
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from database import DataManager
from event_processor import TwitterProcessor

dataManager = DataManager()
appConfig = dataManager.getAppConfig()
eventProcessor = TwitterProcessor()

# Our filter function:
def process_tweet(tweet):
    json_tweet = json.loads(tweet)
    if 'lang' in json_tweet.keys(): # When the lang key was not present it caused issues
        if json_tweet['lang'] == appConfig['TWITTER_LANG_FILTER']:
            event = eventProcessor.createEvent(json_tweet)
            print(json.dumps(event))
            dm = DataManager()
            dm.insertJson('events',json.dumps(event))
            print(str(datetime.datetime.now()), file=open("load.log","a"))
            return True
    return False
 
# SparkContext(“local[1]”) would not work with Streaming bc 2 threads are required
sc = SparkContext("local[2]", "Twitter Loader Job")
ssc = StreamingContext(sc, 10) #10 is the batch interval in seconds
IP = appConfig['TWITTER_CONNECTOR_HOST']
Port = int(appConfig['TWITTER_CONNECTOR_PORT'])
lines = ssc.socketTextStream(IP, Port)

#lines.foreachRDD( lambda rdd: rdd.foreach(process_tweets) )
tweets = lines.flatMap(lambda line: line.split("\n"))
tweets.foreachRDD(lambda rdd: rdd.foreach(process_tweet))

# You must start the Spark StreamingContext, and await process termination…
ssc.start()
ssc.awaitTermination()