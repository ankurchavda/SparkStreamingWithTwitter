import time
import json
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
 

def filter_tweets(tweet):
    json_tweet = json.loads(tweet)
    if json_tweet.has_key('lang'): 
        if json_tweet['lang'] == 'en':
            return True 
    return False
 
sc = SparkContext("local[2]", "Spark Stream Demo")
sc.setLogLevel(("ERROR"))
ssc = StreamingContext(sc, 120) #120 is the batch interval in seconds
IP = "localhost"
Port = 6000
lines = ssc.socketTextStream(IP, Port)

lines.foreachRDD( lambda rdd: rdd.filter( filter_tweets ).coalesce(1).saveAsTextFile("./tweets/%f" % time.time()) )

ssc.start()
ssc.awaitTermination()