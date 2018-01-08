import os
import json
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
from event_processor import EventEnhancer

DB_CONFIG = json.load(open('dbconfig.json'))
CASSANDRA_HOST = DB_CONFIG["cassandra"]["host"]
CASSANDRA_KEYSPACE = DB_CONFIG["cassandra"]["keyspace"]

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages datastax:spark-cassandra-connector:2.0.6-s_2.11 --conf spark.cassandra.connection.host='+CASSANDRA_HOST+' pyspark-shell'

sc = SparkContext("local[2]", "Event Enhance Job")
sql = SQLContext(sc)

def process_event(event):
    eventEnhancer = EventEnhancer()
    event = eventEnhancer.categorizeEvent(event)
    event = eventEnhancer.processGeolocation(event)

while True:    
    result = sql.read.format("org.apache.spark.sql.cassandra").load(keyspace=CASSANDRA_KEYSPACE, table="events").select("*").where("location_level is null").orderBy(desc("timestamp_ms"))
    result.rdd.foreach(process_event)