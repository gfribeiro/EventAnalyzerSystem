import os
import json
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
from event_processor import EventEnhancer
from pyspark.sql import Row

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
    rdd = sc.parallelize([tuple(event.values())])
    df = rdd.toDF(list(event.keys()))
    df.write.format("org.apache.spark.sql.cassandra").mode('append').options(table="events", keyspace=CASSANDRA_KEYSPACE).save()
    print(event)
    return event

#while True:    
result = sql.read.format("org.apache.spark.sql.cassandra").load(keyspace=CASSANDRA_KEYSPACE, table="events").select("*").where("location_level is null").orderBy(desc("timestamp_ms")).limit(5)
rs = result.collect()
for row in rs:
    process_event(row.asDict())