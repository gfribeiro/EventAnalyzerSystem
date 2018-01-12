import os
import json
import datetime
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import Row
from pyspark.sql.functions import *
from pyspark.sql.types import *
from event_processor import EventEnhancer
from pyspark.sql import Row

DB_CONFIG = json.load(open('dbconfig.json'))
CASSANDRA_HOST = DB_CONFIG["cassandra"]["host"]
CASSANDRA_KEYSPACE = DB_CONFIG["cassandra"]["keyspace"]
LOG_FILE = 'enhance_job.log'

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages datastax:spark-cassandra-connector:2.0.6-s_2.11 --conf spark.cassandra.connection.host='+CASSANDRA_HOST+' pyspark-shell'

sc = SparkContext("local[2]", "Event Enhance Job")
sql = SQLContext(sc)

def get_event_schema():
    return StructType([StructField('source_type', StringType(), False),
                       StructField('source_id', StringType(), False),
                       StructField('sender_id', StringType(), True),
                       StructField('sender_name', StringType(), True),
                       StructField('sender_influence', IntegerType(), True),
                       StructField('category_name', StringType(), True),
                       StructField('event_desc', StringType(), True),
                       StructField('lang', StringType(), True),
                       StructField('reference_count', IntegerType(), True),
                       StructField('favorite_count', IntegerType(), True),
                       StructField('latitude', DoubleType(), True),
                       StructField('longitude', DoubleType(), True),
                       StructField('street', StringType(), True),
                       StructField('neighborhood', StringType(), True),
                       StructField('city', StringType(), True),
                       StructField('state', StringType(), True),
                       StructField('country', StringType(), True),
                       StructField('location_level', IntegerType(), True),
                       StructField('timestamp_ms', StringType(), True)])
                       

def process_event(event):
    event = event.asDict()
    eventEnhancer = EventEnhancer()
    event = eventEnhancer.categorizeEvent(event)
    event = eventEnhancer.processGeolocation(event)
    return Row(**event)

while True:    
    events = sql.read.format("org.apache.spark.sql.cassandra").load(keyspace=CASSANDRA_KEYSPACE, table="events").select("*").where("location_level is null").limit(100)
    rdd = events.rdd.map(process_event)
    schema = get_event_schema()
    df = sql.createDataFrame(rdd,schema)
    df.write.format("org.apache.spark.sql.cassandra").mode('append').options(table="events", keyspace=CASSANDRA_KEYSPACE).save()
    print('%s - PROCESSED: %d ROWS' % (datetime.datetime.now(),df.count()),file=open(LOG_FILE,"a"))