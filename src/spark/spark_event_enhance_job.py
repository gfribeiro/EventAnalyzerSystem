#spark_event_enhance_job
import os
from pyspark import SparkContext
from pyspark.sql import SQLContext

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages anguenot/pyspark-cassandra:0.7.0 --conf spark.cassandra.connection.host=172.17.0.3 pyspark-shell'

sc = SparkContext("local[2]", "Event Enhance Job")

sql = SQLContext(sc)

events = sql.read.format("org.apache.spark.sql.cassandra").load(keyspace="sea", table="events")

events.registerTempTable("events")

result = sql.sql("select count(0) from events")
result.show()