import os
from pyspark import SparkContext,SparkConf
from pyspark.sql import SQLContext

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages datastax:spark-cassandra-connector:2.0.6-s_2.11 --conf spark.cassandra.connection.host=172.17.0.2 pyspark-shell'

conf = SparkConf()
conf.setMaster("local[2]")
conf.setAppName("Cassandra crud test")

sc = SparkContext(conf=conf)
sql = SQLContext(sc)

config = sql.read.format("org.apache.spark.sql.cassandra").load(keyspace="dwa", table="app_config")

config.registerTempTable("config")

result = sql.sql("select * from config")
result.show()

rdd = sc.parallelize([("APP_NAME","Outro"),("2_TWITTER_CONNECTOR_HOST","LOCAL")])

df = rdd.toDF(['config_key','config_value'])

df.write.format("org.apache.spark.sql.cassandra").mode('append').options(table="app_config", keyspace="dwa").save()
result = sql.sql("select * from config")
result.show()
