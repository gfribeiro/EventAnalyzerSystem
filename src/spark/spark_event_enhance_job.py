import os
import nltk
from pyspark import SparkContext
from pyspark.sql import SQLContext

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages anguenot/pyspark-cassandra:0.7.0 --conf spark.cassandra.connection.host=172.17.0.3 pyspark-shell'

sc = SparkContext("local[2]", "Event Enhance Job")

sql = SQLContext(sc)

events = sql.read.format("org.apache.spark.sql.cassandra").load(keyspace="sea", table="events")

events.registerTempTable("events")

result = sql.sql("select count(0) from events")
result.show()

rows = sql.sql("select event_desc,regexp_extract(event_desc,'[no|em] ([A-Z][a-z]+)',1) as words from events where event_desc rlike '.* (no|em) [A-Z][a-z]+ .*'").take(1)
stok = nltk.data.load('tokenizers/punkt/portuguese.pickle')
for row in rows:
    print(row.event_desc)
    for word in nltk.tokenize.word_tokenize(row.event_desc):
        print('w: ' + sent)
    #print(nltk.pos_tag(row.event_desc))
    #print(row.event_desc + '>> words: ' + row.words)
