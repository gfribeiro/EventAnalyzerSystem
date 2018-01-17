# -*- coding: utf-8 -*-
import dash
import dash_core_components as dcc
import dash_html_components as html
import os
from pyspark import SparkContext,SparkConf
from pyspark.sql import SQLContext

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages datastax:spark-cassandra-connector:2.0.6-s_2.11 --conf spark.cassandra.connection.host=172.17.0.2 pyspark-shell'

conf = SparkConf()
conf.setMaster("local[2]")
conf.setAppName("Cassandra crud test")

sc = SparkContext(conf=conf)
sql = SQLContext(sc)

events = sql.read.format("org.apache.spark.sql.cassandra").load(keyspace="sea", table="events")

events.registerTempTable("events")

result = sql.sql("select sender_name, count(0) cnt from events group by sender_name limit 10")

app = dash.Dash()

app.layout = html.Div(children=[
    html.H1(children='Hello Dash'),

    html.Div(children='''
        Dash: A web application framework for Python.
    '''),

    dcc.Graph(
        id='example-graph',
        figure={
            'data': [
                {'x': result.toPandas()['sender_name'], 'y': result.toPandas()['cnt'], 'type': 'bar', 'name': 'Sender'},
            ],
            'layout': {
                'title': 'Dash Data Visualization'
            }
        }
    )
])

if __name__ == '__main__':
    app.run_server(debug=False,port=8050, host='172.17.0.3')