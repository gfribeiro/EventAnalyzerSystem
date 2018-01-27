import dash
import dash_core_components as dcc
import dash_html_components as html
import plotly.graph_objs as go
import os
from pyspark import SparkContext,SparkConf
from pyspark.sql import SQLContext

# SPARK CONTEXT

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages datastax:spark-cassandra-connector:2.0.6-s_2.11 --conf spark.cassandra.connection.host=172.17.0.3 pyspark-shell'

conf = SparkConf()
conf.setMaster("local[*]")
conf.setAppName("Cassandra crud test")

sc = SparkContext(conf=conf)
sql = SQLContext(sc)

app = dash.Dash()

# REGISTER EVENTS TABLE

events = sql.read.format("org.apache.spark.sql.cassandra").load(keyspace="sea", table="events")
events.registerTempTable("events")

# RESULTSETS
category_names = sql.sql("select category_name from events where category_name is not null group by category_name").toPandas()

# DASHBOARD LAYOUT

app.layout = html.Div([
    html.H1(children='Eventos de Segurança Pública'),
    html.Button('Atualizar', id='update-btn'),
    # EVENTS BY STATE
    html.Div(id='events-state',children=[
        html.Div(children='Tipo de evento:'),
        dcc.Dropdown(id='events-state-category',
                    options=[{'label': i, 'value': i} for i in category_names['category_name']],
                    value=category_names['category_name'][0]),
        dcc.Graph(id='events-state-graph')
    ]),
    
    #EVENTS MONTH
    html.Div(id='events-month',children=[
        dcc.Graph(id='events-month-graph')
    ])
    
])

# CALLBACKS

# EVENTS BY STATE CALLBACK
@app.callback(
    dash.dependencies.Output('events-state-graph', 'figure'),
    [dash.dependencies.Input('events-state-category', 'value'),
    dash.dependencies.Input('update-btn', 'n_clicks')])
def update_events_state(events_state_category, n_clicks):
    events_state = sql.sql("select * from (select count(0) total, regexp_replace(state,'State of ','') rep_state from events where country = 'Brazil' and state is not null and category_name = '"+events_state_category+"' group by rep_state) order by total desc").toPandas()
    return {
        'data': [
            {'x': events_state['rep_state'], 'y': events_state['total'], 'type': 'bar', 'name': 'Sender'},
        ],
        'layout': {
            'title': 'Eventos por Estado'
        }
    }

# EVENTS LAST MONTH CALLBACK
@app.callback(
    dash.dependencies.Output('events-month-graph', 'figure'),
    [dash.dependencies.Input('update-btn', 'n_clicks')])
def update_events_time(n_clicks):
    events_last_month = sql.sql("select * from (select count(0) total, from_unixtime(timestamp_ms/1000,'yyyy-MM-dd') event_date, category_name from events where country = 'Brazil' and category_name is not null group by event_date, category_name) order by event_date asc").toPandas()
    event_data = [go.Scatter(
        x = value['event_date'],
        y = value['total'],
        mode = 'lines',
        name = key
    ) for key,value in events_last_month.groupby('category_name')]
    
    return go.Figure(
        data=event_data,
        layout=go.Layout(
            title='Últimos 30 Dias',
            showlegend=True,
            legend=go.Legend(
                x=0,
                y=1.0
            ),
            margin=go.Margin(l=40, r=0, t=40, b=30)
        )
    )

app.css.append_css({
    "external_url": "http://tcc-ds-igti.eastus.cloudapp.azure.com:8080/dashboard.css"
})

if __name__ == '__main__':
    app.run_server(debug=False,port=8050, host='172.17.0.2')