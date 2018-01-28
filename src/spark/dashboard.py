import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_table_experiments as dt
import plotly.graph_objs as go
import os
from datetime import datetime
from pyspark import SparkContext,SparkConf
from pyspark.sql import SQLContext

# SPARK CONTEXT

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages datastax:spark-cassandra-connector:2.0.6-s_2.11 --conf spark.cassandra.connection.host=172.17.0.3 pyspark-shell'

conf = SparkConf()
conf.setMaster("local[*]")
conf.setAppName("Dashboard")

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
    html.Div(id='dash-header',children=[
        html.H1(children='Eventos de Segurança Pública'),
        html.Button('Atualizar', id='update-btn')
    ]),
    
    # EVENT MAP
    # KEY pk.eyJ1IjoiZ2Zlcm5hbmRlc3B5IiwiYSI6ImNqY3hzNzc1cDB4ZWwyeG5zeXBuZjg3Z2YifQ.Wy4-pad8gD2K6GCtnJGJpQ
    html.Div(className='row',children=[
        html.Div(id='events-map',className='eight columns',children=[
            html.H3('Mapa de Eventos'),
            dcc.DatePickerSingle(
                id='map-date-picker',
                display_format="YYYY-MM-DD",
                date=datetime(2017, 12, 29)
            ),
            dcc.Graph(id='events-map-graph'),
            dcc.RangeSlider(
                id='map-range-slider',
                marks={i: '{}h'.format(i) for i in range(0, 23)},
                min=0,
                max=23,
                value=[0, 1]
            )
        ]),  
        # TOP 10 CITIES
        html.Div(id='top-10-cities',className='four columns',children=[
            html.H3('Top 10 Cidades'),
            html.Div(children='Tipo de evento:'),
            dcc.Dropdown(id='top-10-category',
                        options=[{'label': i, 'value': i} for i in category_names['category_name']],
                        value=category_names['category_name'][0]),
            dt.DataTable(
                rows=[{}], # initialise the rows
                selected_row_indices=[],
                id='top-10-cities-table'
            )
        ]),
    ]),
    html.Div(className='row',children=[
        # EVENTS LAST MONTH
        html.Div(id='events-month',className='six columns',children=[
            html.H3('Nos Últimos 30 Dias'),
            dcc.Graph(id='events-month-graph')
        ]),

        # EVENTS BY STATE
        html.Div(id='events-state',className='six columns',children=[
            html.H3('Eventos por Estado'),
            html.Div(children='Tipo de evento:'),
            dcc.Dropdown(id='events-state-category',
                        options=[{'label': i, 'value': i} for i in category_names['category_name']],
                        value=category_names['category_name'][0]),
            dcc.Graph(id='events-state-graph')
        ])
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
            'autosize': True,
            'height': 500
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
            autosize=True,
            height=500,
            showlegend=True,
            legend=go.Legend(
                x=0,
                y=1.0
            ),
            margin=go.Margin(l=40, r=0, t=40, b=30)
        )
    )

# TOP 10 CITIES
@app.callback(dash.dependencies.Output('top-10-cities-table', 'rows'), 
              [dash.dependencies.Input('top-10-category', 'value')])
def update_top10_cities(category):
    top_10_cities = sql.sql("select * from (select count(0) total, city as cidade, regexp_replace(state,'State of ','') estado from events where country = 'Brazil' and city is not null and category_name = '"+category+"' group by cidade, estado) order by total desc limit 10").toPandas()
    return top_10_cities.to_dict('records')

# EVENT MAP
@app.callback(
    dash.dependencies.Output('events-map-graph', 'figure'),
    [dash.dependencies.Input('update-btn', 'n_clicks'),
    dash.dependencies.Input('map-date-picker', 'date'),
    dash.dependencies.Input('map-range-slider', 'value')])
def update_event_map(n_clicks,event_date,event_hour):
    start_date = str(event_date) + ' ' + str(event_hour[0])
    end_date =  str(event_date) + ' ' + str(event_hour[1])
    event_map = sql.sql("select category_name, latitude, longitude from events where country = 'Brazil' and from_unixtime(timestamp_ms/1000,'yyyy-MM-dd H') >= '"+start_date+"' and from_unixtime(timestamp_ms/1000,'yyyy-MM-dd H') <= '"+end_date+"' and latitude is not null and longitude is not null and category_name is not null").toPandas()
    data = go.Data([
        go.Scattermapbox(
            lat=event_map['latitude'],
            lon=event_map['longitude'],
            mode='markers',
            marker=go.Marker(
                size=9
            ),
            text=event_map['category_name'],
        )
    ])
    
    layout = go.Layout(
        autosize=True,
        height=500,
        hovermode='closest',
        margin=go.Margin(l=0, r=0, t=0, b=0),
        mapbox=dict(
            accesstoken='pk.eyJ1IjoiZ2Zlcm5hbmRlc3B5IiwiYSI6ImNqY3hzNzc1cDB4ZWwyeG5zeXBuZjg3Z2YifQ.Wy4-pad8gD2K6GCtnJGJpQ',
            bearing=0,
            center=dict(
                lat=-14.235004,
                lon=-51.92528
            ),
            pitch=0,
            zoom=2
        ),
    )
    
    return go.Figure(data=data,layout=layout)

app.css.append_css({
    "external_url": "http://tcc-ds-igti.eastus.cloudapp.azure.com:8080/dashboard.css"
})

if __name__ == '__main__':
    app.run_server(debug=False,port=8050, host='172.17.0.2')