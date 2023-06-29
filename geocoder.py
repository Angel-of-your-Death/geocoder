import base64
import io
import json
import requests
import aiohttp
import asyncio
import time
import dash
import datetime
from dash.dependencies import Input, Output, State
from dash import dcc, html, dash_table
import dash_bootstrap_components as dbc
import plotly.express as px
import pandas as pd

async def get_data(csv):
    csv['id'] = csv.index.astype(int)
    result = await asyncio.gather(*(request(csv['lat'].loc[i],csv['lon'].loc[i],csv['dt'].loc[i].replace('/','-')) for i in csv['id']))
    return pd.DataFrame(result)

async def request(lat,lon,dt):
    async with aiohttp.ClientSession() as session:
        async with session.get(url="https://archive-api.open-meteo.com/v1/archive?latitude={}&longitude={}&start_date={}&end_date={}&daily=weathercode,temperature_2m_mean,precipitation_sum,windspeed_10m_max,winddirection_10m_dominant&timezone=auto&timeformat=unixtime&min=&max=".format(lat,lon,dt,dt)) as response:
            data = await response.read()
            data = json.loads(data.decode('utf-8'))
            for i in data['daily'].keys():
                data['daily'][i] = data['daily'].get(i)[0]
            return data['daily']

def thunderstorm(code,precipitation):
    if code==29:
        if precipitation==0:
            return "True"
        else:
            return "False"
    else:
        return "None"

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
px.set_mapbox_access_token('pk.eyJ1IjoieXVwZXN0IiwiYSI6ImNqdWpwOTJ6ZTA5MmQzeW1xeGdrb3VhcjkifQ.UEEIc5yM8s1lfMaREu-p6Q')

select = dcc.Dropdown(id='les1-filter',
                      style={
                          'width': '40%',
                          'top': '5px',
                          'marginLeft': '70px'
                      })
select2 = dcc.Dropdown(id='les2-filter',
                       style={
                           'position': 'relative',
                           'width': '40%',
                           'top': '5px',
                           'marginLeft': '70px'
                       })

body = html.Div([
    html.H1("Визуализация данных", style={ 'textAlign': 'center'}),
    html.H5("Дата:", style={'position': 'relative', 'bottom': '-95px', 'marginLeft': '15px'}),
    html.H6("Лесничество 1:", style={'position': 'relative', 'bottom': '-115px', 'marginLeft': '15px'}),
    html.H6("Лесничество 2:", style={'position': 'relative', 'bottom': '-130px', 'marginLeft': '15px'}),
    html.Div([dcc.DatePickerRange(id='date-filter', style={'marginLeft': '80px'}),select,select2,dbc.Button('Применить фильтры',id='submit', n_clicks=0, style={ 'position': 'relative', 'top': '20px', 'marginLeft': '4px'}),dbc.Button('Сбросить фильтры', id='clear', n_clicks=0, style={ 'position': 'relative', 'top': '20px', 'left': '7px', 'border': '1px solid black'})]),
    dbc.Row(
        dbc.Col(dcc.Upload(
        id='upload-data',
        children=html.Div([
            'Для начала работы перетащите или ',
            html.A('выберите файл')
        ]),
        style={
            'fontSize': '18px',
            'width': '50%',
            'height': '47px',
            'lineHeight': '60px',
            'borderWidth': '2px',
            'borderStyle': 'dashed',
            'borderRadius': '2px',
            'textAlign': 'center',
            'margin': '0 25% 0 25%'
        },
        multiple=False))
    ),
    dbc.Row([html.Div(id='output-data-upload',
                      style={
                          'position': 'relative',
                          'top': '10px',
                          'width': '90%',
                          'textAlign': 'center',
                          'margin': '0 auto'
                      })]),
    dbc.Row([html.Button("Скачать таблицу", id="btn-csv",
                         style={
                             'position': 'relative',
                             'top': '10px',
                             'width': '20%',
                             'margin': '0 auto'
                         }),
             dcc.Download(id="download-dataframe-csv"),]),
    dbc.Row([html.Div(id = "output-map")]),
    dcc.Store(id='intermediate-value'),
    dcc.Store(id='non-filtred')
    
    ])
app.layout = html.Div([body])

def parse_contents(contents, filename):
    df_uploaded = pd.DataFrame()
    if contents:
        try:
            content_type, content_string = contents.split(',')
            decoded = base64.b64decode(content_string)
            if 'csv' in filename:
                df_uploaded = pd.read_csv(
                    io.StringIO(decoded.decode('utf-8')))
            elif 'xls' in filename:
                df_uploaded = pd.read_excel(io.BytesIO(decoded))
        except Exception as e:
            print(e)
    return df_uploaded


@app.callback(Output('intermediate-value', 'data', allow_duplicate=True),
              [Input('submit', 'n_clicks')],
              [State('non-filtred', 'data'),
               State('les1-filter','value'),
               State('les2-filter','value'),
               State('date-filter','start_date'),
               State('date-filter','end_date')], prevent_initial_call=True)
def filter(n,data,les1,les2,start,end):
    data = json.loads(data)
    dff = pd.read_json(data, orient='split')
    if start:
        dff = dff.loc[dff['time']>=datetime.datetime.strptime(start, "%Y-%m-%d").timestamp()]
    if end:
        dff = dff.loc[dff['time']<=datetime.datetime.strptime(end, "%Y-%m-%d").timestamp()]
    if les1:
        dff = dff.loc[dff['lesn1']==les1]
    if les2:
        dff = dff.loc[dff['lesn3']==les2]
    dataset = dff.to_json(orient='split', date_format='iso')
    return json.dumps(dataset)


@app.callback(Output('output-map', 'children'),
              Input('intermediate-value', 'data'), prevent_initial_call=True)
def update_map(data):
    dataset = json.loads(data)
    dff = pd.read_json(dataset, orient='split')
    figure = px.scatter_mapbox(dff, lat="lat", lon="lon", hover_data=["weathercode","temperature","precipitation","wind_speed","wind_direction"], color="precipitation", color_continuous_scale=px.colors.diverging.RdYlBu, range_color=[0,10], size_max=100,  zoom=4)
    figure.update_layout(margin=dict(b=0, t=0, l=0, r=0))
    children = dcc.Graph(id = 'map', figure = figure)
    return children

@app.callback(Output('download-dataframe-csv', 'data'),
              Input('btn-csv', "n_clicks"),
              State('intermediate-value', 'data'), prevent_initial_call=True)
def download_data(n_clicks, data):
    dataset = json.loads(data)
    dff = pd.read_json(dataset, orient='split')
    return dcc.send_data_frame(dff.to_csv, "data.csv")

@app.callback(Output('output-data-upload', 'children'),
              Input('intermediate-value', 'data'), prevent_initial_call=True)
def update_output(data):
    dataset = json.loads(data)
    df = pd.read_json(dataset, orient='split')
    components = [
            dash_table.DataTable(
                df.to_dict('records'),
                [{'name': i, 'id': i} for i in df.columns], 
                page_size = 5,
                style_data={
                    'whiteSpace': 'normal',
                    'height': 'auto',
                }
            ),

            html.Hr(),
        ]

    children = html.Div(components)
    return children  

@app.callback([Output('intermediate-value', 'data', allow_duplicate=True),Output('les1-filter', 'value'),Output('les2-filter', 'value'),Output('date-filter','start_date'),Output('date-filter','end_date')],
              Input('clear','n_clicks'),
              State('non-filtred', 'data'), prevent_initial_call=True)
def clear_filters(n,data):
    dataset = json.loads(data)
    dff = pd.read_json(dataset, orient='split')
    return json.dumps(dataset),None,None,None,None

@app.callback([Output('les1-filter', 'options'),Output('les2-filter','options')],
              Input('non-filtred', 'data'), prevent_initial_call=True)
def update_filter(data):
    dataset = json.loads(data)
    dff = pd.read_json(dataset, orient='split')
    options=[]
    options2=[]
    for i in dff['lesn1'].unique():
        options.append({'label': i, 'value': i})
    for i in dff['lesn3'].unique():
        options2.append({'label': i, 'value': i})
    return options,options2

@app.callback([Output('submit','disabled'),Output('clear','disabled'),Output('submit','color'),Output('clear','color'),Output('btn-csv','disabled'),Output('output-map','style')],
              Input('intermediate-value','data'))
def settings(data):
    if not(data):
        return True, True, 'secondary', 'secondary', True, None
    else:
        return False, False, 'primary', 'danger', False, {'position': 'relative', 'top': '20px','width': '75%','margin': '0 auto','height': 'auto','border' : '6px solid black'}
    
@app.callback([Output('intermediate-value', 'data', allow_duplicate=True),Output('non-filtred', 'data')],
              Input('upload-data', 'contents'),
              Input('upload-data', 'filename'), prevent_initial_call=True)
def set_data(contents, filename):
    df = parse_contents(contents, filename)
    try:
        dff = asyncio.run(get_data(df))
        table = pd.DataFrame.from_dict(dff)
        table.to_csv (index = False, header=True)
        df = df.join(table)
        df['dry_thunderstorm']=df.apply(lambda x: thunderstorm(x.weathercode,x.precipitation_sum), axis=1)
        df.drop(columns=['dt'], inplace=True)
        df.rename(columns={'temperature_2m_mean':'temperature','precipitation_sum':'precipitation','windspeed_10m_max':'wind_speed','winddirection_10m_dominant':'wind_direction'}, inplace=True)
        df = df[['id','type_name','type_id','lat','lon','lesn1','lesn3','time','weathercode','temperature','precipitation','wind_speed','wind_direction','dry_thunderstorm']]
        dataset = df.to_json(orient='split', date_format='iso')
        return (json.dumps(dataset), json.dumps(dataset))
    except Exception as e:
        print(e)
    
if __name__ == "__main__":
    app.run_server(debug = True, use_reloader=False)
