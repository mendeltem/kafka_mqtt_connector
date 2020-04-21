import io
import random
import pymongo
from flask import Response
from flask import render_template, Flask, request, redirect, url_for
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from matplotlib.figure import Figure
from app import app
import numpy as np

import pandas as pd
from pymongo import MongoClient
import time
import matplotlib.pyplot as plt
import matplotlib.cm as cm
from dateutil import parser


def collection_to_dataframe(collection):
    
    dataframe  = pd.DataFrame()
    
    cursor = collection.find({})
    
    for document in cursor:
    
        df = pd.DataFrame.from_dict( document['average_info']  , orient='index' ).T
        
        df['Topic'] = pd.Series(document['topic'] , index=df.index)
        df['intervall'] = pd.Series(document['intervall'] , index=df.index)
        
        datetime_time = parser.parse(document['average_info']["time"])
        df['Minutes'] = pd.Series(datetime_time.strftime('%M:%S'), index=df.index)
        df['Hour'] = pd.Series(datetime_time.strftime('%H:%M:%S'), index=df.index)
        df['Day'] = pd.Series(datetime_time.strftime('%d:%m:%Y'), index=df.index)
    
        dataframe =  dataframe.append(df)
    
    return dataframe
  
    
def creat_dataframe_List(db):

    collection_names_list = db.list_collection_names()
    collection_names_list.sort()

    return  [collection_to_dataframe(db[collecion]) for collecion in collection_names_list]  



mongoClient = MongoClient('localhost', 27017)
db = mongoClient['kafka_database']    

list_of_df =  creat_dataframe_List(db)




@app.route("/forward/", methods=['POST'])
def move_forward():
    #Moving forward code
    


    forward_message = "Moving Forward..." + str(list_of_df)
    return render_template('index.html', forward_message=forward_message);


@app.route('/plot.png')
def plot_png():
    fig = create_scatter_minute(list_of_df[0])
    output = io.BytesIO()
    FigureCanvas(fig).print_png(output)
    return Response(output.getvalue(), mimetype='image/png')

@app.route('/plot2.png')
def plot_png2():
    fig = create_scatter_minute(list_of_df[1])
    output = io.BytesIO()
    FigureCanvas(fig).print_png(output)
    return Response(output.getvalue(), mimetype='image/png')

@app.route('/plot3.png')
def plot_png3():
    fig = create_scatter_minute(list_of_df[2])
    output = io.BytesIO()
    FigureCanvas(fig).print_png(output)
    return Response(output.getvalue(), mimetype='image/png')


def create_scatter_minute(df):
    intervall = df['intervall'].values[0]
    
    list_of_colors = ["blue", "red", "yellow", "green", "pink"]
    color_map  = {}
    
    for i,y in zip(df["Topic"].unique(), list_of_colors):
        print(i,y)
        color_map.setdefault(i, y)
    
    colors = df.Topic.map(color_map)
    
    fig = Figure(figsize=(14,7))
    axis = fig.add_subplot(1, 1, 1)

    axis.set_title("Minute Interval every "+str(intervall) + " seconds")
    axis.scatter(df["Minutes"],df["average_value"],color=colors,   s=30, marker="+")
    axis.scatter(df["Minutes"],df["median_value"],color=colors,   s=30, marker="x")
    axis.scatter(df["Minutes"],df["max"],color=colors,   s=30, marker="^")
    axis.scatter(df["Minutes"],df["min"],color=colors,   s=30, marker="v")
    
#    axis.legend()
    
    leg = axis.legend(framealpha = 0, loc = 'best')
    for text in leg.get_texts():
        plt.setp(text, color = 'black')
    
    return fig


@app.route('/', methods=['GET', 'POST'])
@app.route('/index')
def index():
    user = {'username': 'Mendel'}
    
    
    list_of_df =  creat_dataframe_List(db)
    
    
    return render_template('index.html', title='Home', user=user, out = list_of_df)


#@app.route("/")
#def index():
#    return render_template('index.html')

