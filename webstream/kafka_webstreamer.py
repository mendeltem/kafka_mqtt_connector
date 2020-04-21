from flask import Flask, render_template, request
import requests
from bs4 import BeautifulSoup
import pandas as pd
from pymongo import MongoClient
import matplotlib.pyplot as plt
import matplotlib.cm as cm
from dateutil import parser
from matplotlib.figure import Figure
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from flask import Response
import io
import random
import re
import numpy as np
from threading import Timer
app = Flask(__name__)


#number_of_colors = 20
#
#list_of_colors = ["#"+''.join([random.choice('0123456789ABCDEF') for j in range(6)])
#         for i in range(number_of_colors)]
#    
db = "data"

db_collections  = []

def collection_to_dataframe(collection):
    
    dataframe  = pd.DataFrame()
    cursor = collection.find({})
    
    for document in cursor:
    
        df = pd.DataFrame.from_dict( document['average_info']  , orient='index' ).T
        df['Topic'] = pd.Series(document['topic'] , index=df.index)
        df['intervall'] = pd.Series(document['intervall'] , index=df.index)
        datetime_time = parser.parse(document['average_info']["time"])
        df['Seconds'] = pd.Series(datetime_time.strftime('%Hh:%Mm:%Ss'), index=df.index)
        df['Minutes'] = pd.Series(datetime_time.strftime('%Hh:%Mm:%Ss'), index=df.index)
        df['Hour'] = pd.Series(datetime_time.strftime('%H:%M %d.%m  '), index=df.index)
        df['Day'] = pd.Series(datetime_time.strftime('%d:%m:%Y'), index=df.index)
    
        dataframe =  dataframe.append(df)
    
    return dataframe

    
def creat_dataframe_List(db):
    collection_names_list = db.list_collection_names()
    collection_names_list.sort()
    return  [collection_to_dataframe(db[collecion]) for collecion in collection_names_list]  



def creat_dataframe_dict(db):

    topic_list      = [] 
    list_of_rooms   = []
    list_of_units   = []
    rooms_dict      = {}
    
    collection_names_list = db.list_collection_names()
    collection_names_list.sort()
    
    for collecion in collection_names_list:
        x = re.match("^.+?(?=\-time_[0-9]$|$)", collecion)
        if x[0] not in topic_list:
            topic_list.append(x[0])
                
            
    for topic in topic_list:
        x = re.match("^.+?(?=\-.*$|$)", topic)     
        if x[0] not in list_of_rooms:
            list_of_rooms.append(x[0])  
            
    
    for topic in topic_list:
        x = re.search(".+-(.*$)", topic).group(1)     
        if x not in list_of_units:
            list_of_units.append(x)  
            
            
    for collecion in collection_names_list:       
        for room in  list_of_rooms:
            if  room in  collecion:
                rooms_dict.setdefault(room, {})  
                for unit in list_of_units:
                    if unit in collecion:
                        rooms_dict[room].setdefault(unit,[])
                        rooms_dict[room][unit].append(collection_to_dataframe(db[collecion]))
                        
    return rooms_dict,list_of_rooms,list_of_units      
 

def update_data(interval):
    Timer(interval, update_data, [interval]).start()
    
    mongoClient = MongoClient('localhost', 27017)
    global db
    
    db = mongoClient['kafka_database']
    
    global db_collections
    
    db_collections = db.list_collection_names()
    
#    
    db_collections.sort()
    
 
def create_temperatur_scatter_seconds(rooms_dict, room, unit , time):
    
    df = rooms_dict[room][unit][time]
    
    intervall = df['intervall'].values[0]
    
    location  = df['location'].values[0]     
    
    unit    =  df['unit'].values[0]
         
    fig = Figure(figsize=(14,7))
    axis = fig.add_subplot(1, 1, 1)
    
    axis.set_ylabel(unit)
    
    axis.set_xlabel("Time ")

    axis.set_title("Temperatur In "+ str(location) +" Interval every "+str(intervall) + " seconds ")
    
    axis.plot(df["Seconds"],df["average_value"], 'r', zorder=1, lw=1 , color = "blue")
    if len(df["average_value"]) == 1:
        axis.scatter(df["Seconds"],df["average_value"],   s=10, marker="+" , zorder=2, color = "blue")
  
    axis.plot(df["Seconds"],df["median_value"], 'r', zorder=1, lw=1 , color = "green")
    if len(df["median_value"]) == 1:
        axis.scatter(df["Seconds"],df["median_value"],   s=10, marker="+" , zorder=2, color = "green")
        
        
    axis.scatter(df["Seconds"],df["max"], s=20, marker="^", color = "red")
    axis.scatter(df["Seconds"],df["min"], s=20, marker="v", color =  "red")
    
    max_value = np.max(df["max"])
    min_value = np.min(df["min"])
    
    major_ticks = np.arange(min_value - 10, max_value + 10, 1)
 
    axis.set_yticks(major_ticks)

    axis.grid(which='both')
      
    leg = axis.legend(framealpha = 0, loc = 'upper right')
    for text in leg.get_texts():
        plt.setp(text, color = 'black')
    
    return fig


def create_temperatur_scatter_minutes(rooms_dict, room, unit , time):
    
    df = rooms_dict[room][unit][time]
    
    intervall = df['intervall'].values[0]
    
    location  = df['location'].values[0]     
    
    unit    =  df['unit'].values[0]
         
    fig = Figure(figsize=(14,7))
    axis = fig.add_subplot(1, 1, 1)
    
    axis.set_ylabel(unit)
    
    axis.set_xlabel("Time ")

    axis.set_title("Temperatur In "+ str(location) +" Interval every "+str(round(intervall/60,1)) + " Minutes ")
    
    axis.plot(df["Minutes"],df["average_value"], 'r', zorder=1, lw=1 , color = "blue")
    
    axis.scatter(df["Minutes"],df["average_value"],   s=10, marker="+" , zorder=2, color = "blue")
    if len(df["average_value"]) == 1:
        axis.scatter(df["Minutes"],df["average_value"],   s=10, marker="+" , zorder=2, color = "blue")
#    axis.scatter(df["Seconds"],df["median_value"],   s=20, marker="x", color = color)
#    
    
    axis.plot(df["Minutes"],df["median_value"], 'r', zorder=1, lw=1 , color = "green")
    
    if len(df["median_value"]) == 1:
        axis.scatter(df["Minutes"],df["median_value"],   s=10, marker="+" , zorder=2, color = "green")
        
    
    axis.scatter(df["Minutes"],df["max"], s=20, marker="^", color = "red")
    axis.scatter(df["Minutes"],df["min"], s=20, marker="v", color =  "red")
    
    max_value = np.max(df["max"])
    min_value = np.min(df["min"])
    
    major_ticks = np.arange(min_value - 10, max_value + 10, 1)
 
    axis.set_yticks(major_ticks)

    axis.grid(which='both')
      
    leg = axis.legend(framealpha = 0, loc = 'upper right')
    for text in leg.get_texts():
        plt.setp(text, color = 'black')
    
    return fig


def create_brightness_scatter_seconds(rooms_dict, room, unit , time):
    
    df = rooms_dict[room][unit][time]
    
    intervall = df['intervall'].values[0]
    
    location  = df['location'].values[0]     
    
    unit    =  df['unit'].values[0]
         
    fig = Figure(figsize=(14,7))
    axis = fig.add_subplot(1, 1, 1)
    
    axis.set_ylabel(unit)
    
    axis.set_xlabel("Time ")

    axis.set_title("Brighness In "+ str(location) +" Interval every "+str(intervall) + " seconds ")
    
    axis.plot(df["Seconds"],df["average_value"], 'r', zorder=1, lw=1 , color = "blue")
    
    if len(df["average_value"]) == 1:
        axis.scatter(df["Seconds"],df["average_value"],   s=10, marker="+" , zorder=2, color = "blue")
#    axis.scatter(df["Seconds"],df["median_value"],   s=20, marker="x", color = color)
    
    axis.plot(df["Seconds"],df["median_value"], 'r', zorder=1, lw=1 , color = "green")
    
    if len(df["median_value"]) == 1:
        axis.scatter(df["Seconds"],df["median_value"],   s=10, marker="+" , zorder=2, color = "green")
        
        
    axis.scatter(df["Seconds"],df["max"], s=20, marker="^", color = "red")
    axis.scatter(df["Seconds"],df["min"], s=20, marker="v", color =  "red")
    
    max_value = np.max(df["max"])
    min_value = np.min(df["min"])
    
    major_ticks = np.arange(min_value - 10, max_value + 10, 1)
 
    axis.set_yticks(major_ticks)

    axis.grid(which='both')
      
    leg = axis.legend(framealpha = 0, loc = 'upper right')
    for text in leg.get_texts():
        plt.setp(text, color = 'black')
    
    return fig

def create_brightness_scatter_minutes(rooms_dict, room, unit , time):
    
    df = rooms_dict[room][unit][time]
    
    intervall = df['intervall'].values[0]
    
    location  = df['location'].values[0]     
    
    unit    =  df['unit'].values[0]
         
    fig = Figure(figsize=(14,7))
    axis = fig.add_subplot(1, 1, 1)
    
    axis.set_ylabel(unit)
    
    axis.set_xlabel("Time ")

    axis.set_title("Brighness In "+ str(location) +" Interval every "+str(round(intervall/60,1)) + " minutes ")
    
    axis.plot(df["Minutes"],df["average_value"], 'r', zorder=1, lw=1 , color = "blue")
    
    if len(df["average_value"]) == 1:
        axis.scatter(df["Minutes"],df["average_value"],   s=10, marker="+" , zorder=2, color = "blue")


    axis.plot(df["Minutes"],df["median_value"], 'r', zorder=1, lw=1 , color = "green")
    if len(df["average_value"]) == 1:
        axis.scatter(df["Minutes"],df["median_value"],   s=10, marker="+" , zorder=2, color = "green")
        
        
    axis.scatter(df["Minutes"],df["max"], s=20, marker="^", color = "red")
    axis.scatter(df["Minutes"],df["min"], s=20, marker="v", color =  "red")
    
    max_value = np.max(df["max"])
    min_value = np.min(df["min"])
    
    major_ticks = np.arange(min_value - 10, max_value + 10, 1)
 
    axis.set_yticks(major_ticks)

    axis.grid(which='both')
      
    leg = axis.legend(framealpha = 0, loc = 'upper right')
    for text in leg.get_texts():
        plt.setp(text, color = 'black')
    
    return fig


def create_scatter_seconds(rooms_dict, room, unit , time):
    
    df = rooms_dict[room][unit][time]
    
    intervall = df['intervall'].values[0]
    
    location  = df['location'].values[0]     
    
    unit    =  df['unit'].values[0]
         
    fig = Figure(figsize=(14,7))
    axis = fig.add_subplot(1, 1, 1)
    
    axis.set_ylabel(unit)
    
    axis.set_xlabel("Time ")

    axis.set_title( str(unit)+" In "+ str(location) +" Interval every "+str(intervall) + " seconds ")
    
    axis.plot(df["Seconds"],df["average_value"], 'r', zorder=1, lw=1 , color = "blue")  
    axis.plot(df["Seconds"],df["median_value"], 'r', zorder=1, lw=1 , color = "green")
    
    if len(df["average_value"]) == 1:
        axis.scatter(df["Seconds"],df["average_value"],   s=10, marker="+" , zorder=2, color = "blue")    
    if len(df["median_value"]) == 1:
        axis.scatter(df["Seconds"],df["median_value"],   s=10, marker="+" , zorder=2, color = "green") 
        
    axis.scatter(df["Seconds"],df["max"], s=20, marker="^", color = "red")
    axis.scatter(df["Seconds"],df["min"], s=20, marker="v", color =  "red")
    
    max_value = np.max(df["max"])
    min_value = np.min(df["min"])
    
    major_ticks = np.arange(min_value - 10, max_value + 10, 1)
 
    axis.set_yticks(major_ticks)

    axis.grid(which='both')
      
    leg = axis.legend(framealpha = 0, loc = 'upper right')
    for text in leg.get_texts():
        plt.setp(text, color = 'black')
    
    return fig

def create_scatter_minutes(rooms_dict, room, unit , time):
    
    df = rooms_dict[room][unit][time]
    
    intervall = df['intervall'].values[0]
    
    location  = df['location'].values[0]     
    
    unit    =  df['unit'].values[0]
         
    fig = Figure(figsize=(14,7))
    axis = fig.add_subplot(1, 1, 1)
    
    axis.set_ylabel(unit)
    
    axis.set_xlabel("Time ")

    axis.set_title( str(unit)+" In "+ str(location) +" Interval every "+str(round(intervall/60,1)) +  " Minutes ")
    
    axis.plot(df["Minutes"],df["average_value"], 'r', zorder=1, lw=1 , color = "blue")   
    axis.plot(df["Minutes"],df["median_value"], 'r', zorder=1, lw=1 , color = "green")
    if len(df["average_value"]) == 1:
        axis.scatter(df["Minutes"],df["average_value"],   s=10, marker="+" , zorder=2, color = "blue")    
    if len(df["median_value"]) == 1:
        axis.scatter(df["Minutes"],df["median_value"],   s=10, marker="+" , zorder=2, color = "green")           
        
    axis.scatter(df["Minutes"],df["max"], s=20, marker="^", color = "red")
    axis.scatter(df["Minutes"],df["min"], s=20, marker="v", color =  "red")
    
    max_value = np.max(df["max"])
    min_value = np.min(df["min"])
    
    major_ticks = np.arange(min_value - 10, max_value + 10, 1)
 
    axis.set_yticks(major_ticks)

    axis.grid(which='both')
      
    leg = axis.legend(framealpha = 0, loc = 'upper right')
    for text in leg.get_texts():
        plt.setp(text, color = 'black')
    
    return fig
# update data every 10 second
update_data(10)

@app.route('/')
def home():
    return render_template('home.html' , lenght = len(db_collections), liste  =  db_collections )

@app.route("/room1_unit1_time1")
def room1_unit1_time1():
    return render_template('room1_unit1_time1.html' , title  =  'room1_unit1_time1' )

@app.route("/room1_unit1_time2")
def room1_unit1_time2():
    return render_template('room1_unit1_time2.html' , title  =  'room1_unit1_time2')

@app.route("/room1_unit2_time1")
def room1_unit2_time1():
    return render_template('room1_unit2_time1.html' , title  =  'room1_unit2_time1')

@app.route("/room1_unit2_time2")
def room1_unit2_time2():
    return render_template('room1_unit2_time2.html' , title  =  'room1_unit2_time2')


@app.route("/room2_unit1_time1")
def room2_unit1_time1():
    return render_template('room2_unit1_time1.html' , title  =  'room2_unit1_time1' )

@app.route("/room2_unit1_time2")
def room2_unit1_time2():
    return render_template('room2_unit1_time2.html' , title  =  'room2_unit1_time2')

@app.route("/room2_unit2_time1")
def room2_unit2_time1():
    return render_template('room2_unit2_time1.html' , title  =  'room2_unit2_time1')

@app.route("/room2_unit2_time2")
def room2_unit2_time2():
    return render_template('room2_unit2_time2.html' , title  =  'room2_unit2_time2')


@app.route('/building1_room2-lux-time_1.png', methods=['GET'])
def room1_unit1_time1_png():
    rooms_dict,list_of_rooms, list_of_units = creat_dataframe_dict(db)
    
    room = list_of_rooms[0]
    unit = list_of_units[1]
    time = 0
 
    if "Celsius" in unit:
    
        fig = create_temperatur_scatter_seconds( rooms_dict,
                                                 room,
                                                 unit,
                                                 time)
    if "lux" in unit:
        
        fig = create_brightness_scatter_seconds( rooms_dict,
                                                 room,
                                                 unit,
                                                 time)
        
    else:
        
        fig = create_scatter_seconds( rooms_dict,
                                                 room,
                                                 unit,
                                                 time) 
        
    output = io.BytesIO()
    FigureCanvas(fig).print_png(output)

    return Response(output.getvalue(), mimetype='image/png') 






@app.route('/building1_room2-lux-time_2.png', methods=['GET'])
def room1_unit1_time2_png():
    rooms_dict,list_of_rooms, list_of_units = creat_dataframe_dict(db)
    
    room = list_of_rooms[0]
    unit = list_of_units[1]
    time = 1
 
    if "Celsius" in unit:
    
        fig = create_temperatur_scatter_minutes( rooms_dict,
                                                 room,
                                                 unit,
                                                 time)
    if "lux" in unit:
        
        fig = create_brightness_scatter_minutes( rooms_dict,
                                                 room,
                                                 unit,
                                                 time)
    else:
        

        fig = create_scatter_minutes( rooms_dict,
                                                 room,
                                                 unit,
                                                 time)
          
    output = io.BytesIO()
    FigureCanvas(fig).print_png(output)

    return Response(output.getvalue(), mimetype='image/png') 


@app.route('/building1_room2-CelsiusScale-time_1.png', methods=['GET'])
def room1_unit2_time1_png():
    rooms_dict,list_of_rooms, list_of_units = creat_dataframe_dict(db)
    
    room = list_of_rooms[0]
    unit = list_of_units[0]
    time = 0
 
    if "Celsius" in unit:
    
        fig = create_temperatur_scatter_seconds( rooms_dict,
                                                 room,
                                                 unit,
                                                 time)
    if "lux" in unit:
        
        fig = create_brightness_scatter_seconds( rooms_dict,
                                                 room,
                                                 unit,
                                                 time)
        
    else:
        
        fig = create_scatter_seconds( rooms_dict,
                                                 room,
                                                 unit,
                                                 time) 
        
    
    output = io.BytesIO()
    FigureCanvas(fig).print_png(output)

    return Response(output.getvalue(), mimetype='image/png') 


@app.route('/building1_room2-CelsiusScale-time_2.png', methods=['GET'])
def room1_unit2_time2_png():
    rooms_dict,list_of_rooms, list_of_units = creat_dataframe_dict(db)
    
    room = list_of_rooms[0]
    unit = list_of_units[0]
    time = 1
 
    if "Celsius" in unit:
    
        fig = create_temperatur_scatter_minutes( rooms_dict,
                                                 room,
                                                 unit,
                                                 time)
    if "lux" in unit:
        
        fig = create_brightness_scatter_minutes( rooms_dict,
                                                 room,
                                                 unit,
                                                 time)
    else:
        

        fig = create_scatter_minutes( rooms_dict,
                                                 room,
                                                 unit,
                                                 time)
        
    
    output = io.BytesIO()
    FigureCanvas(fig).print_png(output)

    return Response(output.getvalue(), mimetype='image/png') 


@app.route('/building1_room7-CelsiusScale-time_1.png', methods=['GET'])
def room2_unit1_time1_png():
    rooms_dict,list_of_rooms, list_of_units = creat_dataframe_dict(db)
    
    room = list_of_rooms[1]
    unit = list_of_units[0]
    time = 0
 
    if "Celsius" in unit:
    
        fig = create_temperatur_scatter_seconds( rooms_dict,
                                                 room,
                                                 unit,
                                                 time)
    if "lux" in unit:
        
        fig = create_brightness_scatter_seconds( rooms_dict,
                                                 room,
                                                 unit,
                                                 time)
        
    else:
        
        fig = create_scatter_seconds( rooms_dict,
                                                 room,
                                                 unit,
                                                 time) 
        
    output = io.BytesIO()
    FigureCanvas(fig).print_png(output)

    return Response(output.getvalue(), mimetype='image/png') 


@app.route('/building1_room7-CelsiusScale-time_2.png', methods=['GET'])
def room2_unit1_time2_png():
    rooms_dict,list_of_rooms, list_of_units = creat_dataframe_dict(db)
    
    room = list_of_rooms[1]
    unit = list_of_units[0]
    time = 1
 
    if "Celsius" in unit:
    
        fig = create_temperatur_scatter_minutes( rooms_dict,
                                                 room,
                                                 unit,
                                                 time)
    if "lux" in unit:
        
        fig = create_brightness_scatter_minutes( rooms_dict,
                                                 room,
                                                 unit,
                                                 time)
    else:
        

        fig = create_scatter_minutes( rooms_dict,
                                                 room,
                                                 unit,
                                                 time)
          
    output = io.BytesIO()
    FigureCanvas(fig).print_png(output)

    return Response(output.getvalue(), mimetype='image/png') 


@app.route('/building1_room7-lux-time_1.png', methods=['GET'])
def room2_unit2_time1_png():
    rooms_dict,list_of_rooms, list_of_units = creat_dataframe_dict(db)
    
    room = list_of_rooms[1]
    unit = list_of_units[1]
    time = 0
 
    if "Celsius" in unit:
    
        fig = create_temperatur_scatter_seconds( rooms_dict,
                                                 room,
                                                 unit,
                                                 time)
    if "lux" in unit:
        
        fig = create_brightness_scatter_seconds( rooms_dict,
                                                 room,
                                                 unit,
                                                 time)
        
    else:
        
        fig = create_scatter_seconds( rooms_dict,
                                                 room,
                                                 unit,
                                                 time) 
        
    
    output = io.BytesIO()
    FigureCanvas(fig).print_png(output)

    return Response(output.getvalue(), mimetype='image/png') 


@app.route('/building1_room7-lux-time_2.png', methods=['GET'])
def room2_unit2_time2_png():
    rooms_dict,list_of_rooms, list_of_units = creat_dataframe_dict(db)
    
    room = list_of_rooms[1]
    unit = list_of_units[1]
    time = 1
 
    if "Celsius" in unit:
    
        fig = create_temperatur_scatter_minutes( rooms_dict,
                                                 room,
                                                 unit,
                                                 time)
    if "lux" in unit:
        
        fig = create_brightness_scatter_minutes( rooms_dict,
                                                 room,
                                                 unit,
                                                 time)
    else:
        

        fig = create_scatter_minutes( rooms_dict,
                                                 room,
                                                 unit,
                                                 time)
        
    
    output = io.BytesIO()
    FigureCanvas(fig).print_png(output)

    return Response(output.getvalue(), mimetype='image/png') 



if __name__ == '__main__':   
    app.run(debug=True, host='0.0.0.0', port='5000')

