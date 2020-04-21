#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""


@author: temuulen
"""

import json

from pymongo import MongoClient
import paho.mqtt.client as mqtt
import pprint


def connect_mongoClient(ip ='localhost', port = 27017 , db_name = 'kafka_database' ):
    mongoClient = None
    try:
        mongoClient = MongoClient(ip, port)
        db = mongoClient[db_name]
            
    except Exception as ex:
        print('Exception while connecting Mongo')
        print(str(ex))
    finally:
        return db
    

def on_connect_config(client, userdata, flags, rc):
    
    if rc==0:
        print("Connected with result code " + str(rc))
        # von config
        client.subscribe("kafka/config/#")
    else:
        print("Bad connection Returned code=",rc)
        
#consumer.seek_to_end()
#posts = db.test_collection
def on_message_config(client, userdata, msg):
    global topics, config_data  
    m_decode=str(msg.payload.decode("utf-8","ignore"))
#    print("data Received type",type(m_decode))
#    print("data Received",m_decode)
#    print("Converting from Json to Object")
    config_data=json.loads(m_decode) #decode json data    

def connect_mqttClient(mqtt_broker_ip ='localhost', mqtt_broker_port = 1883  ):
    client = mqtt.Client()   
    client.on_connect = on_connect_config
    client.on_message = on_message_config
        
    try:
        client.connect(mqtt_broker_ip, mqtt_broker_port,  60)
    except Exception as ex:
        print("No Connection to MQQT Broker")
            
    except Exception as ex:
        print('Exception while connecting Mongo')
        print(str(ex))
    finally:
        return client