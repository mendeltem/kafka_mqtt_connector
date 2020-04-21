#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Apr 18 12:43:02 2020

@author: panda
"""
import json
import paho.mqtt.client as mqtt


#import library


class Mqtt_config:
    def __init__(self,
                 mqtt_broker_ip ='localhost',
                 mqtt_broker_port = 1883,
                 config_topic = "kafka/config/#"
                 ):

            self.config_data = []
            self.client = mqtt.Client()   
            self.client.on_connect = self.on_connect_config
            self.client.on_message = self.on_message_config
        
         
            try:
                self.client.connect(mqtt_broker_ip, mqtt_broker_port,  60)
            except Exception as ex:
                print("No Connection to MQQT Broker: ", ex)
        

    def on_connect_config(self, client, userdata, flags, rc):
        
        if rc==0:
            print("Connected with result code " + str(rc))
            # von config
            client.subscribe("kafka/config/#")
        else:
            print("Bad connection Returned code=",rc)
        
    #consumer.seek_to_end()
    #posts = db.test_collection
    def on_message_config(self, client, userdata, msg):
        global topics, config_data  
        m_decode=str(msg.payload.decode("utf-8","ignore"))
    #    print("data Received type",type(m_decode))
    #    print("data Received",m_decode)
    #    print("Converting from Json to Object")
        self.config_data=json.loads(m_decode) #decode json data    