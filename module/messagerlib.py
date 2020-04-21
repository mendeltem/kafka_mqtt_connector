#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Apr 18 16:36:07 2020

@author: panda
"""

from module.library import connect_mongoClient
from module.kafkalib import Message_info,change
from module.mqttlib import Mqtt_config
import time

class Messager:
    def __init__(self,
                 mqtt_broker_ip,
                 mqtt_broker_port,
                 kafka_broker_ip,
                 kafka_broker_port,
                 mongodb_ip,
                 mongodb_port,
                 db_name = 'kafka_database',
                 interval_tm_sec_list = [5,10,20,30],
                 wait_time = 2
                 ):

        self.db  =  connect_mongoClient(ip =mongodb_ip, port = mongodb_port, db_name =db_name )
        self.config_client =   Mqtt_config()
        self.config_client.client.loop_start()

        self.topic            = ''
        self.kafka_topics     = []        
        self.kafka_config     = {}
        self.config_data = []
        self.save_db = []
        self.wait_time = wait_time
        
        self.objects = [Message_info(interval_tm_sec,kafka_broker_ip,kafka_broker_port, i+1 ) for i, interval_tm_sec in enumerate(interval_tm_sec_list)] 
        
      
    def config_topics(self):
        
        time_cut =  round(time.perf_counter())
        time.sleep(1)
        
        while True: 
            
            try:
                if self.config_client.config_data:                   
                    for sensor in self.config_client.config_data["sensors"]:   
                        
                        if len(sensor['mqtt-topic'])  < 40:
                            self.topic = sensor['mqtt-topic'] +  self.config_client.config_data['device-uuid']
                        else:
                            self.topic = sensor['mqtt-topic'] 
                        
                        kafka_topic = ''.join([change(c, "/", "_") for c in self.topic])
                        kafka_unit = ''.join([change(c, "/", "_") for c in sensor["units"]])
        #                
                        if kafka_topic not in self.kafka_topics:
                            print("sucsribing to :", kafka_topic)
                            
                            self.kafka_topics.append(kafka_topic)
        
                            if self.kafka_topics:
                                self.objects[0].consumer.subscribe(self.kafka_topics)
                                                       
                            self.kafka_config.setdefault(kafka_topic, 0)
                            
                            self.kafka_config[kafka_topic] = {"unit": kafka_unit,
                             "location": self.config_client.config_data["location"]}         
                                 
            except ValueError:
                print("Bridge not started")
            
            
            if abs(time_cut -  round(time.perf_counter())) >= self.wait_time:
                break


    def calculate_stream(self):
        
        while True:
            
            self.config_topics()
            if self.kafka_topics: 
         
                for obj in self.objects:
                        obj.calculate_message(self.kafka_config, self.db, self.kafka_topics)
                        
    def showtopic(self):
        
        for obj in self.objects:
                obj.show_topic()
                        
                        
                