#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Apr 18 12:43:02 2020

@author: panda
"""
import json
import paho.mqtt.client as mqtt
import time
from threading import Timer
from module.kafkalib import change,connect_kafka_producer
from kafka.errors import KafkaError

#import library


class Connector:
    def __init__(self,
                 mqtt_broker_ip ='localhost',
                 mqtt_broker_port = 1883,
                 kafka_broker_ip = 'localhost',
                 kafka_broker_port = 9092,
                 
                 config_topic = "kafka/config/#"
                 ):
        
        
            self.mqtt_broker_ip   = mqtt_broker_ip
            self.mqtt_broker_port = mqtt_broker_port

            self.config_data               = []
            self.config_cllient            = mqtt.Client()   
            self.config_cllient.on_connect = self.on_connect_config
            self.config_cllient.on_message = self.on_message_config
                    

            
            self.message_client            = mqtt.Client()   
            self.message_client.on_connect = self.on_connect
            self.message_client.on_message = self.on_message
            
            
            self.topics = []
            
            
            self.producer = connect_kafka_producer(kafka_broker_ip + ":" + \
                                              str(kafka_broker_port), 2)
            
            
            self.message_client.disconnect()
        

            try:
                self.config_cllient.connect(self.mqtt_broker_ip, 
                                            self.mqtt_broker_port, 
                                            60)
            except Exception as ex:
                print("No Connection to MQQT Broker: ", ex)

            
        
            self.config_cllient.loop_start()
            

    def on_connect_config(self, config_cllient, userdata, flags, rc):
        
        if rc==0:
            print("Connected with result code " + str(rc))
            # von config
            config_cllient.subscribe("kafka/config/#")
        else:
            print("Bad connection Returned code=",rc)
        
    #consumer.seek_to_end()
    #posts = db.test_collection
    def on_message_config(self, config_cllient, userdata, msg):
        global topics, config_data  
        m_decode=str(msg.payload.decode("utf-8","ignore"))
    #    print("data Received type",type(m_decode))
    #    print("data Received",m_decode)
    #    print("Converting from Json to Object")
        self.config_data=json.loads(m_decode) #decode json data    

    def on_connect(self, message_client, userdata, flags, rc):
        
        if rc==0:
            print("Connected with result code " + str(rc))
            # von config
            #os.system('clear')
            print("subscribe to : ", self.topics)
            message_client.subscribe(self.topics) 
        else:
            print("Bad connection Returned code=",rc)
    
    def on_message(self,message_client, userdata, msg):
    
        topic=msg.topic
        p = [topic, msg.payload]
#        interval = 1
#        Timer(interval, self.send_message_to_kafka, [p]).start()
        
        self.send_message_to_kafka(p)
    
    def start(self):

        
        while True: 
            
            if self.config_data:
            
                try:
        
                    for sensor in self.config_data["sensors"]: 
        
                        if len(sensor['mqtt-topic'])  < 40:
                            topic = sensor['mqtt-topic'] +  self.config_data['device-uuid']
                        else:
                            topic = sensor['mqtt-topic'] 
                            
                        if (topic,0) not in self.topics:
                            
             
                            self.topics.append((topic,0))
                            
                            self.message_client.loop_stop() 

                            try:
                                self.message_client.connect(self.mqtt_broker_ip,
                                                            self.mqtt_broker_port, 
                                                            60)    
                            except Exception as ex:
                                print("No Connection  to MQQT Broker: ", ex)    

                            self.message_client.loop_start()
                            
                            
                            
                            
                            
                except ValueError:
                    print("Bridge not started")
                
#                
#            if abs(time_cut -  round(time.perf_counter())) >= 5:
#                break
#                
#        print("self.topics " ,self.topics)
#               
#        self.message_client.loop_start() 


    def send_message_to_kafka(self, p):
        """
        Send message to kafka consumer
        """
        mqtttopic = p[0]
        message = p[1]
        
        print("Kafka-MQTT Connector")
        
        print("Mqtt Topic: " , mqtttopic)
        
        kafka_topic = ''.join([change(c, "/", "_") for c in mqtttopic])

        kafka_message = message.decode("utf-8")
        future = self.producer.send(kafka_topic, {"value": kafka_message
                                        })    
        #Block for 'synchronous' sends
        try:
            record_metadata = future.get(timeout=10)
        except KafkaError:
            # Decide what to do if produce request failed...
            print("send error")
            pass
            
        # Successful result returns assigned partition and offset
        print ("topic:     ",record_metadata.topic)
        print ("partition: ",record_metadata.partition)
        print ("offset:    ",record_metadata.offset)
#        
##            
        self.producer.flush()
        
        print("Value:",kafka_message)
        
        
        
        
#    def send_message(self):
#        
#        while True:
##        print("sending")
##        
#            if self.topics:
#               self.message_client.loop_start()
#               time.sleep(10)
#               self.message_client.loop_stop()
#            
        
#        
