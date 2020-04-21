#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Apr 18 12:24:55 2020

@author: panda
"""
from kafka import KafkaConsumer,KafkaProducer
import msgpack
import json
import time
from pymongo import MongoClient
import pprint
import heapq


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
    
    
class streamMedian:
    def __init__(self):
        self.minHeap, self.maxHeap = [], []
        self.cumulative_sum = 0.0             # new instance variable
        self.N=0


    def insert(self, num):
        if self.N%2==0:
            heapq.heappush(self.maxHeap, -1*num)
            self.N+=1
            if len(self.minHeap)==0:
                return
            if -1*self.maxHeap[0]>self.minHeap[0]:
                toMin=-1*heapq.heappop(self.maxHeap)
                toMax=heapq.heappop(self.minHeap)
                heapq.heappush(self.maxHeap, -1*toMax)
                heapq.heappush(self.minHeap, toMin)
        else:
            toMin=-1*heapq.heappushpop(self.maxHeap, -1*num)
            heapq.heappush(self.minHeap, toMin)
            self.N+=1

    # median code...
    def getMedian(self):
        if self.N%2==0:
            return (-1*self.maxHeap[0]+self.minHeap[0])/2.0
        else:
            return -1*self.maxHeap[0]
    
    
def getAvg(prev_avg, new_value, n): 
    """streaming average"""
    return ((prev_avg * n + new_value) / (n + 1)); 


def change(f , char1, char2):
    """replace a character with another if containes
    
    return character
    """
    return char2 if f == char1 else f


def connect_kafka_producer(ip ='localhost:9092',  type_of_message = 1):
    _producer = None
    try:
        if type_of_message == 1:
            _producer = KafkaProducer(bootstrap_servers=[ip],
                                value_serializer=msgpack.dumps)
        if type_of_message == 2:
            _producer = KafkaProducer(bootstrap_servers=[ip],
              value_serializer=lambda v: json.dumps(v).encode('utf-8'))
            
        
        connect_kafka_producer
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer


def connect_kafka_consumer(ip ='localhost:9092'):
    _consumer = None
    try:
        _consumer = KafkaConsumer(bootstrap_servers=['localhost:9092'],
                         consumer_timeout_ms=1000
                         ) 
            
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _consumer


class Message_info:
    def __init__(self,
                 interval_tm_sec,
                 kafka_broker_ip,
                 kafka_broker_port,
                 time_number = 1
                 ):
        
        
        self.pp = pprint.PrettyPrinter(indent=1)
        self.time_dict = {}
        self.it  = interval_tm_sec
        
        self.time_number  = time_number
        
        
        self.consumer = connect_kafka_consumer()
      
        if not time_number == 1:
            self.consumer.subscribe("time_" + str(time_number -1))
            
            self.send_topic_name = "time_" + str(time_number)
            
        else:
            self.send_topic_name = "time_1"
                    
        self.kafka_minute_saver = connect_kafka_producer(kafka_broker_ip + ":" + \
                                  str(kafka_broker_port), 2)            
        
        self.timestamps       = {}
        self.values           = {}
        self.message_counter  = {}
        
        self.calculated_message_counter  = {}
        
        self.median_object    = {}
        self.media_values     = {}
        self.min_values       = {}
        self.avg_values       = {}
        
        
        self.time_list           = {} 
        self.message_counterlist = {} 
        self.values_list         = {} 
        self.avg_values_list     = {} 
        
        self.min_list            = {} 
        self.max_list            = {} 
        
        self.median_list         = {} 
        
        self.calculated_avg_values       = {}
        
        self.max_values       = {}
        self.all_info         = {}

        
        
        self.location         = {}
        self.unit             = {}       
        
        
    def calculate_message(self, kafka_config, db,  kafka_topics):
        
        for i,message in enumerate(self.consumer):
            record = json.loads(message.value)
            
            
    
            if "topic" in record.keys():
                unit = record["unit"]
                location = record["location"]
                
                saved = 1
            else:
                unit = kafka_config[message.topic]["unit"]
                location = kafka_config[message.topic]["location"]
                
                saved = 0
                
            topic = location +"-" +unit    
                
            #fertig    
            self.message_counter.setdefault(topic, 0)    
            self.timestamps.setdefault(topic, 0)    
            self.timestamps[topic] = message.timestamp    
            self.message_counter[topic] += 1 
            self.values.setdefault(topic, 0)
            self.avg_values.setdefault(topic, 0)     
            self.median_object.setdefault(topic, streamMedian())
            
            #debug controll
            self.message_counterlist.setdefault(topic, [])  
            self.values_list.setdefault(topic, [])  
            self.avg_values_list.setdefault(topic, [])  
            self.min_list.setdefault(topic, [])   
            self.max_list.setdefault(topic, [])          
            self.time_list.setdefault(topic, [])    
            self.median_list.setdefault(topic, [])   
            

            if saved == 1:
                self.values[topic]  = record["average_info"]["average_value"]
                min_value           = record["average_info"]["min"]
                print("min", min_value)
                print("counter ", self.message_counter[topic] )
                max_value           = record["average_info"]["max"]
                median_value        = record["average_info"]["median_value"]
                
            else:    
                self.values[topic]  = round(float(record["value"]), 2)
                
                min_value           = self.values[topic] 
                max_value           = self.values[topic] 
                median_value        = self.values[topic] 
                
            self.min_values.setdefault(topic, min_value)   
            self.max_values.setdefault(topic, max_value)   
            self.media_values.setdefault(topic, 0) 
            self.avg_values.setdefault(topic, 0)
            
            time_of_record = time.asctime(time.localtime(self.timestamps[topic]/1000.))
            
            self.median_object[topic].insert(median_value)
            
            self.media_values[topic] = round(self.median_object[topic].getMedian(),
                                                2)     
            avg = getAvg(self.avg_values[topic], 
                         self.values[topic], 
                         self.message_counter[topic] - 1)
            
            self.avg_values[topic] = round(avg, 2)   
            
            if self.message_counter[topic] == 1 and saved:
                self.min_values[topic]   = min_value
                self.max_values[topic]   = max_value  

 
            if self.min_values[topic] > self.values[topic]: 
                self.min_values[topic] = min_value
            
            if self.max_values[topic] < self.values[topic]: 
                self.max_values[topic] = max_value   
                     
            self.message_counterlist[topic].append(self.message_counter[topic])
            self.values_list[topic].append(self.values[topic])   
            self.avg_values_list[topic].append(self.avg_values[topic])  
            self.min_list[topic].append(self.min_values[topic])  
            self.max_list[topic].append(self.max_values[topic])  
            
            self.median_list[topic].append(self.media_values[topic] )  
            
            
            self.time_list[topic].append(time_of_record)  
            
            self.all_info[topic] = {
                   "average_value"      : self.avg_values[topic], 
                   "median_value"       : self.media_values[topic], 
                   "unit"               : unit,
                   "timestamp"          : self.timestamps[topic],
                   "min"                : self.min_values[topic],
                   "max"                : self.max_values[topic],
                   "location"           : location,
                   "message_counter"    : self.message_counter[topic],
                   "time"               : time_of_record
            }

            self.get_intervall(
                                topic,
                                self.timestamps[topic], 
                                self.all_info[topic],
                                db,
                                kafka_topics
                                )
              
            
    def get_intervall(self, topic, new_time, value , db ,  kafka_topics):
             
        transtime = new_time/ 1000
   
        self.time_dict.setdefault(topic,{"old_time" : transtime,
                                    "new_time" : transtime}
                                 )
        self.time_dict[topic].update({"new_time" : transtime}) 
        
        intervall = abs(self.time_dict[topic]["old_time"] - self.time_dict[topic]["new_time"])
#            
        if intervall >=  self.it:
            print("sending intervall: ", round(intervall) , " sec")
            self.time_dict[topic].update({"old_time" : self.time_dict[topic]["new_time"]}) 
            
            self.message_counter[topic] = 0
            
            self.avg_values[topic]      =  self.values[topic]
            self.min_values[topic]      =  self.values[topic]
            self.max_values[topic]      =  self.values[topic]
            self.median_object[topic]= streamMedian()
     
            post =  {  "topic"            : topic,
                       "average_info"     : value,
                       "intervall"        : round(intervall),
                       "save_topic"       : self.send_topic_name,
                       'location'         : value['location' ],
                       'unit'             : value['unit' ]
                       
                    }
            
            
            location = ''.join([change(c, "/", "_") for c in value['location' ]])
                     
            print("post ")
            
            self.pp.pprint(post)  
            
            self.kafka_minute_saver.send(self.send_topic_name , 
                                         post
                                         )   
            
            self.kafka_minute_saver.flush()
            
            collection_names = db.list_collection_names()
            
            
            save_topic_name = location +"-" + value['unit' ] + "-" +self.send_topic_name 
            
            print("save_topic_name", save_topic_name)
#            
            
            if save_topic_name in collection_names:
                collection =  db[save_topic_name]
            else: 
                db.create_collection(save_topic_name, 
                                     capped = True, 
                                     size = 10000, 
                                     max = 10 )
                collection =  db[save_topic_name]
                   
            collection.insert_one(post)
            
    def show_visibletopic(self):
        print("all visible topics:", self.consumer.topics())        
            
            

    def show_topic(self):

            
        print("all assigned topics topics:",self.consumer.assignment())      
        