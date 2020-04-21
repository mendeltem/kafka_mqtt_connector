#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan 28 06:11:28 2020

@author: panda
"""

import paho.mqtt.client as mqtt
from kafka import KafkaProducer
from kafka.errors import KafkaError
import msgpack
import json
import os
import time
from threading import Timer
#from module.kafkalib import change, connect_kafka_producer
import configparser
from module.mqttlib import Mqtt_config

from module.connectorlib import Connector


kafka_broker_ip = 'localhost'
kafka_broker_port = 9092

#server mqqt broker
#ip = "localhost"
#
#ip = "192.168.137.62"

data = configparser.ConfigParser()
data.sections()
data.read('config.ini')

mqtt_broker_ip              =  data["DATA"]["mqtt_broker_ip"]
mqtt_broker_port            =  int(data["DATA"]["mqtt_broker_port"])

kafka_broker_ip             =  data["DATA"]["kafka_broker_ip"]
kafka_broker_port           =  int(data["DATA"]["kafka_broker_port"])

db_name                     =  data["DATA"]["db_name"]
interval_tm_sec_list        = json.loads(data["DATA"]["interval_tm_sec_list"])

config_topic                =  data["DATA"]["config_topic"]



if __name__ == "__main__":
    
    connector =   Connector( mqtt_broker_ip,
                             mqtt_broker_port,
                             kafka_broker_ip,
                             kafka_broker_port, 
                             config_topic
                             )
    connector.start()
    
