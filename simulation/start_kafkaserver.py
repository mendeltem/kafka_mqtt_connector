#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Apr  7 00:03:17 2020

@author: panda
"""



import os

os.chdir("/home/panda/Desktop/kafkamqtt/kafka-2.3.1-src")
os.system("bin/kafka-server-start.sh config/server.properties")


