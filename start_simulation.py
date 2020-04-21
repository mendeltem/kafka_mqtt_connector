#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Mar 26 16:52:46 2020

@author: panda
"""

import subprocess
import time
import os

os.chdir("simulation")

subprocess.Popen(['python', 'bridge1.py'])
subprocess.Popen(['python', 'bridge2.py'])
subprocess.Popen(['python', 'bridge3.py'])
subprocess.Popen(['python', 'bridge4.py'])

##
subprocess.Popen(['python', 'im1.py'])
time.sleep(2)
subprocess.Popen(['python', 'im2.py'])
time.sleep(2)
subprocess.Popen(['python', 'im3.py'])
time.sleep(2)
subprocess.Popen(['python', 'im4.py'])
time.sleep(2)
subprocess.Popen(['python', 'im5.py'])
time.sleep(2)
subprocess.Popen(['python', 'im6.py'])
time.sleep(2)
subprocess.Popen(['python', 'im7.py'])
time.sleep(2)
subprocess.Popen(['python', 'im8.py'])
