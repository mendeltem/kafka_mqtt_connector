

# Kafka Konnektor


![Architekturbild](architekturbild.png)


##Installation

### MQTT Broker
https://mosquitto.org/

### MQTT Python
https://pypi.org/project/paho-mqtt/

### Kafka Python
https://kafka-python.readthedocs.io/en/master/install.html

### MongoDB
https://docs.mongodb.com/manual/installation/

### Starte die Server

###start mqtt broker
sudo service mosquitto start 
sudo service  mosquitto stop

###start mongodb
sudo mongod




simulation/

#start zookeeper
python start_kafkazookeeper.py 

#start kafka broker
python start_kafkaserver.py 

#start simulation
python start_sim.py 








