

# Kafka Konnektor


![Architekturbild](architekturbild.png)


## Installation

### MQTT Broker
https://mosquitto.org/

### MQTT Python
https://pypi.org/project/paho-mqtt/

### Kafka Python
https://kafka-python.readthedocs.io/en/master/install.html

### MongoDB
https://docs.mongodb.com/manual/installation/

### Flask Webserver
https://pypi.org/project/Flask/


## Starte die Server

### Start den MQTT Broker
sudo service mosquitto start 

### Starte den MongoDB
sudo mongod


### Starte Kafka Zookeeper
### In Ordner simulation/
python start_kafkazookeeper.py 

### Starte Kafka Broker
### In Ordner simulation/
python start_kafkaserver.py 

## Starte die Simulation
### In Ordner simulation/
python start_sim.py 








