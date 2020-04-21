

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
bin/zookeeper-server-start.sh config/zookeeper.properties

### Starte Kafka Broker
bin/kafka-server-start.sh config/server.properties

## Starte die Simulation
### In Ordner simulation/
python start_sim.py 








