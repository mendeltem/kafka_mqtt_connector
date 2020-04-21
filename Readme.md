

# Kafka Konnektor


![Architekturbild](architekturbild.png)


## Installation

### MQTT Broker
    https://mosquitto.org/

### MQTT Python
    https://pypi.org/project/paho-mqtt/

### Kafka Broker
    https://www.apache.org/dyn/closer.cgi?path=/kafka/2.5.0/kafka_2.12-2.5.0.tgz

### Kafka Python
    https://kafka-python.readthedocs.io/en/master/install.html

### MongoDB
    https://docs.mongodb.com/manual/installation/

### Flask Webserver
    https://pypi.org/project/Flask/


## Starte die Server

### Start den MQTT Broker
    sudo service mosquitto start 

### Starte Kafka Broker
    https://kafka.apache.org/quickstart


### Starte den MongoDB
sudo mongod


## Konfiguration in config.ini
In confing.ini werden die IP Adressen mit Port von den Servern eingestellt.

Hier werden die zwei Zeitintervalle aufgenommen. In Sekunden. 
Alle 60 Sekunden und alle 300 Sekunden.
    interval_tm_sec_list    =  [60,300]


Der Topicname für die Konfiguration
    config_topic            = kafka/config/#

Datenbankname
    db_name                 = kafka_database



## Starte die Simulation
    python start_simulation.py









