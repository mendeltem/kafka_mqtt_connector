import json
import configparser
from module.messagerlib import Messager

data = configparser.ConfigParser()
data.sections()
data.read('config.ini')

mqtt_broker_ip              =  data["DATA"]["mqtt_broker_ip"]
mqtt_broker_port            =  int(data["DATA"]["mqtt_broker_port"])

kafka_broker_ip             =  data["DATA"]["kafka_broker_ip"]
kafka_broker_port           =  int(data["DATA"]["kafka_broker_port"])


mongodb_ip                  =  data["DATA"]["mongodb_ip"]
mongodb_port                =  int(data["DATA"]["mongodb_port"])

db_name                     =  data["DATA"]["db_name"]
interval_tm_sec_list        = json.loads(data["DATA"]["interval_tm_sec_list"])


if __name__ == "__main__":

    messager =   Messager( mqtt_broker_ip,
                     mqtt_broker_port,
                     kafka_broker_ip,
                     kafka_broker_port,
                     mongodb_ip,
                     mongodb_port,
                     db_name,
                     interval_tm_sec_list 
                     )
                 
    messager.calculate_stream()       
    
    
          
            
            
