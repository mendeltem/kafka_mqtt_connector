import time
import random
import threading
import paho.mqtt.client as mqtt

threadLock = threading.Lock()
DEVICE_UUID_1 = "DEVICE1-8A12-4F4F-8F69-6B8F3C2E78FF"

my_ip = "localhost"

BROKER_ADDR = my_ip
BROKER_PORT = 1883

DEVICE_NAME = "temp2"
SENSOR_TOPIC_1 = 'sensor/temperature/'+DEVICE_NAME+'/' + DEVICE_UUID_1


def mqtt_connect(mqtt_client, userdata, flags, rc):
    
    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    mqtt_client.subscribe("led/" + DEVICE_UUID_1)

def mqtt_message(mqtt_client, userdata, msg):
    print("MESSAGE topic: " + msg.topic + "MESSAGE payload: " + msg.payload.decode())
    
def sensor_loop():
    while True:
        message = random.randint(29, 31)
        print(SENSOR_TOPIC_1+":"+str(message))
        mqtt_client.publish(SENSOR_TOPIC_1, message)
        time.sleep(10)
        

if __name__ == "__main__":
    mqtt_client = mqtt.Client()
    mqtt_client.on_connect = mqtt_connect
    mqtt_client.on_message = mqtt_message

    mqtt_client.disconnect()
    mqtt_client.connect(BROKER_ADDR, BROKER_PORT, 60)
    mqtt_client.loop_stop()

    sensor_handler = threading.Thread(target=sensor_loop)
    sensor_handler.start()

    mqtt_client.loop_start()
    sensor_handler.join()



#
#mqtt_client.loop_stop()
#mqtt_client.disconnect()
