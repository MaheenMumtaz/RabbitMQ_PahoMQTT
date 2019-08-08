#Publisher sends random numbers (1-100) at random intervals (1-30 seconds)
#Publishes data on broker - RabbitMQ hosted locally

import paho.mqtt.client as paho
import time
import random

mqtthost = "127.0.0.1"  #localhost
mqttuser = "guest"  
mqttpass = "guest"

def on_connect(client, userdata, flags, rc):
    print("CONNACK received with code %d." % (rc))
    
def on_publish(client, userdata, mid):
    print("Successfully published: ", Rand_Num)

client = paho.Client(client_id="C1")    #Initializing Publisher               
client.on_connect = on_connect          #Function to be called when CONNACK received
client.on_publish = on_publish          #Function to be called when successfully published
client.username_pw_set(mqttuser,mqttpass) #Setting broker credentials
client.connect(mqtthost, 1883)          
client.loop_start()

try:
    while True:
        Rand_Num=random.randint(1,100)
        Rand_Time=random.randint(1,30)
        client.publish("Random/Number",Rand_Num)
        time.sleep(Rand_Time)

except (KeyboardInterrupt, SystemExit):        
    client.loop_stop()
    client.disconnect()
    print("Disconnected")

