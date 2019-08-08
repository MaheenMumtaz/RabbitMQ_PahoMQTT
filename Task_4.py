#Subsribes to Topic 'Random/Number/Averages' on RabbitMQ hosted locally and
#prints the average values received
#String starting from A - One minute Average
#String starting from B - Five minute Average
#String starting from C - Thirty minute Average
#Data printed in tabular form
#Continuous incoming values can be plotted for production friendly interface

import paho.mqtt.client as paho
import time
from beautifultable import BeautifulTable
 
mqtthost = "127.0.0.1"  
mqttuser = "guest"  
mqttpass = "guest"

Av_Values = ""
Av_1min=0
Av_5min=0
Av_30min=0
table = BeautifulTable()

def init_table():
    table.set_style(BeautifulTable.STYLE_BOX_DOUBLED)
    table.column_headers = ["Number of Minutes", "Average Value"]
    table.append_row([1, 0])
    table.append_row([5, 0])
    table.append_row([30,0])

    
def on_connect(client2, userdata, flags, rc):
    print("CONNACK received with code %d." % (rc))

def on_message(client2, userdata, msg): #updates and prints the table when new values received
    print("Topic: "+msg.topic+"   "+"Received Msg: "+str(msg.payload.decode("utf-8")))
    Av_Values=msg.payload.decode("utf-8")
    Type, Value = Av_Values.split("_",1)
    if(Type=="A"):
        Av_1min=float(Value)
        table[0][1]=Value
    if(Type=="B"):
        Av_5min=float(Value)
        table[1][1]=Value
    if(Type=="C"):
        Av_30min=float(Value)
        table[2][1]=Value
    print(table)
        
def on_subscribe(client2, userdata, mid, granted_qos):
    print("Successfully Subscribed")

#Initialization
init_table()
client2 = paho.Client(client_id="C3")
client2.on_connect = on_connect
client2.on_message = on_message
client2.on_subscribe = on_subscribe
client2.username_pw_set(mqttuser,mqttpass)
client2.connect(mqtthost, 1883 ,60)
client2.loop_start()

#Subscribing
client2.subscribe("Random/Number/Averages")

#Main Loop
while True:
    time.sleep(1)
 
    
    
client2.loop_stop()
