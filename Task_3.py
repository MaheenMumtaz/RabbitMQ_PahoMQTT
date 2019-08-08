#Subscribes to RabbitMQ hosted locally, reads the data on Topic "Random/Number"
#Calculates 1min, 5min and 30min averages of the data
#Publishes the averages on another topic "Random/Number/Averages"
#Averages are updated after every one, five and thirty minutes respectively.
#Moving averages can be calculated for future upgrade

import paho.mqtt.client as paho
import time
 
mqtthost = "127.0.0.1"  
mqttuser = "guest"  
mqttpass = "guest"
Numbers = []
Numbers_int = []
Numbers_5min = []
Numbers_30min = []
Temp_list = []
Temp2_list = []
Mins_Count = 0
Avg_1min = 0
Avg_5min = 0
Avg_30min = 0

def on_connect(client2, userdata, flags, rc):
    print("CONNACK received with code %d." % (rc))

def on_message(client2, userdata, msg):
    print(msg.topic+" "+str(msg.payload.decode("utf-8")))
    Numbers.append(msg.payload.decode("utf-8"))
   # print(Numbers)
    
def on_subscribe(client2, userdata, mid, granted_qos):
    print("Successfully Subscribed")

def on_publish(client2, userdata, mid):
    print("Published Averages")

def Average(lst): 
    return sum(lst) / len(lst)

def Calculate_Avg (min_count, min_comp, lst):
    if(min_count % min_comp == 0):
        Avg = Average(lst)
       # print(lst)
        return (Avg, lst)
    else:
       # print(min_comp, "Not up yet")
        return([], [])

#Initialization   
client2 = paho.Client(client_id="C2") 
client2.on_connect = on_connect
client2.on_message = on_message
client2.on_subscribe = on_subscribe
client2.on_publish = on_publish
client2.username_pw_set(mqttuser,mqttpass)
client2.connect(mqtthost, 1883 ,60)
client2.loop_start()
client2.subscribe("Random/Number")

#Main Loop
try:
    while True:
        time.sleep(60)
        Numbers_int = [int(i) for i in Numbers] #converting to int values
        Avg_1min = Average(Numbers_int)         #Calculating one minute average
        Mins_Count = Mins_Count + 1             #Minute count increased
       #print(Avg_1min, Mins_Count)
        Numbers_5min.extend(Numbers_int)        #Store the data for 5min average calculation
        Numbers.clear()                         #Clear the list after moving the data
        client2.publish("Random/Number/Averages","A_"+str(round(Avg_1min,2)))
        
        Avg_5min,Temp_list = Calculate_Avg (Mins_Count, 5, Numbers_5min) #Calculate 5min average if 5 minutes are over
        Numbers_30min.extend(Temp_list)                                  #Move data for 30min average calculation
        if(Avg_5min != []):
            Numbers_5min.clear()                                    
            client2.publish("Random/Number/Averages","B_"+str(round(Avg_5min,2)))
            Temp_list.clear()

        Avg_30min,Temp2_list = Calculate_Avg (Mins_Count, 30, Numbers_30min) #Calculate 30min average if 30 minutes are over
        if(Avg_30min != []):
            Numbers_30min.clear()
            Mins_Count = 0
            client2.publish("Random/Number/Averages","C_"+str(round(Avg_30min,2)))

except (KeyboardInterrupt, SystemExit):        
    client2.loop_stop()
    client2.disconnect()
    print("Disconnected")

