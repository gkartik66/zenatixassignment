import csv
import paho.mqtt.client as mqtt
import paho.mqtt.subscribe as subscribe
import time

broker_address="localhost"
client = mqtt.Client() #create new instance
print("connecting to broker")
client.connect(broker_address) #connect to broker

buffer_list = list()

def sixty_sec():
    """
    This Function is sending data every 60 seconds on cloud_topic over MQTT. 
    """
    with open('/home/vvdn/kartik_bk/interviews/zenatix/dataset.csv','r') as csv_file: #Opens the file in read mode
        csv_reader = csv.reader(csv_file) # Making use of reader method for reading the file
    #Iterate through the loop to read line by line in csv file
        for line in csv_reader:
            listToStr = '  '.join([str(elem) for elem in line])
            print("Publishing message to topic","cloud_topic")
            client.publish("cloud_topic",listToStr)
            res = get_response()
            res = str(res,"utf-8")
            if res == "Failure":
                buffer_list.append(listToStr)
                five_sec()
            time.sleep(60)

def get_response():
    """
    This function is getting Failure and Success from server side.
    """
    msg = subscribe.simple("result_topic")
    #print("%s %s" % (msg.topic, msg.payload))
    return msg.payload

def five_sec():
    """
    In this function buffered data is getting publish and list is getting cleared.
    """
    #print(buffer_list)
    client.publish("cloud_topic",str(buffer_list))
    buffer_list.clear()
    time.sleep(5)

sixty_sec()
