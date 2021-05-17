import paho.mqtt.client as mqtt
import random
import paho.mqtt.publish as publish

def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))
    client.subscribe("cloud_topic")

def on_message(client, userdata, msg):
    print(msg.topic+" "+str(msg.payload))
    n = random.randint(0,1)
    if (n == 0):
        result = "Failure"
    else:
        result = "Success"
    print("Publishing message to a ","result_topic")
    pub = publish.single("result_topic", result, qos=2, retain=True)
    with open('/home/vvdn/kartik_bk/interviews/zenatix/new.csv', 'ab') as file:
        file.write(msg.payload)
        file.write(b'\n')

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.connect("localhost", 1883, 60)
client.loop_forever()
