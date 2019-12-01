# imports
import sqlite3
import paho.mqtt.client as mqtt
import time
import json
import os
from events import *
from dotenv import load_dotenv
import threading


load_dotenv()

# MQTT credentials
ACCESS_TOKENS = os.getenv('ACCESS_TOKENS').split(',') # access tokens for each device
BROKER = os.getenv('BROKER')
PORT = int(os.getenv('PORT'))
TOPIC = os.getenv('TOPIC')

# set up database connection and cursor for sql
conn = sqlite3.connect('MyProjectNewData.db', check_same_thread= False) 

# functions to get and send the data neccessary to thingsboard
'''
function to get all mac_addresses used for XBee
'''
def getMacAddresses():
    c = conn.cursor()
    c.execute('SELECT DISTINCT mac_address from dataToPlot;')
    results = []
    for macAddress in c.fetchall():
        results.append(macAddress[0])
    return results
   

'''
function to get latest temperature from device
macAddr param is a string representing the MAC address of a device 
'''
def getTemperatures(macAddr):
    c = conn.cursor()
    params = (macAddr, )
    c.execute("SELECT temperature FROM dataToPlot WHERE mac_address = ? ORDER BY date_time ASC, RANDOM() LIMIT 20", params)
    results = [temperature[0] for temperature in c.fetchall()]
    return results
    

    

'''
function that sends the data to thingsboard 
data param is a dict with two keys, temperature and mac address
client param is a MQTT object
'''
def sendData(client, data):
    data = json.dumps(data)
    print("publish topic, ", TOPIC, " data out = ", data)
    ret = client.publish(TOPIC, data, 0)
    client.loop()
'''

'''
def sendToThingsBoard(macAddress, accessToken): 
    data = {"macAddress" : macAddress}
    temperatures = getTemperatures(macAddress)

    client = mqtt.Client() # init mqtt client
    # set up flags

    mqtt.Client.connected_flag = False  #create flag in class
    mqtt.Client.suppress_puback_flag = False   

    # set event handlers       
    client.on_log=on_log
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_publish = on_publish

    # set up connection
    client.username_pw_set(accessToken)
    client.connect(BROKER, PORT)
    
    # wait in loop for connection
    while not client.connected_flag:
        client.loop()
        time.sleep(1)

    for temperature in temperatures:
        data['temperature'] = temperature    
        sendData(client, data)
        time.sleep(3)
    client.disconnect()

    time.sleep(2)
        

if __name__ == "__main__":
    macAddresses = getMacAddresses() # get mac addresses from sensors
    threads = []

    for i in range(len(macAddresses)): 
        macAddress = macAddresses[i]
        accessToken = ACCESS_TOKENS[i]
        t = threading.Thread(target = sendToThingsBoard, args = [macAddress, accessToken]) # args is parameters for target function of thread
        t.start()
        threads.append(t)

    for thread in threads:
        thread.join()     
    conn.close() # close connection and finish up program
    