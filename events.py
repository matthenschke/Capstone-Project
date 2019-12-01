'''
File for MQTT event handlers
'''
def on_log(client, userdata, level, buf):
    print(buf)

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        client.connected_flag = True # set flag
        print("Connected OK")
    else:
        print("Bad Connection Returned code = ", rc)
        client.loop_stop()

def on_disconnect(client, userdata, rc):
    print("client disconnected ok")

def on_publish(client, userdata, mid):
    print("In on_publish callback mid = ", mid)