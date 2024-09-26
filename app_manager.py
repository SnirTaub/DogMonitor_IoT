import paho.mqtt.client as mqtt
import time
import random
import sqlite3
from mqtt_init import *
from icecream import ic
from datetime import datetime

def time_format():
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S') + " | "

ic.configureOutput(prefix=time_format)
ic.configureOutput(includeContext=False)  # Use True for including script file context

# Create a database connection with check_same_thread=False
conn = sqlite3.connect('dog_movement.db', check_same_thread=False)
cursor = conn.cursor()

# Create a table if it doesn't exist
cursor.execute('''
CREATE TABLE IF NOT EXISTS movements (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT NOT NULL,
    value REAL NOT NULL
)
''')
conn.commit()

# Define callback functions
def on_log(client, userdata, level, buf):
    ic("log: " + buf)

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        ic("connected OK")
    else:
        ic("Bad connection Returned code=", rc)

def on_disconnect(client, userdata, flags, rc=0):
    ic("Disconnected result code " + str(rc))

def on_message(client, userdata, msg):
    topic = msg.topic
    m_decode = str(msg.payload.decode("utf-8", "ignore"))
    ic("message from: " + topic, m_decode)

    try:
        # Extract the value part from the message
        value_str = m_decode.split('value: ')[1]
        value = float(value_str)

        if value < dog_movement_THR:
            ic("Threshold warning! Your dog is running excessively: " + str(value))
            send_msg(client, topic_alarm, "Threshold warning! Your dog is running excessively: " + str(value))

        # Insert the data into the database
        retry_count = 0
        while retry_count < 5:
            try:
                cursor.execute('''
                INSERT INTO movements (timestamp, value) VALUES (?, ?)
                ''', (datetime.now().strftime('%Y-%m-%d %H:%M:%S'), value))
                conn.commit()
                break
            except sqlite3.OperationalError as e:
                if 'database is locked' in str(e):
                    retry_count += 1
                    time.sleep(1)
                else:
                    raise
    except ValueError as e:
        ic(f"Exception in handle_database_operations: {e}")

def send_msg(client, topic, message):
    client.publish(topic, message)

def client_init(cname):
    client = mqtt.Client(cname)
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_log = on_log
    client.on_message = on_message
    client.username_pw_set(username, password)
    client.connect(broker_ip, int(broker_port))
    return client

def main():
    client = client_init("IOT_client-Id-" + str(random.randrange(1, 10000000)))
    client.loop_start()
    client.subscribe(sub_topic)
    while True:
        time.sleep(1)

if __name__ == "__main__":
    main()