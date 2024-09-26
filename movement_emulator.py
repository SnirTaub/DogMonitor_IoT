import sys
import random
from PyQt5 import QtGui, QtCore, QtWidgets
from PyQt5.QtWidgets import *
from PyQt5.QtGui import *
from PyQt5.QtCore import *
import paho.mqtt.client as mqtt
from datetime import datetime


# Global variables
clientname = ""
CONNECTED = False
DHT_topic = 'pr/home/IoT/FinalProject'
update_rate = 5000  # 5 seconds

class Mqtt_client:
    def __init__(self):
        self.broker = ""
        self.port = ""
        self.clientname = ""
        self.username = ""
        self.password = ""
        self.subscribeTopic = ""
        self.publishTopic = ""
        self.publishMessage = ""
        self.on_connected_to_form = ""

    # Setters and getters
    def set_on_connected_to_form(self, on_connected_to_form):
        self.on_connected_to_form = on_connected_to_form

    def get_broker(self):
        return self.broker

    def set_broker(self, value):
        self.broker = value

    def get_port(self):
        return self.port

    def set_port(self, value):
        self.port = value

    def get_clientName(self):
        return self.clientName

    def set_clientName(self, value):
        self.clientName = value

    def get_username(self):
        return self.username

    def set_username(self, value):
        self.username = value

    def get_password(self):
        return self.password

    def set_password(self, value):
        self.password = value

    def get_subscribeTopic(self):
        return self.subscribeTopic

    def set_subscribeTopic(self, value):
        self.subscribeTopic = value

    def get_publishTopic(self):
        return self.publishTopic

    def set_publishTopic(self, value):
        self.publishTopic = value

    def get_publishMessage(self):
        return self.publishMessage

    def set_publishMessage(self, value):
        self.publishMessage = value

    def on_log(self, client, userdata, level, buf):
        print("log: " + buf)

    def on_connect(self, client, userdata, flags, rc):
        global CONNECTED
        if rc == 0:
            print("Connected successfully")
            CONNECTED = True
            self.on_connected_to_form()
        else:
            print("Bad connection. Returned code =", rc)

    def on_disconnect(self, client, userdata, flags, rc=0):
        CONNECTED = False
        print("Disconnected. Result code: " + str(rc))

    def on_message(self, client, userdata, msg):
        topic = msg.topic
        m_decode = str(msg.payload.decode("utf-8", "ignore"))
        print("Message from:", topic, m_decode)

    def connect_to(self):
        self.client = mqtt.Client(self.clientname, clean_session=True)
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_log = self.on_log
        self.client.on_message = self.on_message
        self.client.username_pw_set(self.username, self.password)
        print("Connecting to broker", self.broker)
        self.client.connect(self.broker, self.port)

    def disconnect_from(self):
        self.client.disconnect()

    def start_listening(self):
        self.client.loop_start()

    def stop_listening(self):
        self.client.loop_stop()

    def subscribe_to(self, topic):
        if CONNECTED:
            self.client.subscribe(topic)
        else:
            print("Can't subscribe. Connection should be established first")

    def publish_to(self, topic, message):
        if CONNECTED:
            self.client.publish(topic, message)
        else:
            print("Can't publish. Connection should be established first")

class ConnectionDock(QDockWidget):
    def __init__(self, mc):
        QDockWidget.__init__(self)
        
        self.mc = mc
        self.mc.set_on_connected_to_form(self.on_connected)
        self.eHostInput = QLineEdit()
        self.eHostInput.setText("broker.hivemq.com")
        
        self.ePort = QLineEdit()
        self.ePort.setValidator(QIntValidator())
        self.ePort.setMaxLength(4)
        self.ePort.setText("1883")
        
        self.eClientID = QLineEdit()
        self.eClientID.setText(clientname)
        
        self.eUserName = QLineEdit()
        self.eUserName.setText("username")
        
        self.ePassword = QLineEdit()
        self.ePassword.setEchoMode(QLineEdit.Password)
        self.ePassword.setText("password")
        
        self.eKeepAlive = QLineEdit()
        self.eKeepAlive.setValidator(QIntValidator())
        self.eKeepAlive.setText("60")
        
        self.eSSL = QCheckBox()
        
        self.eCleanSession = QCheckBox()
        self.eCleanSession.setChecked(True)
        
        self.eConnectbtn = QPushButton("Enable/Connect", self)
        self.eConnectbtn.setToolTip("Click to connect")
        self.eConnectbtn.clicked.connect(self.on_button_connect_click)
        self.eConnectbtn.setStyleSheet("background-color: gray")
        
        self.ePublisherTopic = QLineEdit()
        self.ePublisherTopic.setText(DHT_topic)

        self.Movement = QLineEdit()
        self.Movement.setText('')

        formLayout = QFormLayout()       
        formLayout.addRow("Turn On/Off", self.eConnectbtn)
        formLayout.addRow("Pub topic", self.ePublisherTopic)
        formLayout.addRow("Movement", self.Movement)

        widget = QWidget(self)
        widget.setLayout(formLayout)
        self.setTitleBarWidget(widget)
        self.setWidget(widget)
        self.setWindowTitle("Connect")

    def on_connected(self):
        self.eConnectbtn.setStyleSheet("background-color: green")

    def on_button_connect_click(self):
        # Convert the hostname to ASCII using IDN encoding
        broker_host = self.eHostInput.text().encode('idna').decode()

        # Connect to the broker
        self.mc.set_broker(broker_host)
        self.mc.set_port(int(self.ePort.text()))
        self.mc.set_clientName(self.eClientID.text())
        self.mc.set_username(self.eUserName.text())
        self.mc.set_password(self.ePassword.text())
        self.mc.connect_to()
        self.mc.start_listening()

class MainWindow(QMainWindow):
    def __init__(self, parent=None):
        QMainWindow.__init__(self, parent)
                
        self.mc = Mqtt_client()
        
        self.timer = QtCore.QTimer(self)
        self.timer.timeout.connect(self.update_data)
        self.timer.start(update_rate)  # in milliseconds
        
        self.setUnifiedTitleAndToolBarOnMac(True)

        self.setGeometry(30, 600, 300, 150)
        self.setWindowTitle("Movement Emulator")
        
        self.connectionDock = ConnectionDock(self.mc)        
       
        self.addDockWidget(Qt.TopDockWidgetArea, self.connectionDock)

    def update_data(self):
        print("Next update")
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        movement = random.uniform(0, 100)
        movement_message = f'movement: "{current_time}", value: {movement}'
        self.connectionDock.Movement.setText(movement_message)
        self.mc.publish_to(DHT_topic, movement_message)

app = QApplication(sys.argv)
mainwin = MainWindow()
mainwin.show()
app.exec_()
