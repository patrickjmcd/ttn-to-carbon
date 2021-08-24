#!/usr/bin/python3

import time
import logging
import sys
import os
from pathlib import Path
import carbon
from datetime import datetime
import csv
import json
import paho.mqtt.client as mqtt
import os


Path("output").mkdir(parents=True, exist_ok=True)

User = os.getenv('TTN_USER', "")
if not User:
    print("No TTN_USER environment variable set")
    sys.exit(1)

Password = os.getenv('TTN_KEY', "")
if not Password:
    print("No TTN_KEY environment variable set")
    sys.exit(1)

theRegion = os.getenv('TTN_REGION', "NAM1")


def send_to_carbon(msg):

    eui = msg["end_device_ids"]['device_id']
    temperature = msg['uplink_message']['decoded_payload']['temperature']
    humidity = msg['uplink_message']['decoded_payload']['humidity']
    pressure = msg['uplink_message']['decoded_payload']['pressure']
    illuminance = msg['uplink_message']['decoded_payload']['illuminance']

    rssi = msg['uplink_message']['rx_metadata'][0]['rssi']
    snr = msg['uplink_message']['rx_metadata'][0]['snr']
    sf = msg['uplink_message']['settings']['data_rate']['lora']['spreading_factor']
    raw = msg
    node = carbon.check_node(eui)
    carbon.send_values(
        node['id'], temperature, humidity, pressure, illuminance, rssi, sf, snr, raw)


# Write uplink to tab file
def saveToFile(someJSON):
    end_device_ids = someJSON["end_device_ids"]
    device_id = end_device_ids["device_id"]
    application_id = end_device_ids["application_ids"]["application_id"]

    received_at = someJSON["received_at"]
    try:
        uplink_message = someJSON["uplink_message"]
        f_port = uplink_message["f_port"]
        f_cnt = uplink_message["f_cnt"]
        frm_payload = uplink_message["frm_payload"]
        rssi = uplink_message["rx_metadata"][0]["rssi"]
        snr = uplink_message["rx_metadata"][0]["snr"]
        data_rate_index = uplink_message["settings"]["data_rate_index"]
        consumed_airtime = uplink_message["consumed_airtime"]
        humidity = uplink_message["decoded_payload"]["humidity"]
        temperature = uplink_message["decoded_payload"]["temperature"]
        pressure = uplink_message["decoded_payload"]["pressure"]
        illuminance = uplink_message["decoded_payload"]["illuminance"]

        # Daily log of uplinks
        now = datetime.now()
        pathNFile = "output/" + now.strftime("%Y%m%d") + ".txt"
        print(pathNFile)
        if (not os.path.isfile(pathNFile)):
            with open(pathNFile, 'a', newline='') as tabFile:
                fw = csv.writer(tabFile, dialect='excel-tab')
                fw.writerow(["received_at", "application_id", "device_id", "f_port", "f_cnt", "frm_payload", "rssi",
                            "snr", "data_rate_index", "consumed_airtime", "temperature", "humidity", "pressure", "illuminance"])

        with open(pathNFile, 'a', newline='') as tabFile:
            fw = csv.writer(tabFile, dialect='excel-tab')
            fw.writerow([received_at, application_id, device_id, f_port, f_cnt, frm_payload, rssi,
                        snr, data_rate_index, consumed_airtime, temperature, humidity, pressure, illuminance])

        # Application log
        pathNFile = "output/" + application_id + ".txt"
        print(pathNFile)
        if (not os.path.isfile(pathNFile)):
            with open(pathNFile, 'a', newline='') as tabFile:
                fw = csv.writer(tabFile, dialect='excel-tab')
                fw.writerow(["received_at", "device_id", "f_port", "f_cnt", "frm_payload", "rssi", "snr",
                            "data_rate_index", "consumed_airtime", "temperature", "humidity", "pressure", "illuminance"])

        with open(pathNFile, 'a', newline='') as tabFile:
            fw = csv.writer(tabFile, dialect='excel-tab')
            fw.writerow([received_at, device_id, f_port, f_cnt, frm_payload, rssi, snr,
                        data_rate_index, consumed_airtime, temperature, humidity, pressure, illuminance])

        # Device log
        pathNFile = "output/" + application_id + "__" + device_id + ".txt"
        print(pathNFile)
        if (not os.path.isfile(pathNFile)):
            with open(pathNFile, 'a', newline='') as tabFile:
                fw = csv.writer(tabFile, dialect='excel-tab')
                fw.writerow(["received_at", "f_port", "f_cnt", "frm_payload", "rssi", "snr", "data_rate_index",
                            "consumed_airtime", "temperature", "humidity", "pressure", "illuminance"])

        with open(pathNFile, 'a', newline='') as tabFile:
            fw = csv.writer(tabFile, dialect='excel-tab')
            fw.writerow([received_at, f_port, f_cnt, frm_payload, rssi, snr, data_rate_index,
                        consumed_airtime, temperature, humidity, pressure, illuminance])
    except KeyError:
        print("empty payload?")

# MQTT event functions


def on_connect(mqttc, obj, flags, rc):
    print("\nConnect: rc = " + str(rc))


def on_message(mqttc, obj, msg):
    # + " " + str(msg.payload))
    print("\nMessage: " + msg.topic + " " + str(msg.qos))
    parsedJSON = json.loads(msg.payload)
    # Uncomment this to fill your terminal screen with JSON
    # print(json.dumps(parsedJSON, indent=4))
    saveToFile(parsedJSON)
    try:
        send_to_carbon(parsedJSON)
    except:
        print("Error sending to Carbon")


def on_subscribe(mqttc, obj, mid, granted_qos):
    print("\nSubscribe: " + str(mid) + " " + str(granted_qos))


def on_log(mqttc, obj, level, string):
    print("\nLog: " + string)
    logging_level = mqtt.LOGGING_LEVEL[level]
    logging.log(logging_level, string)


mqttc = mqtt.Client()

mqttc.on_connect = on_connect
mqttc.on_subscribe = on_subscribe
mqttc.on_message = on_message
# mqttc.on_log = on_log		# Logging for debugging OK, waste

# Setup authentication from settings above
mqttc.username_pw_set(User, Password)


# IMPORTANT - this enables the encryption of messages
mqttc.tls_set()  # default certification authority of the system

# mqttc.tls_set(ca_certs="mqtt-ca.pem") # Use this if you get security errors
# It loads the TTI security certificate. Download it from their website from this page:
# https://www.thethingsnetwork.org/docs/applications/mqtt/api/index.html
# This is normally required if you are running the script on Windows

url = theRegion.lower() + ".cloud.thethings.network"
print("Connecting to {}".format(url))
print("Sending to {}".format(carbon.CARBON_URL))
mqttc.connect(url, 8883, 60)

topic = "v3/{}/devices/+/up".format(User)
print("...subscribing to topic {}".format(topic))
mqttc.subscribe(topic, 0)  # all device uplinks

try:
    run = True
    while run:
        mqttc.loop(10) 	# seconds timeout / blocking time
        # feedback to the user that something is actually happening
        print(".", end="", flush=True)


except KeyboardInterrupt:
    print("Exit")
    sys.exit(0)
