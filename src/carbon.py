import requests
import json
import os


CARBON_API_CREDS = os.getenv('CARBON_API_CREDS', "")
if not CARBON_API_CREDS:
    print("Please set the CARBON_API_CREDS environment variable")
    exit(1)

CARBON_URL = os.getenv('CARBON_URL', "")
if not CARBON_URL:
    print("Please set the CARBON_URL environment variable")
    exit(1)

CARBON_FOLDER_ID = int(os.getenv('CARBON_FOLDER_ID', 0))
if CARBON_FOLDER_ID == 0:
    print("Please set the CARBON_FOLDER_ID environment variable")
    exit(1)

NODETYPE_ID = int(os.getenv('NODETYPE_ID', 0))
if NODETYPE_ID == 0:
    print("Please set the NODETYPE_ID environment variable")
    exit(1)


def check_node(eui):
    filter = {"fieldName": "uniqueId",
              "operator": "eq", "value": eui}
    query = {"filters": [filter]}
    try:
        r = requests.post("{}/api/nodes/query".format(CARBON_URL),
                          json=query, headers={"Authorization": CARBON_API_CREDS, "Content-Type": "application/json"})
        node = r.json()['results'][0]
        return node
    except KeyError:
        return []
    except IndexError:
        print("need to create the node")

    payload = {
        'folderId': CARBON_FOLDER_ID,
        'nodetypeId': NODETYPE_ID,
        'uniqueId': eui,
        'vanity': "MKRWAN + MKRENV - " + eui[-4:],
        'metadata': {},
        'isActive': True
    }

    node_request = requests.post("{}/api/nodes".format(CARBON_URL), json=payload, headers={
        'Authorization': CARBON_API_CREDS})
    return node_request.json()


def send_values(node_id, temperature, humidity, pressure, illuminance, rssi, sf, snr, raw):

    payload = [
        {
            "channelName": "temp",
            "nodeId": node_id,
            "value": temperature,
        },
        {
            "channelName": "hum",
            "nodeId": node_id,
            "value": humidity,
        },
        {
            "channelName": "pressure",
            "nodeId": node_id,
            "value": pressure,
        },

        {
            "channelName": "illuminance",
            "nodeId": node_id,
            "value": illuminance,
        },

        {
            "channelName": "rssi",
            "nodeId": node_id,
            "value": rssi,
        },

        {
            "channelName": "sf",
            "nodeId": node_id,
            "value": sf,
        },

        {
            "channelName": "snr",
            "nodeId": node_id,
            "value": snr,
        },

        {
            "channelName": "raw",
            "nodeId": node_id,
            "value": raw,
        }
    ]

    channel_request = requests.post("{}/api/publish/batch".format(CARBON_URL), json=payload, headers={
        'Authorization': CARBON_API_CREDS})
    return {'status_code': channel_request.status_code}
