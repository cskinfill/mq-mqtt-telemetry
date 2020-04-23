import paho.mqtt.client as mqtt
import os
from influxdb import InfluxDBClient
import json
import logging
import signal

logging.basicConfig(level=logging.DEBUG)

def handleSigTerm(client):
    def handleit(signalNum, frame):
        logging.debug("Handling signal %s", signalNum)
        if signalNum == signal.SIGTERM:
            logging.info("Disconnecting MQTT client")
            client.disconnect()
    return handleit

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    logging.info("Connected with result code %s", rc)

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe(os.environ["MQTT_TOPIC"])


# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    logging.debug("message in %s", msg.topic)
    payload = json.loads(msg.payload)
    logging.debug("payload is: %s", payload)
    tags = {"device": payload['DevEUI']}
    logging.debug("tags are %s", tags)
    telemetry = {
        "measurement": "environment",
        "tags": {
            "gateway": payload['GatewayID'],
            "decoder": payload['DecoderType'],
            "SpreadingFactor": payload['SpreadingFactor'],
            "SubBand": payload['SubBand'],
            "Channel": payload['Channel'],
            "topic": msg.topic
        },
        "time": payload['Time'],
        "fields": {k: float(v) for k,v in payload['decoded_payload'].items()}
    }
    points = [telemetry]
    for G in payload['GatewayList']:
        lora = {
            "measurement": "lorawan",
            "tags": {
                "gateway":G['GatewayID'],
            },
            "time": payload['Time'],
            "fields": {
                "snr": float(G['GatewaySNR']),
                "rssi": float(G['GatewayRSSI']),
                "esp": float(G['GatewayESP'])
            }
        }
        points.append(lora)
    logging.debug(msg.topic+" "+str(points))
    influxdb_client.write_points(points, tags=tags)


client = mqtt.Client()
client.tls_set()
client.on_connect = on_connect
client.on_message = on_message
client.username_pw_set(os.environ["MQTT_USERNAME"],os.environ["MQTT_PASSWORD"])
client.connect(host=os.environ["MQTT_HOSTNAME"], port=int(os.environ["MQTT_PORT"]), keepalive=60)

influxdb_client = InfluxDBClient(host=os.environ["INFLUXDB_HOSTNAME"], port=int(os.environ["INFLUXDB_PORT"]), username=os.environ["INFLUXDB_USERNAME"], password=os.environ["INFLUXDB_PASSWORD"], database=os.environ["INFLUXDB_DATABASE"])

signal.signal(signal.SIGTERM, handleSigTerm(client))

# Blocking call that processes network traffic, dispatches callbacks and
# handles reconnecting.
# Other loop*() functions are available that give a threaded interface and a
# manual interface.
client.loop_forever()
