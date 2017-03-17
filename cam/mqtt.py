#!/usr/bin/env python
#Version:simplified to onltbdeal witg Sensor topic

 
import os
import xively
import subprocess
import time
import datetime
import requests
import paho.mqtt.client as paho
import logging
import logging.handlers


mqtt_logger = logging.getLogger('Gerald')
mqtt_logger.setLevel(logging.DEBUG)

mqtt_handler = logging.handlers.SysLogHandler(address = '/dev/log')
formatter = logging.Formatter('%(name)s: %(levelname)s %(message)s')
mqtt_handler.setFormatter(formatter)
mqtt_logger.addHandler(mqtt_handler)
 
# extract feed_id and api_key from environment variables
FEED_ID = 1805499709
API_KEY = "ohwsDVg1hajFNonUZtMm2Jo5Fdf1R9ai0EWcg40YqbU07Scs"

FEED_IDS = {'200':1805499709, 
            '201':69178581, 
            '202':2147148339,
            '210':263057938,
            '211':263057938
           }
API_KEYS = {'200': "ohwsDVg1hajFNonUZtMm2Jo5Fdf1R9ai0EWcg40YqbU07Scs",
            '201': "fS2iSdIN8F0pVYIA9PUqYKVogfMhRmh0oAXAEmn8oLcJhnc8",
            '202': "Zwsq4jn8HO3OyPsRIqLXAZaB573YoeMO1HuR0vHqba1M9gKD",
            '210': "LhtVwlwHceHU0GYKZsBiuwgexkbMad2vfkvbB5fb0Z0Yzoas",
            '211': "LhtVwlwHceHU0GYKZsBiuwgexkbMad2vfkvbB5fb0Z0Yzoas"
           }
#topic fields
SID = 0 #sensor id = last ip byte
MSG = 1
SUBMSG = 2

 
# initialize api client
api = xively.XivelyAPIClient(API_KEY)
feed = api.feeds.get(FEED_ID)

#do the apis
apis = {}
for k,v in API_KEYS.iteritems():
  apis[k] = xively.XivelyAPIClient(v)

feeds ={}
for k,v in apis.iteritems():
  feeds[k] = v.feeds.get(FEED_IDS.get(k))


#creare mqtt client
mqttc=paho.Client("mqttc")

def on_connect(mosq, obj, rc):
    print("Connected")

def on_log(mosq, obj, level, string):
    print(string)

def on_message(mosq, obj, msg):
    mqtt_logger.debug((str(msg.payload) + " " + msg.topic))
    rx_msg(msg)

def on_publish(mosq, obj, mid):
    print("mid: " + str(mid))

def on_subscribe(mosq, obj, mid, granted_qos):
    print("Subscribed: " + str(mid) + " " + str(granted_qos))


def rx_msg(msg):
    topics=msg.topic.split('/')
    #print (topics)
    if topics[MSG] =="sensor":
      #got a sensor reading
      #print(str(msg.payload))
      #(rx_val,epoch) = str(msg.payload).split(" ")
      rx_val=str(msg.payload).split(" ")[1]
      #timestamp is in epoch
      epoch = str(msg.payload).split(" ")[0]

      # tell openHAB BODGE
      mqttc.publish("openHAB/"+msg.topic, rx_val)

      feedid = (topics[MSG]+"-"+topics[SUBMSG]) 
      #print (topics[0],feeds.get(topics[0]),feedid)
      try:
        datastream = get_datastream(feeds.get(topics[SID]), feedid)
        datastream.current_value = rx_val
        datastream.at = datetime.datetime.utcnow()
        #mqtt_logger.debug( ("posting sensor:" +str(msg.payload)))
        try:
          datastream.update()
        except requests.HTTPError as e:
          #print "HTTPError rx_msg({0}): {1}".format(e.errno, e.strerror)
          mqtt_logger.debug("Got HTTP error on update")
        except:
          mqtt_logger.debug("Got error on update")
      except:
        mqtt_logger.debug("Got error on update")


#mqttc.on_connect = on_connect
#mqttc.on_log = on_log
mqttc.on_message = on_message
#mqttc.on_publish = on_publish
#mqttc.on_subscribe = on_subscribe
 
# function to return a datastream object. This either creates a new datastream,
# or returns an existing one
def get_datastream(feed,feedid):
  try:
    datastream = feed.datastreams.get(feedid)
    return datastream
  except requests.HTTPError as e:
    print "HTTPError get_datastream({0}): {1}".format(e.errno, e.strerror)
    mqtt_logger.debug("Got HTTP error on get")
  except:
    mqtt_logger.debug("failed to get datastream " +feedid)
    #datastream = feed.datastreams.create(feedid, tags=feedid)
 
# main program entry point - runs continuously updating our datastream with the
# current 1 minute load average
def run():
  mqtt_logger.info("Starting mqtt")
 
  #connect to mqtt
  mqttc.connect("192.168.101.210")
  mqttc.subscribe("+/sensor/+",0)


  while True:

    mqttc.loop() 
    time.sleep(1)
 
run()

