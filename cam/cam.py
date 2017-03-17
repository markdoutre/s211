#!/usr/bin/env python
 
import os
import io
import time
import datetime
import paho.mqtt.client as paho
import logging
import logging.handlers
import picamera
from PIL import Image
from PIL import ImageDraw
from PIL import ImageFont

cam_logger = logging.getLogger('mqttCam')
cam_logger.setLevel(logging.DEBUG)

cam_handler = logging.handlers.SysLogHandler(address = '/dev/log')
formatter = logging.Formatter('%(name)s: %(levelname)s %(message)s')
cam_handler.setFormatter(formatter)
cam_logger.addHandler(cam_handler)


#create mqtt client
mqttc=paho.Client("mqttCam")

def on_message(mosq, obj, msg):
    cam_logger.debug((str(msg.payload) + ":" + msg.topic))
    rx_msg(msg)

def rx_msg(msg):
    topics=msg.topic.split('/')

    if topics[1] == "cam":
      #got a CMD
      if topics[2] == "cmd":
      # camera command


        stream = io.BytesIO()
        with picamera.PiCamera() as camera:
          camera.resolution = (640, 480)
          #camera.start_preview()
          try:
            # Camera warm-up time
            time.sleep(2)
            #camera.capture('/home/pi/openhab/webapps/images/cam.jpg')
            camera.capture(stream,format='jpeg')
            stream.seek(0)

            cam_logger.debug("done seek")

            with io.BytesIO() as output:
              with Image.open(stream) as img:
                draw =ImageDraw.Draw(img)
                draw.text((0,0),str(datetime.datetime.utcnow()),(255,255,255)) 
                img.save(output, 'JPEG')
              databuf = bytearray(output.getvalue())

            cam_logger.debug("sending image")
            mqttc.publish("211/cam/dbuf",databuf)
          finally:
            camera.close()
 
mqttc.on_message = on_message

def run():
  cam_logger.info("Starting cam")
  mqttc.connect("192.168.101.210")
  mqttc.subscribe("+/cam/+",0)

  while True:
    mqttc.loop()
    time.sleep(1)

run()
