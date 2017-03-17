#!/bin/sh
now=$(date +"%s")
mqttid=$(hostname -I|cut -d"." -f4)
#mqttid=210
temp=$(/opt/vc/bin/vcgencmd measure_temp| egrep "[0-9.]{4,}" -o)
echo $now $temp| mosquitto_pub -h 192.168.101.210 -t `echo $mqttid`/sensor/temperature -l
mosquitto_pub -h 192.168.101.210 -t 211/cam/cmd -n
