#!/bin/sh

APP_NAME=hystrix-dashboard.jar
nohup  java -jar $APP_NAME >>logs/start.log 2>>logs/startError.log &

sleep 15

if test $(pgrep -f $APP_NAME|wc -l) -eq 0
then
   echo "start failed"
else
   echo "start successed"
fi