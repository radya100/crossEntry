#!/bin/bash

cd /etc/bash_etl/crossEntry/ || return
test -z "$1" && echo "нужен файл коннектов" && exit

pid=$(pgrep -f "bonus/insert.sh $1")
if [ -z $(echo $pid) ]; then
#  bash bonus/insert.sh $1
  echo bonus/insert.sh $1
else
  echo "Расчет бонусов уже запущен"
fi



