#!/bin/bash

test -z "$1" && echo "нужен файл коннектов" && exit

pid=$(pgrep -f "bonus/insert.sh $1")
if [ -z $(echo $pid) ]; then
  bash /etc/bash_etl/crossEntry/bonus/insert.sh $1
else
  echo "Расчет бонусов уже запущен"
fi



