#!/bin/sh

sleep 10

pip3 install -r /scripts/requirements.dev.txt
pip3 install -r /scripts/requirements.txt

cp /data/mysql-connector-java-8.0.21.jar /usr/local/lib/python3.8/dist-packages/pyspark/jars/

if ! mysql -h mysql_db -uroot -psecret -e 'use baseball'; then
  mysql -h mysql_db -uroot -psecret -e 'create database baseball;'
  mysql -h mysql_db -uroot -psecret baseball < /data/baseball.sql
fi

mysql -h mysql_db -uroot -psecret baseball < /scripts/FINAL.sql

python3 /scripts/Final4.py