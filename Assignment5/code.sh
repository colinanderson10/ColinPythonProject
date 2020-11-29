#!/bin/sh

if ! mysql -h mysql_db -uroot -psecret -e 'use baseball'; then
  mysql -h mysql_db -uroot -psecret -e "create database baseball;"
  mysql -h mysql_db -uroot -psecret -D baseball < /data/baseball.sql
fi

mysql -h mysql_db -uroot -psecret baseball < /scripts/Assignment5Batting.sql