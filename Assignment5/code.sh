#!/bin/sh


mysql -h mysql_db -uroot -psecret -e "create database baseball;"
mysql -h mysql_db -uroot -psecret -D baseball < /data/baseball.sql
mysql -h mysql_db -uroot -psecret baseball < /scripts/Assignment5Batting.sql