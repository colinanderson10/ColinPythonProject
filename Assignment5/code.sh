#!/bin/sh

sleep 10

if ! mysql -h mysql_db -uroot -psecret -e 'use baseball'; then
  mysql -h mysql_db -uroot -psecret -e "create database baseball;"
  mysql -h mysql_db -uroot -psecret -D baseball < /data/baseball.sql
fi

mysql -h mysql_db -uroot -psecret baseball < /scripts/Assignment5Batting.sql

mysql -h mysql_db -uroot -psecret baseball -e '
  SELECT *
  FROM allbatterrollingbattingaverage;' > /results/all_players_rolling_avg.txt

mysql -h mysql_db -uroot -psecret baseball -e '
  SELECT *
  FROM gamerollingbattingaverage;' > /results/game_players_rolling_avg.txt
