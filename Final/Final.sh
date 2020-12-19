#!/usr/bin/env bash

FILE=baseball.sql

if [ -f "$FILE" ]; then
    echo "$FILE exists."
else
  echo "$FILE not found. Downloading."
  curl -O https://teaching.mrsharky.com/data/baseball.sql.tar.gz
  tar -xvzf baseball.sql.tar.gz
fi

docker-compose -v down

docker-compose up -d --build mysql

docker exec -it mysql_db bash -c "chmod -R 0777 /data/"

docker-compose up -d --build code