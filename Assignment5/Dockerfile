FROM ubuntu:latest

COPY baseball.sql /data/baseball.sql
COPY Assignment5Batting.sql /scripts/Assignment5Batting.sql
COPY code.sh /scripts/code.sh

RUN apt-get update
RUN apt-get install -y mysql-client
RUN chmod +x /scripts/code.sh
RUN mkdir /results

CMD scripts/code.sh
