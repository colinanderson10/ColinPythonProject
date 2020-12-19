FROM ubuntu:latest

COPY baseball.sql /data/baseball.sql
COPY FINAL.sql /scripts/FINAL.sql
COPY code.sh /scripts/code.sh
COPY Final4.py /scripts/Final4.py
COPY requirements.dev.txt /scripts/requirements.dev.txt
COPY requirements.txt /scripts/requirements.txt
COPY mysql-connector-java-8.0.21.jar /data/mysql-connector-java-8.0.21.jar

RUN apt-get update
RUN apt-get install -y mysql-client
RUN apt-get install -y openjdk-8-jdk
RUN apt-get install -y python3
RUN apt-get install -y python3-pip
RUN chmod +x /scripts/code.sh
RUN mkdir /results

CMD scripts/code.sh
