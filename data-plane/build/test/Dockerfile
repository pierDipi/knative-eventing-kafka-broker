FROM maven:3.6.3-openjdk-14 as builder

COPY data-plane/ /app

RUN cd /app && mvn package
