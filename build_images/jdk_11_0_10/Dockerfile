# Build on the connect version we're running to ensure compatibility
FROM openjdk:11.0.10-jdk-slim

RUN mkdir -p /usr/share/man/man1
RUN apt-get -y update \
    && apt-get -y install git maven jq unzip \
    && apt-get clean

ADD ./ build/

RUN cd build/ \
    && mvn clean install -DskipITs -DskipTests