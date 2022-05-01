# YES I tried alpine, no i wont build my my own OS for this docker to work
FROM ubuntu

# Update and 
RUN apt-get update

# Get python
RUN apt-get -y install python3

# Get pip
RUN apt-get -y install python3-pip

# Get java
RUN apt-get -y install openjdk-8-jre

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

COPY requirements.txt /usr/src/app/

RUN pip3 install --no-cache-dir -r requirements.txt

COPY . /usr/src/app

EXPOSE 8080

ENV INGEST_BASE_PATH=file:///tmp

# run the command to start uWSGI
CMD ["uwsgi", "app.ini"]
