# YES I tried alpine, no i wont build my my own OS for this docker to work
FROM ubuntu

# Set variable for snowfalke tzdata install
ENV TZ=Europe/Lisbon
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

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

# Set PySpark's Python path
ENV PYSPARK_PYTHON=/usr/bin/python3

COPY . /usr/src/app

EXPOSE 8080

# run the command to start uWSGI
CMD ["uwsgi", "app.ini"]
