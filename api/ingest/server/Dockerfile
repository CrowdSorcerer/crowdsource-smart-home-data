FROM alpine

# Get bash
RUN apk add --update --no-cache bash

# Get python and pip
RUN apk add --update --no-cache python3 && ln -sf python3 /usr/bin/python
RUN python3 -m ensurepip

# Get java
RUN apk add openjdk8

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

COPY requirements.txt /usr/src/app/

RUN pip3 install --no-cache-dir -r requirements.txt

COPY . /usr/src/app

EXPOSE 8080

ENTRYPOINT ["python3"]

ENV INGEST_BASE_PATH=file:///tmp

CMD ["-m", "crowdsorcerer_server_ingest"]