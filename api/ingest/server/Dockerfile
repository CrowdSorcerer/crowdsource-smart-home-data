FROM python:3.8.13-alpine

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

COPY requirements.txt /usr/src/app/

RUN pip3 install --no-cache-dir -r requirements.txt

COPY . /usr/src/app

EXPOSE 8080

ENTRYPOINT ["python3"]

ENV INGEST_BASE_PATH=file:///tmp

CMD ["-m", "crowdsorcerer_server_ingest"]
