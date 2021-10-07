FROM python:3.8

ADD . /api

WORKDIR /api

RUN apt-get update \
  && apt-get install -y python3-pip python3-dev \
  && pip3 install --upgrade pip

RUN pip3 install -r requirements.txt