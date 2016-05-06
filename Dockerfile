FROM ubuntu:14.04

MAINTAINER Team Aruha, team-aruha@zalando.de

RUN apt-get update && apt-get upgrade
RUN apt-get install -y python-yaml python-pip
RUN pip install --upgrade stups-tokens click

WORKDIR /
ADD end2end /end2end
ADD run.py /run.py
ADD test.yaml /test.yaml
ADD scm-source.json /scm-source.json

EXPOSE 8080

# run the server when a container based on this image is being run
ENTRYPOINT python /run.py --config /test.yaml --port 8080

