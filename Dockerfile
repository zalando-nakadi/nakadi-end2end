FROM registry.opensource.zalan.do/stups/python:3.5.1-33

MAINTAINER Team Aruha, team-aruha@zalando.de

RUN apt-get update && apt-get install python3-pycurl

RUN pip3 install --upgrade stups-tokens click pyyaml tornado

WORKDIR /
ADD end2end /end2end
ADD run.py /run.py
ADD test.yaml /test.yaml
ADD scm-source.json /scm-source.json

EXPOSE 8080

# run the server when a container based on this image is being run
ENTRYPOINT python /run.py --config /test.yaml --port 8080

