FROM joyzoursky/python-chromedriver:latest

RUN mkdir -p /opt/parser
WORKDIR /opt/parser

ADD requirements.txt .
RUN pip3 install -r requirements.txt

RUN apt update
RUN apt install -y --fix-missing nmap

ADD const.py .
ADD main.py .
