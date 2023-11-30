FROM joyzoursky/python-chromedriver:latest

RUN mkdir -p /opt/parser
WORKDIR /opt/parser

ADD requirements.txt .
RUN pip3 install -r requirements.txt

ADD const.py .
ADD main.py .
