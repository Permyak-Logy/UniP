FROM python3:latest

RUN mkdir -p /opt/backend
WORKDIR /opt/backend

ADD requirements.txt .
RUN pip3 install -r requirements.txt

ADD main.py .
