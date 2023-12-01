FROM python:3.9

RUN mkdir -p /opt/backend
WORKDIR /opt/backend

ADD requirements.txt .
RUN pip3 install -r requirements.txt

ADD main.py .
ADD queries.py .

CMD python ./main.py