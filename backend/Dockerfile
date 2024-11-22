FROM python:3.12.4

RUN mkdir -p /opt/backend
WORKDIR /opt/backend

ADD requirements.txt .
RUN pip3 install -r requirements.txt

ADD main.py .
ADD queries.py .
ADD applicant_service.py .

CMD python ./main.py