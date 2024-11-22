# FROM selenium/standalone-chrome:126.0
FROM joyzoursky/python-chromedriver:3.8

# # install google chrome
# RUN wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add -
# RUN sh -c 'echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list'
# RUN apt-get -y update
# RUN apt-get install -y google-chrome-stable
#
# # install chromedriver
# RUN apt-get install -yqq unzip
# RUN wget -O /tmp/chromedriver.zip http://chromedriver.storage.googleapis.com/`curl -sS chromedriver.storage.googleapis.com/LATEST_RELEASE`/chromedriver_linux64.zip
# RUN unzip /tmp/chromedriver.zip chromedriver -d /usr/local/bin/
#USER root
#RUN apt install software-properties-common
#RUN apt update
#RUN add-apt-repository universe
#RUN apt update
#RUN apt install python3 -y
#RUN apt install python3-pip -y

RUN mkdir -p /opt/parser
WORKDIR /opt/parser

ADD requirements.txt .
RUN pip3 install -r requirements.txt

ADD main.py .

CMD python ./main.py