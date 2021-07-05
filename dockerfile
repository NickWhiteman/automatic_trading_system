FROM python:3.9
RUN apt -y update
RUN apt install nano
RUN apt -y upgrade

ENV FLASK_APP=main.py
ENV FLASK_RUN_HOST=0.0.0.0

COPY . /server
WORKDIR /server

RUN pip install -r requirements.txt

EXPOSE 5000
CMD ["flask", "run"]