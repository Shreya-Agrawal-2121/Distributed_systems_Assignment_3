FROM ubuntu:20.04

RUN apt-get update
RUN apt-get install -y python3 python3-pip
RUN pip install --upgrade pip setuptools wheel
EXPOSE 5000
COPY requirements.txt .
RUN pip install  -r requirements.txt
RUN pip install flask[async]
RUN  apt-get install -y sqlite3 libsqlite3-dev

COPY . .

CMD ["python3", "app.py"]