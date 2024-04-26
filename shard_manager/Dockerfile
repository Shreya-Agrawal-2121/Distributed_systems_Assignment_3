FROM ubuntu:20.04
WORKDIR /sm

RUN apt-get update
RUN apt-get install -y python3 python3-pip
RUN pip install --upgrade pip setuptools wheel

COPY requirements.txt .
RUN pip install  -r requirements.txt
RUN  apt-get install -y sqlite3 libsqlite3-dev

COPY . .

EXPOSE 5000
CMD ["python3", "sm.py"]