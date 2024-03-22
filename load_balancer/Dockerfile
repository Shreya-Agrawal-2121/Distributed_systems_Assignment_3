FROM ubuntu:20.04
WORKDIR /lb
COPY . .
RUN apt-get update
RUN apt-get install -y python3 python3-pip
RUN pip install --upgrade pip setuptools wheel

RUN pip install --no-cache-dir -r requirements.txt
RUN  apt-get install -y sqlite3 libsqlite3-dev
EXPOSE 5000
CMD ["python3", "lb.py"]