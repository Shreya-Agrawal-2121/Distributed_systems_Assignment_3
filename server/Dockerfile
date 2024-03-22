FROM ubuntu:20.04
COPY . .
RUN apt-get update
RUN apt-get install -y python3 python3-pip
RUN pip install --upgrade pip setuptools wheel

RUN pip install --no-cache-dir -r requirements.txt
RUN  apt-get install -y sqlite3 libsqlite3-dev
CMD ["python3", "app.py"]