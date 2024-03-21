FROM mysql:8.0-debian

COPY deploy.sh /always-initdb.d/
COPY . /lb
WORKDIR /lb

RUN apt-get update
RUN apt-get install -y python3
RUN apt-get install -y python3-pip
RUN apt-get install -y python3-flask python3-flask-cors   python3-mysqldb \
    python3-docker python3-requests

RUN chmod +x /lb/custom-entry.sh

ENV USER=theuser
RUN adduser --home /home/$USER --disabled-password --gecos GECOS $USER \
  && chsh -s /bin/zsh $USER
RUN touch /lb/topicLog.db
RUN chown $USER /lb/topicLog.db

USER $USER
ENV HOME=/home/$USER
ENV MYSQL_ROOT_PASSWORD="abc"

EXPOSE 5000

ENTRYPOINT ["/lb/custom-entry.sh"]
CMD ["mysqld"]