FROM python:3.6.8

ARG APP_DIR=/home/app

RUN apt-get update \
 && apt-get install unixodbc -y \
 && apt-get install unixodbc-dev -y \
 && apt-get install freetds-dev -y \
 && apt-get install freetds-bin -y \
 && apt-get install tdsodbc -y \
 && apt-get install --reinstall build-essential -y

RUN echo "[FreeTDS]\n\
Description=FreeTDS Driver\n\
Driver=/usr/lib/x86_64-linux-gnu/odbc/libtdsodbc.so\n\
Setup=/usr/lib/x86_64-linux-gnu/odbc/libtdsS.so" >> /etc/odbcinst.ini

COPY ./deployment/app/requirements.txt requirements.txt

RUN pip install --upgrade pip
RUN pip install -r requirements.txt

RUN mkdir -p ${APP_DIR}
WORKDIR ${APP_DIR}

RUN chmod a+rwx ${APP_DIR}

ENV SQL_SERVER_NAME=__SQL_SERVER_NAME__
ENV SQL_DB_NAME=__SQL_DB_NAME__
ENV SQL_USER_NAME=__SQL_USER_NAME__
ENV SQL_PSWD=__SQL_PSWD__

COPY ./src/ ${APP_DIR}
CMD ["python", "app.py"]
