FROM python:3.6.8

WORKDIR /app

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

ADD . /app

RUN python --version
RUN pip --version

RUN pip install --upgrade pip
RUN pip install -r requirements.txt

EXPOSE 5000

ENV SQL_SERVER_NAME=SQL_SERVER_NAME
ENV SQL_DB_NAME=SQL_DB_NAME
ENV SQL_USER_NAME=DB_USER_NAME
ENV SQL_PSWD=SQL_PSWD

CMD ["python", "app.py"]
