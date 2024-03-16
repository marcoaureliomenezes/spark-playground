FROM python:3.8-slim-buster

WORKDIR /app

COPY ./requirements.txt /app
COPY ./src /app

RUN pip install --upgrade pip && \
    pip install -r requirements.txt



ENTRYPOINT [ "sleep", "infinity" ]