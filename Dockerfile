FROM python:3.12-slim

ENV PYTHONUNBUFFERED = 1
ENV DONTWRITEBYTECODE = 1

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .