FROM python:3.11-slim
EXPOSE 8000

WORKDIR /app

COPY requirements.txt requirements.txt
COPY /app .

RUN pip3 install -r requirements.txt

CMD python3 app.py