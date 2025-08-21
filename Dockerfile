
FROM python:3.12-slim

WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1


RUN pip install --no-cache-dir fastapi uvicorn[standard] aiokafka

COPY . /app

CMD ["tail", "-f", "/dev/null"]