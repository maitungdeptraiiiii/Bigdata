FROM python:3.10-slim

WORKDIR /app

COPY backend /app/backend
COPY frontend /app/frontend

RUN pip install -r /app/backend/requirements.txt

CMD ["python3", "/app/backend/app.py"]
