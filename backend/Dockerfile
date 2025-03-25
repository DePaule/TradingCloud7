FROM python:3.11-slim

WORKDIR /app

# Install Alembic and project dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
    && pip install alembic

# Copy your app code
COPY app/ ./app

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
