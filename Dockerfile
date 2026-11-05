# Supported, minimal base image (python:3.8 is end of life)
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# Install dependencies first so this layer is cached across code changes
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy only what the pipeline needs to run; never the whole build context
COPY main.py .
COPY src/ src/

# Run as an unprivileged user
RUN useradd --create-home --uid 10001 etl
USER etl

CMD ["python", "./main.py"]
