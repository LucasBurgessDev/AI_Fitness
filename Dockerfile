# Use a small, compatible Python base
FROM python:3.11-slim

# Prevent python from writing .pyc and buffer stdout
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Cloud Run writes to /tmp
ENV SAVE_PATH=/tmp

WORKDIR /app

# System deps: CA certs for HTTPS, build essentials for any wheels
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Install deps first to leverage docker layer caching
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy the rest of the repo
COPY . /app

# Make sure a /tmp exists (it will on Cloud Run, harmless locally)
RUN mkdir -p /tmp

# Default command for Cloud Run Job
CMD ["python", "cloud_run_entrypoint.py"]
