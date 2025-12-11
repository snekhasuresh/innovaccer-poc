# Use a small Python base image
FROM python:3.11-slim

# Set a working directory inside the container
WORKDIR /app

# Install system dependencies (if you later need gcc, librdkafka, etc.)
# RUN apt-get update && apt-get install -y --no-install-recommends \
#     build-essential librdkafka-dev && \
#     rm -rf /var/lib/apt/lists/*

# Copy requirement file and install Python dependencies first
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .

# Environment variables can be overridden at runtime
ENV PYTHONUNBUFFERED=1

# Default command to run the service
CMD ["python", "app.py"]
