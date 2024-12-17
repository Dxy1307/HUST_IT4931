# Use an official Python runtime as a parent image
FROM python:3.8-slim

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container
COPY . /app

# Install system dependencies for Kafka and HDFS
RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Install required Python packages
RUN pip install --no-cache-dir \
    pandas \
    confluent-kafka \
    pyhdfs

# Set the default command to run the Kafka consumer
CMD ["python", "consumer_to_hdfs.py"]
