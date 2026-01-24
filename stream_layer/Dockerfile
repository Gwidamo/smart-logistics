# Use a lightweight Python base
FROM python:3.9-slim

# Set working directory
WORKDIR /app

# Install the required library
RUN pip install kafka-python

# The command will be overridden by docker-compose, 
# but this is a good default to keep the container alive or run the script
CMD ["python", "-u", "/app/stream_layer/producer.py"]