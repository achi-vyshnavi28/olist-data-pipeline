FROM python:3.12-slim-bookworm
# Spark needs a Java runtime
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 PYTHONUNBUFFERED=1
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY pipeline pipeline
COPY data/sample data/sample
# mount the full Olist folder and set PIPELINE_RAW to run on all of it
CMD ["python", "-m", "pipeline.run"]
