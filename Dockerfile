FROM python:3.12-slim

WORKDIR /app

COPY pyproject.toml README.md ./
COPY src ./src

RUN pip install --no-cache-dir -e ".[api]"

# The demo run: generate 1000 synthetic orders, run the pipeline, print the
# report, and leave the lakehouse on disk under /data for inspection.
ENV LAKEHOUSE_ROOT=/data/lakehouse
VOLUME ["/data"]

CMD ["python", "-m", "lakehouse_platform.cli", "--events", "1000", "--out", "/data/lakehouse"]
