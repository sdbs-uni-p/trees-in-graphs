FROM python:3.11-slim

RUN pip install --no-cache-dir kuzu pandas tqdm polars pyarrow

RUN mkdir -p /kuzu_data /app/init

COPY init/ /app/init/
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

WORKDIR /app

ENTRYPOINT ["/app/entrypoint.sh"]
