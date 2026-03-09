# SPDX-License-Identifier: GPL-3.0-only

FROM python:3.11-slim

RUN pip install --no-cache-dir neo4j pandas tqdm

RUN mkdir -p /app/init

COPY init/ /app/init/
COPY entrypoint.sh /app/entrypoint.sh
RUN chmod +x /app/entrypoint.sh

WORKDIR /app

ENTRYPOINT ["/app/entrypoint.sh"]
