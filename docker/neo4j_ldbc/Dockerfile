# SPDX-License-Identifier: GPL-3.0-only

FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends librsvg2-bin poppler-utils \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir neo4j pandas tqdm

RUN mkdir -p /app
COPY neo4j/init/00_init_neo4j.py /app/base_init.py
COPY neo4j_ldbc/init.py neo4j_ldbc/entrypoint.sh /app/
RUN chmod +x /app/entrypoint.sh
WORKDIR /project
ENTRYPOINT ["/app/entrypoint.sh"]
