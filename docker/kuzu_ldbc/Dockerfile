# SPDX-License-Identifier: GPL-3.0-only

FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends librsvg2-bin poppler-utils \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir kuzu==0.11.3 pandas tqdm

RUN mkdir -p /app /kuzu_data
COPY kuzu/init/00_init_kuzu.py /app/base_init.py
COPY kuzu_ldbc/init.py kuzu_ldbc/entrypoint.sh /app/
RUN chmod +x /app/entrypoint.sh
WORKDIR /project
ENTRYPOINT ["/app/entrypoint.sh"]
