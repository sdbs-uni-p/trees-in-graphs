# SPDX-License-Identifier: GPL-3.0-only

FROM apache/age:release_PG16_1.5.0

RUN apt-get update \
	&& apt-get install -y --no-install-recommends postgresql-16-pg-hint-plan \
	&& rm -rf /var/lib/apt/lists/*

COPY run-all-init.sh /usr/local/bin/run-all-init.sh
COPY entrypoint-resumable-init.sh /usr/local/bin/entrypoint-resumable-init.sh

RUN chmod +x /usr/local/bin/run-all-init.sh /usr/local/bin/entrypoint-resumable-init.sh

ENTRYPOINT ["/usr/local/bin/entrypoint-resumable-init.sh"]
CMD ["postgres", "-c", "shared_preload_libraries=age,pg_stat_statements,auto_explain,pg_hint_plan"]
