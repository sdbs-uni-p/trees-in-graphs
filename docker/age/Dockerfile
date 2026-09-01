# SPDX-License-Identifier: GPL-3.0-only

FROM apache/age:release_PG16_1.5.0

COPY run-all-init.sh /usr/local/bin/run-all-init.sh
COPY entrypoint-resumable-init.sh /usr/local/bin/entrypoint-resumable-init.sh

RUN chmod +x /usr/local/bin/run-all-init.sh /usr/local/bin/entrypoint-resumable-init.sh

ENTRYPOINT ["/usr/local/bin/entrypoint-resumable-init.sh"]
CMD ["postgres", "-c", "shared_preload_libraries=age,pg_stat_statements,auto_explain"]
