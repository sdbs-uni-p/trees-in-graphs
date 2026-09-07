# SPDX-License-Identifier: GPL-3.0-only
"""Write run configuration before measurements, without connection credentials."""
import json
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path


def write_run_metadata(output_dir, *, workload, executors, runs, warmup, **settings):
    repo = Path(__file__).resolve().parents[2]
    commit = os.environ.get('GIT_COMMIT') or None
    if commit is None:
        try:
            commit = subprocess.check_output(
                ['git', '-C', str(repo), 'rev-parse', 'HEAD'], stderr=subprocess.DEVNULL,
                text=True,
            ).strip()
        except (OSError, subprocess.CalledProcessError):
            pass
    payload = {
        'schema_version': 1,
        'created_at': datetime.now(timezone.utc).isoformat(),
        'git_commit': commit,
        'workload': workload,
        'executors': {method: type(executor).__name__ for method, executor in executors.items()},
        'runs': runs, 'warmup_runs': warmup,
        'timeout_ms': None,
        'timeout_source': 'not set by runner; database/driver settings may apply',
        'settings': settings,
    }
    (Path(output_dir) / 'metadata.json').write_text(
        json.dumps(payload, indent=2, ensure_ascii=False, default=str) + '\n', encoding='utf-8',
    )
