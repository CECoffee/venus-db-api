# app/config.py
import os
from typing import Dict

DB_CONFIG: Dict[str, object] = {
    "host": "localhost",
    "port": 5432,
    "dbname": "venusDB_API",
    "user": "postgres",
    "password": "0909",
}

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

# JWT config (production: use env vars / secrets manager)
# JWT_SECRET = os.getenv("JWT_SECRET", "change-me-in-prod")
# JWT_ALGORITHM = os.getenv("JWT_ALGORITHM", "HS256")

# Back-compat API keys (optional). In prod these would live in DB or Vault.
# API_KEYS = {
#     "sk_live_88889999": {"owner": "researcher_a", "scopes": ["deep_sea_meer_1", "group:public"]},
#     "sk_public_demo": {"owner": "demo", "scopes": ["group:public"]},
# }

# default db scope
DEFAULT_DB_SCOPE = ["group:public"]

LANGUAGE_CODES = ["en_us", "zh_cn"]

# Redis keys / namespaces
QUEUE_KEY = "search_queue"             # Redis list storing task IDs (RPUSH)
TASK_HASH_PREFIX = "task:"             # full key: task:{task_id}

SLURM_PARTITION = "CPU"
TASK_WORKDIR_BASE = "/tmp/slurm-workspace"
SLURM_USER= "`whoami`"