# app/queue_redis.py
import time
import uuid
import asyncio
import json
from typing import Dict, Any, Optional, List
from utils.redis_cli import get_redis
from config import QUEUE_KEY, TASK_HASH_PREFIX

from utils.database import execute  # 用于后续转存到 Postgres

redis = get_redis()

async def push_job_to_queue(job: Dict[str, Any]) -> str:
    task_id = job.get("task_id") or f"job_{uuid.uuid4().hex}"
    job["task_id"] = task_id
    job_key = TASK_HASH_PREFIX + task_id

    to_store = {
        "task_id": task_id,
        "created_at": str(job.get("created_at", time.time())),
        "owner": job.get("owner"),
        "token_key": job.get("token_key"),
        "content": job.get("content"),
        "input_mode": job.get("input_mode"),
        "requested_db_scope": ",".join(job.get("requested_db_scope")),
        "filters": json.dumps(job.get("filters") or {}),
        "status": job.get("status", "PENDING"),
        "progress": str(job.get("progress", 0)),
        "detected_mode": job.get("detected_mode", job.get("input_mode")),
        "error": job.get("error") or "",
    }

    await redis.hset(job_key, mapping=to_store)
    await redis.rpush(QUEUE_KEY, task_id)

    # don't persist to Postgres here if you already do that in other place
    return task_id

async def get_task(task_id: str) -> Optional[Dict[str, Any]]:
    job_key = TASK_HASH_PREFIX + task_id
    exists = await redis.exists(job_key)
    if not exists:
        return None
    data = await redis.hgetall(job_key)
    # parse fields
    data_parsed = dict(data)
    # convert fields
    data_parsed["requested_db_scope"] = data_parsed.get("requested_db_scope", "")
    if data_parsed["requested_db_scope"]:
        data_parsed["requested_db_scope"] = data_parsed["requested_db_scope"].split(",")
    else:
        data_parsed["requested_db_scope"] = []
    data_parsed["token_key"] = data_parsed.get("token_key")
    data_parsed["progress"] = int(data_parsed.get("progress") or 0)
    data_parsed["status"] = data_parsed.get("status") or "PENDING"
    data_parsed["error"] = data_parsed.get("error") or None
    data_parsed["detected_mode"] = data_parsed.get("detected_mode") or None
    data_parsed["query_text"] = data_parsed.get("content")
    return data_parsed

async def set_task_fields(task_id: str, mapping: Dict[str, Any]):
    job_key = TASK_HASH_PREFIX + task_id
    str_map = {}
    for k, v in mapping.items():
        if isinstance(v, (dict, list)):
            str_map[k] = json.dumps(v)
        else:
            str_map[k] = "" if v is None else str(v)
    await redis.hset(job_key, mapping=str_map)

# --- results helpers ---
async def set_task_results(task_id: str, results: List[Dict[str, Any]], total: int):
    """
    Write results array to task:{task_id} hash as JSON (field 'results').
    Set status= DONE and progress=100 and set TTL 600 seconds.
    """
    job_key = TASK_HASH_PREFIX + task_id
    payload = {
        "total": total,
        "results": results
    }
    await redis.hset(job_key, mapping={
        "results": json.dumps(payload),
        "status": "DONE",
        "progress": "100",
        "error": ""
    })
    data = await redis.hgetall(job_key)
    # ensure tasks table has task metadata (upsert)
    try:
        requested_db_scope = []
        req_scope_raw = data.get("requested_db_scope") or ""
        if req_scope_raw:
            requested_db_scope = req_scope_raw.split(",")
        try:
            filters = json.loads(data.get("filters") or "{}")
        except Exception:
            filters = {}

        await execute(
            """
            INSERT INTO tasks (id, owner, content, input_mode, detected_mode, requested_db_scope, filters,
                               status, progress, error, created_at, token_key)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, to_timestamp($11), $12)
            ON CONFLICT (id) DO UPDATE
                SET owner              = EXCLUDED.owner,
                    content            = EXCLUDED.content,
                    input_mode         = EXCLUDED.input_mode,
                    detected_mode      = EXCLUDED.detected_mode,
                    requested_db_scope = EXCLUDED.requested_db_scope,
                    filters            = EXCLUDED.filters,
                    status             = EXCLUDED.status,
                    progress           = EXCLUDED.progress,
                    error              = EXCLUDED.error,
                    updated_at         = now(),
                    token_key          = EXCLUDED.token_key
            """,
            task_id,
            data.get("owner"),
            data.get("content"),
            data.get("input_mode"),
            data.get("detected_mode"),
            requested_db_scope,
            json.dumps(filters),
            data.get("status") or "DONE",
            int(data.get("progress") or 100),
            data.get("error") or None,
            float(data.get("created_at") or time.time()),
            data.get("token_key") or None,
        )
    except Exception as e:
        print(f"[schedule_transfer] Failed to upsert task metadata for {task_id}: {e}")
        # do not abort migration; try to persist results anyway

    # persist results into results table
    try:
        await execute(
            """
            INSERT INTO results (task_id, total, results, meta)
            VALUES ($1, $2, $3::jsonb, $4::jsonb)
            ON CONFLICT (task_id) DO UPDATE
                SET total      = EXCLUDED.total,
                    results    = EXCLUDED.results,
                    meta       = EXCLUDED.meta,
                    created_at = now()
            """,
            task_id,
            total,
            json.dumps(results),
            json.dumps({})
        )
    except Exception as e:
        print(f"[schedule_transfer] Failed to persist results for {task_id}: {e}")
    # set TTL 600s
    await redis.expire(job_key, 600)

async def get_task_results_from_redis(task_id: str) -> Optional[Dict[str, Any]]:
    job_key = TASK_HASH_PREFIX + task_id
    exists = await redis.exists(job_key)
    if not exists:
        return None
    data = await redis.hget(job_key, "results")
    if not data:
        return None
    return json.loads(data)  # returns {"total":..., "results":[...]}

# async def schedule_transfer_to_postgres_after_delay(task_id: str, delay_seconds: int = 600):
#     """
#     Sleep 'delay_seconds' then attempt to move results from Redis -> Postgres results table,
#     and delete the redis key.
#     """
#     await asyncio.sleep(delay_seconds)
#     job_key = TASK_HASH_PREFIX + task_id
#     # try to read results (if someone already removed it, skip)
#     exists = await redis.exists(job_key)
#     if not exists:
#         return
#     data = await redis.hgetall(job_key)
#     if not data:
#         return
#
#     # parse results subfield
#     results_json = data.get("results")
#     if not results_json:
#         # nothing to persist
#         return
#     try:
#         payload = json.loads(results_json)
#     except Exception as e:
#         print(f"[schedule_transfer] Invalid JSON results for {task_id}: {e}")
#         return
#
#     total = payload.get("total", len(payload.get("results", [])))
#     results_list = payload.get("results", [])
#
#     # ensure tasks table has task metadata (upsert)
#     try:
#         requested_db_scope = []
#         req_scope_raw = data.get("requested_db_scope") or ""
#         if req_scope_raw:
#             requested_db_scope = req_scope_raw.split(",")
#         filters = {}
#         try:
#             filters = json.loads(data.get("filters") or "{}")
#         except Exception:
#             filters = {}
#
#         await execute(
#             """
#             INSERT INTO tasks (id, owner, content, input_mode, detected_mode, requested_db_scope, filters,
#                                status, progress, error, created_at)
#             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, to_timestamp($11))
#             ON CONFLICT (id) DO UPDATE
#                 SET owner              = EXCLUDED.owner,
#                     content            = EXCLUDED.content,
#                     input_mode         = EXCLUDED.input_mode,
#                     detected_mode      = EXCLUDED.detected_mode,
#                     requested_db_scope = EXCLUDED.requested_db_scope,
#                     filters            = EXCLUDED.filters,
#                     status             = EXCLUDED.status,
#                     progress           = EXCLUDED.progress,
#                     error              = EXCLUDED.error,
#                     updated_at         = now()
#             """,
#             task_id,
#             data.get("owner"),
#             data.get("content"),
#             data.get("input_mode"),
#             data.get("detected_mode"),
#             requested_db_scope,
#             json.dumps(filters),
#             data.get("status") or "DONE",
#             int(data.get("progress") or 100),
#             data.get("error") or None,
#             float(data.get("created_at") or time.time())
#         )
#     except Exception as e:
#         print(f"[schedule_transfer] Failed to upsert task metadata for {task_id}: {e}")
#         # do not abort migration; try to persist results anyway
#
#     # persist results into results table
#     try:
#         await execute(
#             """
#             INSERT INTO results (task_id, total, results, meta)
#             VALUES ($1, $2, $3::jsonb, $4::jsonb)
#             ON CONFLICT (task_id) DO UPDATE
#                 SET total      = EXCLUDED.total,
#                     results    = EXCLUDED.results,
#                     meta       = EXCLUDED.meta,
#                     created_at = now()
#             """,
#             task_id,
#             total,
#             json.dumps(results_list),
#             json.dumps({
#                 "migrated_from_redis_at": time.time(),
#                 "source": "redis"
#             })
#         )
#     except Exception as e:
#         print(f"[schedule_transfer] Failed to persist results for {task_id}: {e}")
#
#     # finally delete redis key
#     try:
#         await redis.delete(job_key)
#     except Exception as e:
#         print(f"[schedule_transfer] Failed to delete redis key for {task_id}: {e}")


# helper to pop from queue (blocking)
async def pop_task_from_queue(block_timeout: int = 5) -> Optional[str]:
    # use BRPOP; returns (key, value) or None
    try:
        res = await redis.brpop([QUEUE_KEY], timeout=block_timeout)
        if not res:
            return None
        # res is (queue_key, task_id)
        return res[1]
    except Exception as e:
        # fallback: try LPOP (non-blocking)
        try:
            val = await redis.lpop(QUEUE_KEY)
            return val
        except Exception:
            return None

async def get_queue_position(task_id: str) -> Optional[int]:
    """
    Use LPOS to find index. If not supported on Redis server, fallback to LRANGE scan (less efficient).
    Returns 0-based index, or None if not found.
    """
    try:
        pos = await redis.lpos(QUEUE_KEY, task_id)
        if pos is None:
            return None
        return int(pos)
    except Exception:
        # fallback: scan full list
        lst = await redis.lrange(QUEUE_KEY, 0, -1)
        try:
            return lst.index(task_id)
        except ValueError:
            return None
