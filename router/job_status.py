from utils.database import fetchrow
from . import router
from fastapi import Depends, HTTPException, status

from auth import get_principal, Principal, check_db_scope_permission
from queue_redis import get_queue_position, get_task
from schemas import StatusResponse


@router.get("/api/v1/search/job/{task_id}/status", response_model=StatusResponse)
async def get_job_status(task_id: str, principal: Principal = Depends(get_principal)):
    # rate limit check
    # allowed = await check_rate_limit(principal.id)
    # if not allowed:
    #   raise HTTPException(status_code=status.HTTP_429_TOO_MANY_REQUESTS, detail="Too Many Requests")
    """
    先尝试从 Redis 读取 task:{task_id}（快速路径）。
    如果 Redis 中不存在该 key，则回退到 Postgres 的 tasks 表读取（持久化路径）。
    权限检查基于 principal.platform_id 与 principal.scopes。
    """
    # --------- Fast path: try Redis ----------
    data = await get_task(task_id)  # returns dict or None
    source = "redis"
    if not data:
        # --------- Fallback: try Postgres tasks table ----------
        source = "postgres"
        trow = await fetchrow(
            "SELECT owner, token_key, requested_db_scope, detected_mode, content, status, progress, error "
            "FROM tasks WHERE id = $1",
            task_id
        )
        if not trow:
            # Not found anywhere
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Not Found")

        # Normalize the row into same shape as Redis get_task produces
        data = {
            "owner": trow.get("owner"),
            "token_key": trow.get("token_key") or "",
            "requested_db_scope": trow.get("requested_db_scope") or [],
            "detected_mode": trow.get("detected_mode"),
            "query_text": trow.get("content"),
            "status": trow.get("status") or "PENDING",
            "progress": int(trow.get("progress") or 0),
            "error": trow.get("error"),
        }

    # --------- Permission check ----------
    # Allow if the principal's owner matches the task owner (platform-level ownership),
    # otherwise require that principal.scopes grants access to all private dbs used by the task.
    # owner = data.get("owner")
    token_key = data.get("token_key")
    # if principal.kind == "api_key" and principal.token_key == token_key:
    #     pass
    # elif owner and principal.owner and owner == principal.owner:
    #     pass
    # else
    #     raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Access denied")
    if not principal.token_key == token_key:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Access denied")
        # requested = data.get("requested_db_scope", []) or []
        # ok, bad_scope = check_db_scope_permission(principal, requested)
        # if not ok:
        #     raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Access denied to private DB")

    # --------- Queue position ----------
    # If the status is PENDING and the task still exists in Redis queue, compute its position.
    # If we read from Postgres (meaning Redis key likely expired / migrated), we default queue_position to 0.
    stat = data.get("status")
    if stat == "PENDING" and source == "redis":
        pos = await get_queue_position(task_id)
        queue_pos = pos if pos is not None else 0
    else:
        queue_pos = 0

    # --------- Build response ----------
    resp = {
        "task_id": task_id,
        "status": stat,
        "progress": int(data.get("progress") or 0),
        "queue_position": int(queue_pos),
        "search_meta": None,
        "error": data.get("error") or None,
    }

    # Intent feedback (detected_mode, query_text, db_scope_used) if available
    if data.get("detected_mode"):
        resp["search_meta"] = {
            "detected_mode": data.get("detected_mode"),
            "query_text": data.get("query_text"),
            "db_scope_used": data.get("requested_db_scope") or []
        }

    return resp
