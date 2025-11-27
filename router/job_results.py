import json
import math

from . import router
from fastapi import Depends, HTTPException, status, Query

from auth import get_principal, Principal
from utils.database import fetchrow

def principal_can_view_task(principal: Principal, task_meta: dict) -> bool:
    token_key = task_meta.get("token_key") or ""
    if principal.token_key == token_key:
        return True
    return False

@router.get("/api/v1/search/job/{job_id}/results")
async def get_results(
    job_id: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    principal: Principal = Depends(get_principal)
):
    row = await fetchrow("SELECT total, results FROM results WHERE task_id = $1", job_id)
    if not row:
        # maybe task never existed or expired
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job Not Found")

    total = row.get("total", 0)
    results_list = json.loads(row["results"])
    total_pages = math.ceil(total / page_size) if page_size else 1
    if page > total_pages != 0:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Pagination Error")

    start = (page - 1) * page_size
    end = start + page_size
    page_results = results_list[start:end]

    # fetch task metadata from tasks table for permission check
    trow = await fetchrow("SELECT owner, requested_db_scope, detected_mode, content, token_key FROM tasks WHERE id = $1", job_id)
    if not trow:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Job Not Found")

    task_meta = {
        "owner": trow["owner"],
        "requested_db_scope": trow["requested_db_scope"] or [],
        "detected_mode": trow["detected_mode"],
        "query_text": trow["content"],
        "token_key": trow["token_key"]
    }
    if not principal_can_view_task(principal, task_meta):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Access denied")

    search_meta = {
        "query_type": task_meta.get("detected_mode"),
        "query_text": task_meta.get("query_text"),
        "scope": task_meta.get("requested_db_scope")
    }

    return {
        "job_id": job_id,
        "total": total,
        "page": page,
        "page_size": page_size,
        "search_meta": search_meta,
        "results": page_results
    }