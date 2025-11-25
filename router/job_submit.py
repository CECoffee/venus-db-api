import time
import uuid

from . import router
from fastapi import Depends, HTTPException, status

from config import DEFAULT_DB_SCOPE
from utils.scope_proceed import normalize_scopes
from auth import get_principal, Principal, check_db_scope_permission
from queue_redis import push_job_to_queue, get_queue_position
from schemas import SearchRequest, JobResponse
from utils.content_proceed import detect_input_mode, validate_sequence_chars


@router.post("/api/v1/search/job/submit", response_model=JobResponse)
async def submit_search_job(req: SearchRequest, principal: Principal = Depends(get_principal)):
    # normalize db_scope
    db_scope = await normalize_scopes((req.db_scope or DEFAULT_DB_SCOPE))

    # permission check
    ok, bad_scope = check_db_scope_permission(principal, db_scope)
    if not ok:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Access denied to private DB")

    # resolve input mode and detection
    input_mode = (req.input_mode or "AUTO").upper()
    resolved_mode = input_mode
    if input_mode == "AUTO":
        resolved_mode = detect_input_mode(req.content)

    # if forced sequence, validate characters
    if resolved_mode == "SEQUENCE":
        if not validate_sequence_chars(req.content):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid sequence format")

    # build job
    task_id = f"job_{uuid.uuid4().hex}"
    created = time.time()
    job = {
        "task_id": task_id,
        "created_at": created,
        "owner": principal.owner,
        "token_key": principal.token_key,
        "content": req.content,
        "input_mode": input_mode,
        "detected_mode": resolved_mode,   # important: intent feedback available immediately
        "requested_db_scope": db_scope,
        "filters": req.filters or {},
        "status": "PENDING",
        "progress": 0,
        "error": "",
    }

    # push to redis queue and store metadata
    await push_job_to_queue(job)

    pos = await get_queue_position(task_id)
    if pos is None:
        queue_position = -1
    else:
        queue_position = pos  # 0-based -> directly returns number of jobs before it

    return JobResponse(task_id=task_id, status="PENDING", queue_position=queue_position)
