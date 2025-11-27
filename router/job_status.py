from fastapi import Depends, HTTPException, status

from auth import get_principal, Principal
from config import SLURM_USER
from router import router
from utils.database import fetchrow
from utils.slurm import get_slurm_queue_position, query_slurm_job_state


@router.get("/api/v1/search/job/{task_id}/status")
async def get_job_status(task_id: str, principal: Principal = Depends(get_principal)):
    trow = await fetchrow(
        "SELECT id, owner, token_key, requested_db_scope, detected_mode, content, status, error, slurm_job_id "
        "FROM tasks WHERE id = $1",
        task_id
    )
    if not trow:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Not Found")

    # 权限校验：principal.token_key 或 owner 匹配
    token_key = trow.get("token_key") or ""
    owner = trow.get("owner")
    if not (principal.token_key == token_key or principal.owner == owner):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Access denied")

    # 基本字段
    job_id = trow.get("id")
    db_status = (trow.get("status") or "PENDING").upper()
    error = trow.get("error") or None
    detected_mode = trow.get("detected_mode")
    query_text = trow.get("content")
    db_scope_used = trow.get("requested_db_scope") or []

    slurm_job_id = trow.get("slurm_job_id")

    # 如果存在 slurm_job_id 且状态为 PENDING 或 RUNNING，查询 Slurm 获取最新状态并映射到接口枚举
    if slurm_job_id and db_status in ("PENDING", "RUNNING", "CREATING"):
        slurm_state = query_slurm_job_state(str(slurm_job_id))
        if slurm_state == "PENDING":
            mapped = "PENDING"
        elif slurm_state == "RUNNING":
            mapped = "RUNNING"
        elif slurm_state == "COMPLETED":
            mapped = "DONE"
            # 若 DB 还没改，我们可以尝试把它改成 DONE（可选），但这里不写 DB 更新，仅返回 DONE
        elif slurm_state == "FAILED":
            mapped = "FAILED"
        else:
            mapped = db_status

        # override显示状态（但不修改 DB）
        status_to_return = mapped

        # queue position 仅在 PENDING 时有效（其他状态返回 0）
        if status_to_return == "PENDING":
            queue_position = get_slurm_queue_position(str(slurm_job_id), SLURM_USER)
            if queue_position is None or queue_position < 0:
                queue_position = 0
        else:
            queue_position = 0

    else:
        # 如果无 slurm_job_id 或者在 DB 中已标记为 DONE/FAILED，直接用 DB 状态映射
        if db_status == "DONE":
            status_to_return = "DONE"
        elif db_status == "FAILED":
            status_to_return = "FAILED"
        elif db_status == "RUNNING":
            status_to_return = "RUNNING"
        else:
            status_to_return = "PENDING"
        queue_position = 0

    # 构造 search_meta（当 detected_mode 可用时返回，否则 null）
    search_meta = None
    if detected_mode:
        search_meta = {
            "query_type": detected_mode,
            "query_text": query_text,
            "scope": db_scope_used
        }

    # 最终响应严格按照文档字段名
    resp = {
        "job_id": job_id,
        "status": status_to_return,
        "queue_position": int(queue_position),
        "search_meta": search_meta,
        "error": error
    }

    return resp
