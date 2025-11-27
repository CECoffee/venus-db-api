from fastapi import HTTPException
from fastapi.params import Depends
from starlette import status

from auth import Principal, get_principal
from router import router
from utils.database import fetchrow, execute


@router.delete("/api/v1/search/job/{job_id}")
async def delete_job(job_id: str, principal: Principal = Depends(get_principal)):
    trow = await fetchrow(
        "SELECT id, owner, token_key, requested_db_scope, detected_mode, content, status, error, slurm_job_id "
        "FROM tasks WHERE id = $1",
        job_id,
    )
    if not trow:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Not Found")

    # 权限校验：principal.token_key 或 owner 匹配
    token_key = trow.get("token_key") or ""
    owner = trow.get("owner")
    if not (principal.token_key == token_key or principal.owner == owner):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Access denied")

    await execute(
        "DELETE FROM results WHERE task_id = $1",
        job_id,
    )
    await execute(
        "DELETE FROM tasks WHERE id = $1",
        job_id,
    )

    return