from fastapi import HTTPException, Depends
from starlette import status

from auth import Principal, get_principal, check_db_scope_permission
from router import router
from utils.database import fetchrow
from utils.scope_proceed import normalize_scopes


@router.get("/api/v1/data/{db_id}/{accession}")
async def get_entry(
    db_id: str,
    accession: str,
    principal: Principal = Depends(get_principal)
):
    db_scope = await normalize_scopes([db_id])
    if not db_scope:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Not found: {db_id}")
    ok, bad_scope = check_db_scope_permission(principal, [db_id])
    if not ok:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=f"Access denied: {bad_scope}")

    result = await fetchrow(f"SELECT * FROM {db_id} WHERE accession = $1", accession)

    if result is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Not found: {accession}")

    row_dict = dict(result)

    attributes = {
        k: v for k, v in row_dict.items()
        if k not in ("accession", "sequence", "external_url") and v is not None
    }

    return {
        "accession": row_dict.get("accession"),
        "sequence": row_dict.get("sequence"),
        "external_url": row_dict.get("external_url"),
        "attributes": attributes,
    }
