# app/auth.py
from typing import Optional, List

from fastapi import Header, HTTPException, status
from pydantic import BaseModel

from config import DEFAULT_DB_SCOPE
from utils.database import fetchrow, fetch
from utils.scope_proceed import normalize_scopes


class Principal(BaseModel):
    owner: str                # owner/platform identifier (e.g., "researcher_a" or "platform_xyz")
    # kind: str              # 'api_key' or 'jwt'
    scopes: List[str]      # list of db ids or group ids allowed by THIS token
    token_id: Optional[int] = None    # id from api_keys table if applicable
    token_key: Optional[str] = None   # raw key string (avoid storing/logging in prod)

# Attempt to decode value as JWT. If success -> return Principal(kind='jwt')
# def try_decode_jwt(value: str) -> Optional[Principal]:
#     try:
#         payload = jwt.decode(value, JWT_SECRET, algorithms=[JWT_ALGORITHM])
#         sub = payload.get("sub")
#         if not sub:
#             return None
#         scopes = payload.get("scopes") or ["group:public"]
#         return Principal(owner=sub, kind="jwt", scopes=scopes, token_id=None, token_key=value)
#     except JWTError:
#         return None

# helper permission check (very simple)
def check_db_scope_permission(principal: Principal, requested_scopes):
    for s in requested_scopes:
        if s not in principal.scopes:
            return False, s
    return True, None

# Lookup API key in Postgres and return Principal(kind='api_key') with token-specific scopes
async def verify_api_key_from_db(key: str) -> Optional[Principal]:
    # Query api_keys table
    row = await fetchrow("SELECT id, owner, is_active FROM api_keys WHERE key = $1", key)
    if not row:
        return None
    if not row["is_active"]:
        return None
    api_key_id = row["id"]
    owner = row["owner"]
    # fetch permissions (token_db_permissions)
    rows = await fetch("SELECT db_id FROM token_db_permissions WHERE api_key_id = $1", api_key_id)
    scopes = await normalize_scopes([r["db_id"] for r in rows] + DEFAULT_DB_SCOPE)
    return Principal(owner=owner, scopes=scopes, token_id=api_key_id, token_key=key)

# Main dependency for routes
async def get_principal(
    # authorization: Optional[str] = Header(None),
    x_api_key: Optional[str] = Header(None),
):
    """
    Resolve caller principal. Priority:
      1) If Authorization present:
         a) try decode JWT -> principal(kind='jwt')
         b) else treat value as raw token -> lookup in api_keys (principal(kind='api_key'))
      2) else if X-API-KEY present -> lookup in api_keys
      3) else 401
    Note: This enables each platform to have multiple tokens (rows in api_keys), and the scopes
    are token-specific (from token_db_permissions).
    """
    # 1) Authorization (Bearer)
    # if authorization:
    #     if not authorization.lower().startswith("bearer "):
    #         raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Authentication failed")
    #     token_val = authorization.split(" ", 1)[1].strip()
    #     # try JWT decode first
    #     princ = try_decode_jwt(token_val)
    #     if princ:
    #         return princ
    #     # else, treat as raw api key and lookup in db
    #     princ = await verify_api_key_from_db(token_val)
    #     if princ:
    #         return princ
    #     raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Authentication failed")

    # 2) X-API-KEY fallback
    if x_api_key:
        princ = await verify_api_key_from_db(x_api_key)
        if princ:
            return princ
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Authentication failed")

    # 3) neither present
    raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Authentication failed")
