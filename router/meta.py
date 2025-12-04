from typing import Optional, List, Dict, Any

from fastapi import Depends, Header

from auth import get_principal, Principal
from config import DEFAULT_DB_SCOPE, LANGUAGE_CODES
from utils.database import fetch
from utils.scope_proceed import normalize_scopes
from . import router


# Helper: load database groups and databases from Postgres
async def load_database_groups() -> List[Dict[str, Any]]:
    rows = await fetch("SELECT id, label, type FROM database_groups ORDER BY id")
    return [{"id": r["id"], "label": r["label"], "type": r["type"]} for r in rows]

async def load_databases(accept_language: str) -> List[Dict[str, Any]]:
    label = f"label_{accept_language}"
    sql = f"""
        SELECT id, label_en_us, {label}, group_id, source_type, disabled
        FROM databases
        ORDER BY id
        """
    rows = await fetch(sql)
    res = []
    sql = f"""
        SELECT key, label_en_us, {label}, unit FROM db_filter_fields WHERE db_id = $1
        """
    for r in rows:
        # load filter_fields for each db
        frows = await fetch(sql, r["id"])
        filter_fields = []
        for f in frows:
            filter_fields.append({
                "key": f["key"],
                "label": f[f"{label}"] or f["label_en_us"],
                "unit": f["unit"]
            })
        item = {
            "id": r["id"],
            "label": r[f"{label}"] or r["label_en_us"],
            "group_id": r["group_id"],
            "source_type": r["source_type"],
            "disabled": r["disabled"] or False,
        }
        if filter_fields:
            item["filter_fields"] = filter_fields
        res.append(item)
    return res

@router.get("/api/v1/meta/config")
async def meta_config(
    accept_language: Optional[str] = Header(None),
    principal: Principal = Depends(get_principal)
):
    """
    Return default_scope, database_groups and databases filtered by principal scopes.
    If a database is private and the principal lacks permission, it will be excluded.
    """
    # parse language
    if accept_language:
        accept_language = accept_language.lower()
    else:
        accept_language = "en_us"
    if not accept_language in LANGUAGE_CODES:
        accept_language = "en_us"

    # load all groups and dbs
    groups = await load_database_groups()
    dbs = await load_databases(accept_language)

    allowed_scopes = normalize_scopes(principal.scopes + DEFAULT_DB_SCOPE)

    # Build filtered db list: keep db if its group_id is public OR principal has explicit db_id
    filtered = []
    for db in dbs:
        group_id = db.get("group_id")
        if group_id == "group:public":
            filtered.append(db)
            continue
        # private group: check if principal has permission to this db id or group
        if db["id"] in allowed_scopes:
            filtered.append(db)
            continue
        # Otherwise, hide the db (do not include)
        # Alternative: include but mark as disabled. Here we choose to exclude for clarity.
        # If you prefer to include but mark disabled, set 'disabled': True and push minimal info.
        # Example:
        db_copy = db.copy()
        db_copy["disabled"] = True
        filtered.append(db_copy)
    # Determine default_scope from config (DEFAULT_DB_SCOPE)
    default_scope = DEFAULT_DB_SCOPE

    return {
        "default_scope": default_scope,
        "database_groups": groups,
        "databases": filtered
    }
