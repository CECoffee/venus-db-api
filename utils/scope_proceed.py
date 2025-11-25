from typing import List

from utils.database import fetch


async def normalize_scopes(items: List[str]) -> List[str]:
    """
    解析输入的资源项和 group:xxx，返回所有资源库 id 的去重列表（数据库存在的）
    使用你已有的 asyncpg fetch() 工具函数。
    """
    if not items:
        return []

    groups = []
    explicit_names = []
    seen = set()

    # 解析输入
    for item in items:
        if not isinstance(item, str):
            continue
        it = item.strip()
        if it.lower().startswith("group:"):
            groups.append(it)
        else:
            if it not in seen:
                explicit_names.append(it)
                seen.add(it)

    results_set = set()
    ordered_result = []

    # --- 1) 查询显式资源名 ---
    if explicit_names:
        # 使用 ANY($1::text[]) 查询
        rows = await fetch(
            "SELECT id FROM databases WHERE id = ANY($1::text[])",
            explicit_names,
        )
        valid_ids = {r["id"] for r in rows}

        # 按输入顺序加入
        for name in explicit_names:
            if name in valid_ids and name not in results_set:
                ordered_result.append(name)
                results_set.add(name)

    # --- 2) 查询组内资源 ---
    if groups:
        rows = await  fetch(
            "SELECT id FROM databases WHERE group_id = ANY($1::text[])",
            groups,
        )
        group_ids = sorted({r["id"] for r in rows})  # 给组内容一个稳定顺序

        for rid in group_ids:
            if rid not in results_set:
                ordered_result.append(rid)
                results_set.add(rid)

    return ordered_result