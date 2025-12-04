# file: job_submit.py
import json
import os
import shlex
import time
import uuid
from typing import List

from fastapi import APIRouter, Depends, HTTPException, status

from auth import get_principal, Principal, check_db_scope_permission
from config import DEFAULT_DB_SCOPE, SLURM_PARTITION, TASK_WORKDIR_BASE, SLURM_USER
from router import router
from schemas import SearchRequest, JobResponse
from utils.content_proceed import detect_input_mode, is_amino_acid_sequence
from utils.database import execute
from utils.scope_proceed import normalize_scopes
from utils.slurm import submit_slurm_job, get_slurm_queue_position

def _safe_path_for_task(task_id: str) -> str:
    base = TASK_WORKDIR_BASE or "/tmp/tasks"
    task_dir = os.path.join(base, task_id)
    os.makedirs(task_dir, exist_ok=True)
    return task_dir

def _build_blastp_command(db_scope: List[str], task_dir: str) -> List[str]:
    # 为每个 db 生成一段命令：运行 blastp 输出 tabular，然后在每行前面加上 source_db，并追加到 combined_out
    # 使用 shell 的 awk/printf 来为每行添加 source_db 前缀，避免 CSV 复杂转义问题（outfmt 6 用 tab 分隔）
    blastp_cmds = []
    query_path = os.path.join(task_dir, "query.fasta")
    combined_out = os.path.join(task_dir, "combined_out.fasta")
    for db in db_scope:
        # outfmt 6 columns: sacc stitle bitscore pident evalue
        # We will add the source_db as the first column using awk
        cmd = (
            f"blastp -query {shlex.quote(query_path)} "
            f"-db {shlex.quote(db)} "
            f"-outfmt \"6 sacc stitle bitscore pident evalue\""
            # write to stdout and pipe into awk to prefix db then append to combined_out
            f" | awk -v DB={shlex.quote(db)} '{{print DB\"\\t\"$0}}' >> {shlex.quote(combined_out)}"
        )
        blastp_cmds.append(cmd)
    return blastp_cmds

@router.post("/api/v1/search/job/submit", response_model=JobResponse)
async def submit_search_job(req: SearchRequest, principal: Principal = Depends(get_principal)):
    # 解析 db_scope
    db_scope = await normalize_scopes(req.db_scope or DEFAULT_DB_SCOPE)
    if not db_scope:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="No valid db scope")
    ok, bad_scope = check_db_scope_permission(principal, db_scope)
    if not ok:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=f"Access denied: {bad_scope}")

    input_mode = (req.input_mode or "AUTO").upper()
    resolved_mode = input_mode
    if input_mode == "AUTO":
        resolved_mode = detect_input_mode(req.content)

    if resolved_mode != "SEQUENCE" or not is_amino_acid_sequence(req.content): # TODO 支持其他检索类型
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid sequence format")

    # 生成 task_id
    task_id = f"job_{uuid.uuid4().hex}"
    created = int(time.time())
    owner = principal.owner
    token_key = principal.token_key or ""

    # 插入 tasks 表：初始状态为 CREATING
    insert_sql = """
    INSERT INTO tasks (id, created_at, owner, token_key, content, input_mode,
                       detected_mode, requested_db_scope, filters, status, slurm_job_id)
    VALUES ($1, to_timestamp($2), $3, $4, $5, $6, $7, $8, $9, $10, $11)
    """
    await execute(insert_sql, task_id, created, owner, token_key, req.content, input_mode,
                  resolved_mode, db_scope, json.dumps(req.filters or {}), "CREATING", None)

    # 准备工作目录与 query 文件 (query.fasta / 等)
    task_dir = _safe_path_for_task(task_id)
    query_path = os.path.join(task_dir, "query.fasta")
    if resolved_mode == "SEQUENCE":
        header = f">{task_id}"
        with open(query_path, "w", encoding="utf-8") as fq:
            fq.write(f"{header}\n")
            fq.write(req.content.strip() + "\n")
    else:
        with open(query_path, "w", encoding="utf-8") as fq:
            fq.write(req.content)

    # 在工作目录写入 process_fasta.py（见 job_submit 附带的脚本内容）
    process_py_path = os.path.join(task_dir, "process_fasta.py")
    from pathlib import Path
    template_process_src = Path.cwd() / "templates" / "process_fasta.py"
    if template_process_src.exists():
        import shutil
        shutil.copyfile(template_process_src, process_py_path)
    os.chmod(process_py_path, 0o750)

    blastp_cmds = _build_blastp_command(db_scope, task_dir)
    combined_out = os.path.join(task_dir, "combined_out.fasta")
    # 写 slurm 脚本并提交
    script_path = os.path.join(task_dir, "run_blastp.sh")
    with open(script_path, "w", encoding="utf-8") as fh:
        fh.write("#!/bin/bash\n")
        fh.write(f"#SBATCH --job-name=job_{task_id}\n")
        if SLURM_PARTITION:
            fh.write(f"#SBATCH --partition={SLURM_PARTITION}\n")
        fh.write(f"#SBATCH --output={os.path.join(task_dir, 'slurm-%j.out')}\n")
        fh.write(f"#SBATCH --error={os.path.join(task_dir, 'slurm-%j.err')}\n")
        fh.write("set -euo pipefail\n")
        fh.write("export BLASTDB=/mnt/vdb/blast-workspace\n")
        fh.write(f"cd {shlex.quote(task_dir)}\n")
        fh.write("echo \"[task] start at $(date)\"\n")
        fh.write(
            f"printf \"source_db\\tsacc\\tstitle\\tbitscore\\tpident\\tevalue\\n\" > {shlex.quote(combined_out)}\n")
        for cmd in blastp_cmds:
            fh.write(f"echo 'RUN: {cmd}'\n")
            fh.write(cmd + "\n")
        fh.write("source /home/tanyang/miniconda3/etc/profile.d/conda.sh\n")
        fh.write("conda activate dbApi\n")
        fh.write(
            f"python3 {shlex.quote(process_py_path)} --input {shlex.quote(combined_out)} --task {shlex.quote(task_id)}\n")
    os.chmod(script_path, 0o750)
    slurm_job_id = submit_slurm_job(script_path)
    if slurm_job_id is None:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to submit job to Slurm")

    # 更新任务表 slurm_job_id & 标记为 PENDING
    await execute("UPDATE tasks SET status=$1, slurm_job_id=$2 WHERE id=$3", "PENDING", slurm_job_id, task_id)

    # 计算队列位置（若失败返回 -1）
    queue_position = get_slurm_queue_position(slurm_job_id, SLURM_USER)

    return JobResponse(task_id=task_id, status="PENDING", queue_position=queue_position)
