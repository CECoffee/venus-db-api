import subprocess
from typing import Optional


def submit_slurm_job(script_path: str) -> Optional[str]:
    try:
        r = subprocess.run(["sbatch", script_path], capture_output=True, text=True, check=True)
        out = r.stdout.strip()
        # Expect "Submitted batch job 12345"
        job_id = out.split()[-1]
        return job_id
    except subprocess.CalledProcessError as e:
        # 失败则返回 None（调用方应标记任务 failed）
        return None

def get_slurm_queue_position(slurm_job_id: str, username: str) -> int:
    """
    返回 0-based 的前方任务数（PENDING）或 -1（未知）。
    """
    try:
        r = subprocess.run(["squeue", "-u", username, "-h", "-o", "%i %T"], capture_output=True, text=True, check=True)
        lines = [ln.strip() for ln in r.stdout.splitlines() if ln.strip()]
        jobids = []
        for ln in lines:
            parts = ln.split()
            if len(parts) >= 2:
                jid, state = parts[0], parts[1]
                # 只计算 PENDING 排队的作业（包含 CONFIGURING）
                if state.upper() in ("PENDING", "CONFIGURING"):
                    jobids.append(jid)
        if slurm_job_id in jobids:
            return jobids.index(slurm_job_id)
        return -1
    except Exception:
        return -1

def query_slurm_job_state(slurm_job_id: str) -> Optional[str]:
    """
    返回 'PENDING' / 'RUNNING' / 'COMPLETED' / 'FAILED' / None(未知)
    """
    try:
        r = subprocess.run(["squeue", "-j", slurm_job_id, "-h", "-o", "%T"], capture_output=True, text=True, check=True)
        s = r.stdout.strip()
        if s:
            st = s.split()[0].upper()
            if st == "RUNNING":
                return "RUNNING"
            return "PENDING"
    except subprocess.CalledProcessError:
        pass

    # sacct fallback
    try:
        r = subprocess.run(["sacct", "-j", f"{slurm_job_id}", "-n", "-o", "State", "--parsable2"], capture_output=True, text=True, check=True)
        lines = [l for l in r.stdout.splitlines() if l.strip()]
        if not lines:
            return None
        state_raw = lines[0].split("|")[0] if '|' in lines[0] else lines[0].strip()
        state = state_raw.upper().split()[0]
        if state.startswith("COMPLETED"):
            return "COMPLETED"
        if state.startswith("FAILED") or state.startswith("NODE_FAIL") or state.startswith("CANCELLED") or state.startswith("TIMEOUT"):
            return "FAILED"
        if state.startswith("RUNNING"):
            return "RUNNING"
        if state.startswith("PENDING") or state.startswith("CONFIGURING"):
            return "PENDING"
        return state
    except Exception:
        return None