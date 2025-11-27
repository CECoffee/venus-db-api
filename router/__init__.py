from fastapi import APIRouter
import sys

router = APIRouter()

_submodules = [
    "job_submit",
    "job_status",
    "job_results",
    "job_delete",
    "meta",
]

# 以包相对方式导入 app.router.<mod>
for m in _submodules:
    modname = f"{__package__}.{m}"  # yields "app.router.job_submit" when package is "app.router"
    try:
        __import__(modname)
    except Exception as e:
        # 打印错误到 stderr，uvicorn 日志会显示，方便排查子模块导入错误
        print(f"Failed importing router submodule {modname}: {e}", file=sys.stderr)
        raise
