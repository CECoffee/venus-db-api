from fastapi import APIRouter
import sys

router = APIRouter()

# 导入所有子路由模块以便它们在模块导入阶段注册到上面的 router。
# 如果你的子模块文件名不同，按实际文件名修改下面列表。
_submodules = [
    "job_submit",
    "job_status",
    "job_results",
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
