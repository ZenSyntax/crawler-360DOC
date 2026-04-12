"""仓库根路径、output-space 子路径与 src 脚本目录解析（本文件位于仓库根下的 ``src/``）。

Python 字节码默认写入各脚本同级的 __pycache__（多为 ``src/__pycache__``）。
默认数据目录：仓库根 ``output-space/``（如 ``my-essay``、``my-category``），由 ``output_space_path()`` 拼接。
约定可执行入口位于仓库根 ``src/``；``ensure_this_file_in_script_dir`` 将该目录插入 ``sys.path`` 并校验调用方路径。
仓库根 ``.env`` 可由 python-dotenv 加载；已存在于进程环境中的变量不被覆盖。
"""
from __future__ import annotations

import sys
from pathlib import Path

OUTPUT_SPACE_DIRNAME = "output-space"

_DOTENV_LOADED = False


def _load_dotenv_from_repo_once() -> None:
    global _DOTENV_LOADED
    if _DOTENV_LOADED:
        return
    _DOTENV_LOADED = True
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    load_dotenv(repo_root() / ".env")


def repo_root() -> Path:
    start = Path(__file__).resolve().parent
    for anc in [start, *start.parents]:
        if (anc / ".env").is_file() or (anc / ".git").is_dir():
            return anc
    return start.parent


def output_space_path(*parts: str) -> Path:
    # 仓库根下 output-space 及其子路径（是否 mkdir 由调用方决定）。
    p = repo_root() / OUTPUT_SPACE_DIRNAME
    for part in parts:
        p /= part
    return p


def script_dir() -> Path:
    _load_dotenv_from_repo_once()
    return (repo_root() / "src").resolve()


def ensure_this_file_in_script_dir(this_file: str) -> tuple[Path, Path]:
    sd = script_dir()
    here = Path(this_file).resolve().parent
    if sd != here:
        raise RuntimeError(
            f"路径约定：入口脚本应在仓库根 src/ 下。当前 {here}，期望 {sd}。"
        )
    if str(sd) not in sys.path:
        sys.path.insert(0, str(sd))
    return repo_root(), sd
