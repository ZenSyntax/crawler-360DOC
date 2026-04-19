"""路径工具：解析仓库根目录、``src/`` 脚本目录与 ``output-space/`` 子路径。

支持按需加载仓库根 ``.env``（若安装 python-dotenv，且不覆盖已存在环境变量）。
"""
from __future__ import annotations

import sys
from functools import lru_cache
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


@lru_cache(maxsize=1)
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


@lru_cache(maxsize=1)
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
