"""从仓库根转发执行 ``src/wc-essay.py``；完整参数与行为见 README「wc-essay」。"""
from __future__ import annotations

import runpy
import sys
from pathlib import Path

_SCRIPT = Path(__file__).resolve().parent / "src" / "wc-essay.py"
if not _SCRIPT.is_file():
    print(f"[ERROR] 未找到 {_SCRIPT}", file=sys.stderr)
    raise SystemExit(2)
runpy.run_path(str(_SCRIPT), run_name="__main__")
