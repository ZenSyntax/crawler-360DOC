"""仓库根入口：转发执行 ``src/wc-essay.py``。参数与行为见 README「wc-essay」。"""
from __future__ import annotations

import runpy
import sys
from pathlib import Path

_REPO = Path(__file__).resolve().parent
_SCRIPT = _REPO / "src" / "wc-essay.py"
_SRC = _REPO / "src"
if not _SCRIPT.is_file():
    print(f"[ERROR] 未找到 {_SCRIPT}", file=sys.stderr)
    raise SystemExit(2)
# 保证与 src 同级的 _site_paths 等可被导入（不依赖 run_path 对 sys.path 的实现细节）
_src = str(_SRC)
if _src not in sys.path:
    sys.path.insert(0, _src)
runpy.run_path(str(_SCRIPT), run_name="__main__")
