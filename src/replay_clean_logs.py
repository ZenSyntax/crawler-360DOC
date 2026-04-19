"""日志回放工具：重试清洗失败资源并对可恢复文章执行一次复洗。"""

from __future__ import annotations

import argparse
import importlib.util
import os
import time
from pathlib import Path

import requests


BASE = "http://www.360doc.com"
BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
)


def load_processer(repo_root: Path):
    fp = repo_root / "src" / "library-processer.py"
    spec = importlib.util.spec_from_file_location("library_processer", fp)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load module: {fp}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def login_if_available(session: requests.Session) -> bool:
    user = os.environ.get("DOC360_USER", "").strip()
    password = os.environ.get("DOC360_PASS", "")
    if not user or not password:
        return False
    try:
        resp = session.post(
            f"{BASE}/login.ashx",
            data={"name": user, "pass": password, "remember": "1"},
            timeout=20,
        )
        _ = resp.status_code
        session.post(
            f"{BASE}/ajax/LoginAlertHandler.ashx?timespan={int(time.time() * 1000)}",
            data={"islogined": "1"},
            timeout=20,
        )
        session.get(BASE + "/", timeout=20)
        return True
    except Exception:
        return False


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Replay clean/not-found logs, retry resource pulls, "
            "and reclean recoverable articles once."
        )
    )
    parser.add_argument(
        "--root",
        default="output-space/my-category",
        help="clean output root (default: output-space/my-category)",
    )
    parser.add_argument(
        "--no-login",
        action="store_true",
        help="do not auto-login via DOC360_USER / DOC360_PASS",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    repo_root = Path(__file__).resolve().parents[1]
    root = Path(args.root).expanduser()
    if not root.is_absolute():
        root = (repo_root / root).resolve()

    proc = load_processer(repo_root)
    logs_dir = repo_root / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    proc.set_clean_error_url_file(logs_dir / "clean_error_url.txt")
    proc.set_clean_article_error_file(logs_dir / "clean_article_error.txt")
    proc.set_resources_not_found_warning_file(
        logs_dir / "resources_not_found_warning.txt"
    )
    proc.set_processer_loggers(
        lambda m: print(f"[INFO] {m}"),
        lambda m: print(f"[WARN] {m}"),
    )

    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": BROWSER_UA,
            "Accept": "*/*",
            "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        }
    )
    if not args.no_login:
        ok = login_if_available(session)
        print(f"[INFO] auto login: {'ok' if ok else 'skipped/failed'}")

    stats = proc.replay_resource_failures_from_logs(root, session)
    print(
        "[INFO] replay done: "
        f"entries={stats.get('entries_total', 0)} "
        f"recoverable={stats.get('entries_recoverable', 0)} "
        f"recleaned_articles={stats.get('articles_recleaned', 0)} "
        f"removed_lines={stats.get('lines_removed', 0)}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
