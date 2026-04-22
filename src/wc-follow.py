"""360doc 关注用户抓取入口：抓取我关注用户的分类文章，并可选清洗/转 Word。"""

from __future__ import annotations

from _site_paths import ensure_this_file_in_script_dir, output_space_path

_REPO_ROOT, _ = ensure_this_file_in_script_dir(__file__)

import argparse
import hashlib
import importlib.util
import os
import random
import re
import sys
import time
import traceback
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse

import requests


def _load_wc_library_core():
    path = Path(__file__).resolve().parent / "wc-library.py"
    spec = importlib.util.spec_from_file_location("_wc_library_core", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"无法加载 wc-library 核心模块: {path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


CORE = _load_wc_library_core()

BASE = CORE.BASE
FOLLOW_USERS_API = f"{BASE}/ajax/HomeIndex/getgzusers.ashx"
FOLLOW_USER_CATEGORY_API = f"{BASE}/ajax/getmyCategory.ashx"
FOLLOW_ARTICLE_LIST_API = "http://api.360doc.com/ajax/ArticleHandler.ashx"
MYFILES_REFERER = f"{BASE}/myfiles.aspx"

FOLLOW_USER_PAGE_SIZE = 50
FOLLOW_ART_PAGE_SIZE = 10
MAX_USER_DIR_STEM = 120
MAX_CATEGORY_DIR_STEM = 120

FOLLOW_ERROR_URL_FILE = Path("logs/follow_error_url.txt")
FOLLOW_NOT_FOUND_WARNING_FILE = Path("logs/follow_not_found_warning.txt")

_FOLLOW_PACING_SEC: tuple[float, float] = (
    CORE.REQUEST_PACING_DEFAULT_MIN_MS / 1000.0,
    CORE.REQUEST_PACING_DEFAULT_MAX_MS / 1000.0,
)

RateLimitError = CORE.RateLimitError
ArticleNotFoundError = CORE.ArticleNotFoundError


def log_info(message: str) -> None:
    CORE.log_info(message)


def log_warn(message: str) -> None:
    CORE.log_warn(message)


def log_error(message: str) -> None:
    CORE.log_error(message)


def _follow_request_pacing_sleep() -> None:
    time.sleep(random.uniform(*_FOLLOW_PACING_SEC))


def append_follow_error_line(line: str) -> None:
    FOLLOW_ERROR_URL_FILE.parent.mkdir(parents=True, exist_ok=True)
    with FOLLOW_ERROR_URL_FILE.open("a", encoding="utf-8") as f:
        f.write(line.rstrip("\n") + "\n")


def append_follow_not_found_warning_line(line: str) -> None:
    FOLLOW_NOT_FOUND_WARNING_FILE.parent.mkdir(parents=True, exist_ok=True)
    with FOLLOW_NOT_FOUND_WARNING_FILE.open("a", encoding="utf-8") as f:
        f.write(line.rstrip("\n") + "\n")


def _parse_csv_tokens(raw: str | None) -> list[str]:
    if not raw or not str(raw).strip():
        return []
    items = [t.strip() for t in re.split(r"[,\n;]+", str(raw)) if t.strip()]
    out: list[str] = []
    seen: set[str] = set()
    for item in items:
        if item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _parse_user_id_filter(raw: str | None) -> set[str]:
    out: set[str] = set()
    for token in _parse_csv_tokens(raw):
        if not re.fullmatch(r"\d+", token):
            raise ValueError(f"--user-id 包含非数字值: {token!r}")
        out.add(token)
    return out


def _normalize_user_name(raw_name: str, uid: str) -> tuple[str, str]:
    name = CORE.decode_text(raw_name.strip()) if raw_name else ""
    name = name.strip() or uid
    safe = CORE.sanitize_name(name, f"user-{uid}")
    safe = CORE.trim_name(safe, MAX_USER_DIR_STEM)
    return name, safe


def _normalize_category_name(raw_name: str, cid: int) -> tuple[str, str]:
    name = CORE.decode_text(raw_name.strip()) if raw_name else ""
    name = name.strip() or str(cid)
    safe = CORE.sanitize_name(name, f"category-{cid}")
    safe = CORE.trim_name(safe, MAX_CATEGORY_DIR_STEM)
    return name, safe


def _decode_follow_user(item: dict) -> dict | None:
    uid = str(item.get("userid", "")).strip()
    if not re.fullmatch(r"\d+", uid):
        return None
    raw_name = str(item.get("username") or item.get("name") or "").strip()
    name, safe_name = _normalize_user_name(raw_name, uid)
    raw_group = str(item.get("groupname") or "").strip()
    group_name = CORE.decode_text(raw_group).strip() if raw_group else ""
    return {
        "id": uid,
        "name": name,
        "safe_name": safe_name,
        "group_name": group_name,
    }


def _build_follow_sign(params: dict[str, str | int]) -> str:
    # 与前端 doccgjio.xfejh 一致：
    # 1) 过滤空字符串值
    # 2) key=value 组成数组后按字典序排序
    # 3) 直接拼接（不加 &）后做 SHA1，再转大写
    pairs: list[str] = []
    for k, v in params.items():
        sv = str(v)
        if sv == "":
            continue
        pairs.append(f"{k}={sv}")
    pairs.sort()
    return hashlib.sha1("".join(pairs).encode("utf-8")).hexdigest().upper()


def fetch_followed_users_page(
    session: requests.Session, *, curnum: int, page_size: int = FOLLOW_USER_PAGE_SIZE
) -> dict:
    params = {
        "pagenum": page_size,
        "curnum": curnum,
        "classid": -1,
        "_": int(time.time() * 1000),
    }
    resp = CORE.http_get(session, FOLLOW_USERS_API, params=params)
    data = CORE.parse_json_lenient(resp.text)
    if not isinstance(data, dict):
        raise ValueError("关注用户列表接口返回非对象")
    return data


def fetch_all_followed_users(session: requests.Session) -> list[dict]:
    page = 1
    total: int | None = None
    users_by_id: dict[str, dict] = {}

    while True:
        data = fetch_followed_users_page(session, curnum=page)
        status = str(data.get("status", "")).strip()
        if status != "1":
            raise ValueError(f"关注用户接口状态异常: status={status!r}")

        if total is None:
            raw_total = str(data.get("gzusernum") or "").strip()
            if raw_total.isdigit():
                total = int(raw_total)

        arr = data.get("gzuser") or []
        if not isinstance(arr, list) or not arr:
            break

        for item in arr:
            if not isinstance(item, dict):
                continue
            one = _decode_follow_user(item)
            if one is None:
                continue
            users_by_id.setdefault(one["id"], one)

        if total is not None and len(users_by_id) >= total:
            break
        if len(arr) < FOLLOW_USER_PAGE_SIZE:
            break

        page += 1
        _follow_request_pacing_sleep()

    def _sort_key(u: dict) -> tuple[int, str]:
        uid = str(u["id"])
        return (int(uid), uid) if uid.isdigit() else (10**18, uid)

    return sorted(users_by_id.values(), key=_sort_key)


def fetch_follow_user_categories(session: requests.Session, user_id: str) -> list[dict]:
    params = {"type": 3, "userid": user_id, "_": int(time.time() * 1000)}
    resp = CORE.http_get(session, FOLLOW_USER_CATEGORY_API, params=params)
    data = CORE.parse_json_lenient(resp.text)
    if not isinstance(data, list):
        raise ValueError(f"关注用户分类接口返回非列表 user={user_id}")

    categories: list[dict] = []
    for item in data:
        if not isinstance(item, dict):
            continue
        raw_id = str(item.get("id", "")).strip()
        if not re.fullmatch(r"-?\d+", raw_id):
            continue
        cid = int(raw_id)
        if cid in (0, 1):
            continue
        raw_name = str(
            item.get("selftitle")
            or item.get("CategoryName")
            or item.get("name")
            or raw_id
        )
        name, safe_name = _normalize_category_name(raw_name, cid)
        categories.append(
            {
                "id": cid,
                "name": name,
                "safe_name": safe_name,
                "artnum": str(item.get("artnum") or "").strip(),
            }
        )
    categories.sort(key=lambda x: x["id"])
    return categories


def select_follow_users(
    all_users: list[dict], *, user_ids_raw: str | None, user_names_raw: str | None
) -> list[dict]:
    id_filter = _parse_user_id_filter(user_ids_raw)
    name_tokens = _parse_csv_tokens(user_names_raw)

    users = all_users
    if id_filter:
        users = [u for u in users if str(u["id"]) in id_filter]
        missing_ids = [uid for uid in sorted(id_filter) if uid not in {str(u["id"]) for u in users}]
        for uid in missing_ids:
            log_warn(f"--user-id 未在关注列表中找到: {uid}")

    if name_tokens:
        matched_ids: set[str] = set()
        for token in name_tokens:
            exact = [u for u in all_users if u["name"] == token]
            contain = [u for u in all_users if token in u["name"]]
            chosen = exact if exact else contain
            if not chosen:
                log_warn(f"--user-name 未匹配到关注用户: {token!r}")
                continue
            matched_ids.update(str(u["id"]) for u in chosen)
        users = [u for u in users if str(u["id"]) in matched_ids]

    return users


def select_follow_categories(all_categories: list[dict], category_filter_raw: str | None) -> list[dict]:
    tokens = _parse_csv_tokens(category_filter_raw)
    if not tokens:
        return all_categories

    selected: list[dict] = []
    selected_ids: set[int] = set()

    for token in tokens:
        matched: list[dict] = []
        if re.fullmatch(r"-?\d+", token):
            wanted = int(token)
            matched = [c for c in all_categories if int(c["id"]) == wanted]
        if not matched:
            matched = [c for c in all_categories if c["name"] == token]
        if not matched:
            matched = [c for c in all_categories if token in c["name"]]
        if not matched:
            log_warn(f"--c 未匹配到分类: {token!r}")
            continue
        for c in matched:
            cid = int(c["id"])
            if cid in selected_ids:
                continue
            selected_ids.add(cid)
            selected.append(c)

    selected.sort(key=lambda x: x["id"])
    return selected


def fetch_follow_category_page(
    session: requests.Session,
    *,
    user_id: str,
    category_id: int,
    curnum: int,
    article_cursor: str,
) -> dict:
    payload: dict[str, str | int] = {
        "op": "getartlistbyartid",
        "pagenum": FOLLOW_ART_PAGE_SIZE,
        "cid": category_id,
        "isoriginal": 0,
        "userid": user_id,
        "sortarttype": 1,
        "arttype": "",
        "articleid": article_cursor,
        "validate": "",
        "curnum": curnum,
    }
    sign = _build_follow_sign(payload)
    params = {
        **payload,
        "sign": sign,
        "_": int(time.time() * 1000),
    }
    headers = {
        **CORE.HEADERS,
        "Referer": MYFILES_REFERER,
        "Accept": "text/html, */*; q=0.01",
    }
    resp = CORE.http_get(session, FOLLOW_ARTICLE_LIST_API, params=params, headers=headers)
    data = CORE.parse_json_lenient(resp.text)
    if not isinstance(data, dict):
        raise ValueError("关注分类文章列表接口返回非对象")
    return data


def save_follow_article_html(
    session: requests.Session,
    article: dict,
    category_dir: Path,
    *,
    user_id: str,
    user_name: str,
    category_id: int,
    category_name: str,
    page_num: int,
    force_html: bool,
) -> bool:
    art_id = str(article.get("articleid", "")).strip()
    if not re.fullmatch(r"\d+", art_id):
        raise ValueError(f"articleid 无效（需数字）: {art_id!r}")

    raw_title = str(article.get("articletitle") or art_id or "untitled")
    title = CORE.decode_text(raw_title)
    safe_title = CORE.sanitize_name(title, art_id or "untitled")
    safe_title = CORE.trim_name(safe_title, CORE.MAX_FILE_STEM)
    file_path = category_dir / f"{art_id}-{safe_title}.html"

    if not force_html:
        local_raw_by_id = sorted(
            p
            for p in category_dir.glob(f"{art_id}-*.html")
            if p.is_file() and not p.name.lower().startswith("clean_")
        )
        if local_raw_by_id:
            log_info(
                f"user={user_id}-{user_name} cat={category_id}-{category_name} "
                f"page={page_num} skip={local_raw_by_id[0].name}"
            )
            return False
        local_clean_by_id = sorted(
            p for p in category_dir.glob(f"clean_{art_id}-*.html") if p.is_file()
        )
        if local_clean_by_id:
            log_info(
                f"user={user_id}-{user_name} cat={category_id}-{category_name} "
                f"page={page_num} skip={local_clean_by_id[0].name}"
            )
            return False

    art_url = CORE.showweb_article_url(art_id)
    try:
        html_resp = CORE.fetch_showweb_article_stream(session, art_url)
    except ArticleNotFoundError:
        append_follow_not_found_warning_line(
            f"user={user_id}-{user_name}\tcat={category_id}-{category_name}\t"
            f"{art_id}-{safe_title}-{art_url}-not_found"
        )
        log_warn(
            f"user={user_id}-{user_name} cat={category_id}-{category_name} "
            f"page={page_num} not_found={art_id}-{safe_title}.html"
        )
        return False

    html_bytes = html_resp.content
    encoding = html_resp.encoding or "utf-8"
    try:
        text_content = html_bytes.decode(encoding)
    except UnicodeDecodeError:
        text_content = html_bytes.decode("utf-8", errors="replace")

    file_path.write_text(text_content, encoding="utf-8")
    log_info(
        f"user={user_id}-{user_name} cat={category_id}-{category_name} "
        f"page={page_num} saved={file_path.name}"
    )
    return True


def crawl_one_follow_user(
    session: requests.Session,
    user: dict,
    root: Path,
    *,
    category_filter_raw: str | None,
    force_html: bool,
) -> None:
    uid = str(user["id"])
    uname = str(user["name"])
    usafe = str(user["safe_name"])
    user_dir = root / f"{uid}-{usafe}"
    user_dir.mkdir(parents=True, exist_ok=True)

    categories = fetch_follow_user_categories(session, uid)
    categories = select_follow_categories(categories, category_filter_raw)
    if not categories:
        log_warn(f"关注用户无可抓分类（可能被 --c 过滤）: {uid}-{uname}")
        return

    log_info(f"开始关注用户: {uid}-{uname}，分类数={len(categories)}")

    for cat in categories:
        cid = int(cat["id"])
        cname = str(cat["name"])
        csafe = str(cat["safe_name"])
        category_dir = user_dir / f"{cid}-{csafe}"
        category_dir.mkdir(parents=True, exist_ok=True)

        page = 1
        article_cursor = "-1"
        seen_tail_ids: set[str] = set()
        category_error_logged = False

        while True:
            data = fetch_follow_category_page(
                session,
                user_id=uid,
                category_id=cid,
                curnum=page,
                article_cursor=article_cursor,
            )
            status = str(data.get("status", "")).strip()
            if status != "1":
                log_warn(
                    f"user={uid}-{uname} cat={cid}-{cname} page={page} "
                    f"status 异常: {status!r}"
                )
                break

            items = data.get("listitem") or []
            if not isinstance(items, list) or not items:
                log_info(f"user={uid}-{uname} cat={cid}-{cname} 抓取结束 page={page}")
                break

            for art in items:
                try:
                    did_fetch = save_follow_article_html(
                        session,
                        art,
                        category_dir,
                        user_id=uid,
                        user_name=uname,
                        category_id=cid,
                        category_name=cname,
                        page_num=page,
                        force_html=force_html,
                    )
                except Exception as exc:
                    art_id = str(art.get("articleid", "unknown"))
                    art_title = CORE.decode_text(str(art.get("articletitle") or "").strip()) or "unknown"
                    art_url = str(art.get("arturl", "")).strip() or "unknown"
                    if not category_error_logged:
                        append_follow_error_line(
                            f"user={uid}-{uname}\tcat={cid}-{cname}\tpage={page}"
                        )
                        category_error_logged = True
                    append_follow_error_line(f"{art_id}-{art_title}-{art_url}-{exc}")
                    log_warn(
                        f"user={uid}-{uname} cat={cid}-{cname} page={page} "
                        f"art={art_id} err={exc}"
                    )
                    _follow_request_pacing_sleep()
                else:
                    if did_fetch:
                        _follow_request_pacing_sleep()

            tail_id = str(items[-1].get("articleid", "")).strip()
            if not tail_id:
                break
            if tail_id == article_cursor:
                log_warn(
                    f"user={uid}-{uname} cat={cid}-{cname} page={page} "
                    "cursor 未推进，停止该分类以避免死循环。"
                )
                break
            if tail_id in seen_tail_ids:
                log_warn(
                    f"user={uid}-{uname} cat={cid}-{cname} page={page} "
                    "cursor 重复，停止该分类以避免死循环。"
                )
                break
            seen_tail_ids.add(tail_id)
            article_cursor = tail_id
            page += 1
            _follow_request_pacing_sleep()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "360doc 关注用户抓取：按“关注用户 -> 分类”抓取文章 HTML，"
            "可选清洗与 Word。"
        )
    )
    parser.add_argument(
        "-d",
        "--d",
        dest="work_dir",
        default=None,
        metavar="DIR",
        help="输出根目录（相对或绝对）。省略则为 <仓库根>/output-space/my-follow。",
    )
    parser.add_argument(
        "-f",
        "--f",
        dest="force",
        action="store_true",
        help="强制覆盖已存在文章 HTML；与清洗/Word 同用时亦强制覆盖 clean_ 前缀 HTML 与 .docx。",
    )
    parser.add_argument(
        "-w",
        "--w",
        dest="gen_word",
        action="store_true",
        help="抓取后生成与 HTML 同名的 .docx（需清洗管线或已有 clean_ 前缀 HTML）。",
    )
    parser.add_argument(
        "--word-only",
        dest="word_only",
        action="store_true",
        help="仅将本地 clean_*.html 转换为 Word（不登录、不抓取；缺 clean 文件则跳过）。",
    )
    parser.add_argument(
        "--clean-only",
        dest="clean_only",
        action="store_true",
        help="仅在本地已有 HTML 上执行清洗（不登录、不抓取）。",
    )
    parser.add_argument(
        "--local-only",
        dest="local_only",
        action="store_true",
        help="仅处理本地已有数据；可配合 -c/-w/--r-c 使用。",
    )
    parser.add_argument(
        "-c",
        dest="do_clean",
        action="store_true",
        help="启用数据清洗（写入 clean_ 前缀 HTML）。",
    )
    parser.add_argument(
        "--r",
        dest="remove_original",
        action="store_true",
        help="清洗或转换成功后删除原始 HTML（仅 Word 时删除 raw 保留 clean_ 文件，除非配合 --r-c）。",
    )
    parser.add_argument(
        "--r-c",
        dest="r_clean_only",
        action="store_true",
        help="仅保留 .docx：删除 raw、clean_ HTML 与本地媒体目录（需同时使用 -w）。",
    )
    parser.add_argument(
        "--user-id",
        dest="user_id",
        default=None,
        metavar="ID",
        help="仅抓指定关注用户 ID（可逗号分隔多个）。",
    )
    parser.add_argument(
        "--user-name",
        dest="user_name",
        default=None,
        metavar="NAME",
        help="仅抓指定关注用户名（支持精确或包含匹配，可逗号分隔多个）。",
    )
    parser.add_argument(
        "--c",
        dest="follow_category",
        default=None,
        metavar="CAT",
        help="按关注用户分类过滤（分类 ID 或名称片段，可逗号分隔多个）。",
    )
    return parser.parse_args()


def _log_startup_follow_config(
    args: argparse.Namespace,
    root: Path,
    *,
    local_only: bool,
    pacing_lo_ms: int,
    pacing_hi_ms: int,
    pacing_src: str,
    n_users: int,
) -> None:
    log_info("── 本次命令行配置（已生效）──")
    if local_only:
        if args.word_only:
            log_info("模式: 仅本地 Word（--word-only），不登录、不抓取")
        elif args.clean_only:
            log_info("模式: 仅本地清洗（--clean-only），不登录、不抓取")
        else:
            log_info("模式: 仅本地处理（--local-only），不登录、不抓取")
    else:
        log_info("模式: 登录并抓取关注用户文章 HTML")
    if args.work_dir:
        log_info(f"输出根目录: {root.resolve()}（由 -d / --d 指定）")
    else:
        log_info(f"输出根目录: {root.resolve()}（默认 output-space/my-follow）")
    log_info(
        f"请求频控: 每篇文章间隔随机等待 {pacing_lo_ms}–{pacing_hi_ms} ms（{pacing_src}）"
    )
    if not local_only:
        log_info(f"参与关注用户数: {n_users}")
        if args.user_id:
            log_info(f"用户过滤 (--user-id): {args.user_id}")
        if args.user_name:
            log_info(f"用户过滤 (--user-name): {args.user_name}")
        if args.follow_category:
            log_info(f"分类过滤 (--c): {args.follow_category}")
    log_info(f"数据清洗 (-c): {'是' if args.do_clean else '否'}")
    log_info(f"Word (-w/--r-c): {'是' if (args.gen_word or args.r_clean_only) else '否'}")
    log_info(f"仅本地 Word (--word-only): {'是' if args.word_only else '否'}")
    log_info(f"仅本地清洗 (--clean-only): {'是' if args.clean_only else '否'}")
    log_info(f"本地模式总开关 (--local-only): {'是' if args.local_only else '否'}")
    log_info(f"强制覆盖 (-f): {'是' if args.force else '否'}")
    log_info(f"删除原 HTML (--r): {'是' if args.remove_original else '否'}")
    log_info(f"仅保留 Word (--r-c): {'是' if args.r_clean_only else '否'}")
    log_info("── 以上配置确认后开始执行 ──")


def _is_processer_rate_limit_error(exc: Exception) -> bool:
    return exc.__class__.__name__ in {
        "CleanRateLimitError",
        "CleanBlacklistError",
        "RateLimitError",
    }


def _is_blacklist_rate_limit_error(exc: Exception) -> bool:
    msg = str(exc or "").lower()
    return "status=403" in msg and "blacklist" in msg


def _send_clean_blacklist_alert(exc: Exception) -> None:
    CORE.send_alert_email(
        "clean-resource-http-403-blacklist",
        "360doc 抓取告警：清洗阶段触发 IP 黑名单拦截 (403)",
        (
            "stage=clean-resource\n"
            "status=403\n"
            "reason=ip-blacklist\n"
            f"exception={exc}"
        ),
        deduplicate=False,
    )


def run() -> None:
    global FOLLOW_ERROR_URL_FILE
    global FOLLOW_NOT_FOUND_WARNING_FILE
    global _FOLLOW_PACING_SEC

    args = parse_args()
    if args.word_only:
        args.gen_word = True
        args.do_clean = False
    if args.clean_only:
        args.do_clean = True
    if args.r_clean_only:
        args.gen_word = True

    local_only_mode = bool(args.local_only or args.word_only or args.clean_only)
    clean_disk = bool(args.do_clean and not args.r_clean_only)

    root = (
        Path(args.work_dir).expanduser()
        if args.work_dir
        else output_space_path("my-follow")
    )
    root.mkdir(parents=True, exist_ok=True)
    logs_dir = _REPO_ROOT / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    FOLLOW_ERROR_URL_FILE = logs_dir / "follow_error_url.txt"
    FOLLOW_NOT_FOUND_WARNING_FILE = logs_dir / "follow_not_found_warning.txt"

    pacing_lo_ms, pacing_hi_ms, pacing_src = CORE.resolve_request_pacing_ms()
    _FOLLOW_PACING_SEC = (pacing_lo_ms / 1000.0, pacing_hi_ms / 1000.0)

    proc = CORE._load_library_processer()
    proc.set_processer_loggers(log_info, log_warn)
    proc.set_clean_error_url_file(logs_dir / "clean_error_url.txt")
    proc.set_clean_article_error_file(logs_dir / "clean_article_error.txt")
    proc.set_resources_not_found_warning_file(logs_dir / "resources_not_found_warning.txt")
    proc.set_category_artnum_map({})

    if local_only_mode:
        if not root.is_dir():
            log_error(f"目录不存在: {root}")
            sys.exit(1)
        if not clean_disk and not args.gen_word and not args.r_clean_only:
            log_error("local-only 模式下需启用清洗或 Word（-c/-w/--r-c/--clean-only/--word-only）。")
            sys.exit(1)

        _log_startup_follow_config(
            args,
            root,
            local_only=True,
            pacing_lo_ms=pacing_lo_ms,
            pacing_hi_ms=pacing_hi_ms,
            pacing_src=pacing_src,
            n_users=0,
        )

        CORE.configure_email_from_environment()
        session = requests.Session()
        session.headers.update(
            {
                "User-Agent": CORE.BROWSER_UA,
                "Accept": "*/*",
                "Accept-Language": "zh-CN,zh;q=0.9",
            }
        )

        local_pipeline_needs_network = bool(args.r_clean_only)
        user = os.environ.get("DOC360_USER", "").strip()
        password = os.environ.get("DOC360_PASS", "")
        if local_pipeline_needs_network and user and password:
            try:
                session.headers.update(CORE.HEADERS)
                CORE.login(session, user, password)
                CORE.prime_browser_context(session)
                log_info("本地模式不使用 session cookie: session cookie 将被用于加载有更严格限制的资源加载")
            except Exception as exc:
                log_warn(f"local mode auto-login failed (continue without login): {exc}")

        try:
            nf = proc.run_clean_and_word_pass(
                root,
                session,
                enable_clean=clean_disk,
                gen_word=bool(args.gen_word or args.r_clean_only),
                force_clean=args.force,
                force_docx=args.force,
                remove_original=args.remove_original,
                r_clean_only=args.r_clean_only,
                remove_raw_when_word_only=args.remove_original and not clean_disk,
                clean_article_pacing_sec=_FOLLOW_PACING_SEC,
                offline_word_only=bool(
                    local_only_mode
                    and args.gen_word
                    and (not clean_disk)
                    and (not args.r_clean_only)
                ),
            )
            if clean_disk and not args.clean_only:
                replay_stats = proc.replay_resource_failures_from_logs(root, session)
                if replay_stats.get("entries_total", 0) > 0:
                    log_info(
                        "log replay reclean: "
                        f"entries={replay_stats.get('entries_total', 0)} "
                        f"recoverable={replay_stats.get('entries_recoverable', 0)} "
                        f"recleaned_articles={replay_stats.get('articles_recleaned', 0)} "
                        f"removed_lines={replay_stats.get('lines_removed', 0)}"
                    )
        except KeyboardInterrupt:
            log_warn("收到键盘中断（KeyboardInterrupt），退出。")
            sys.exit(130)
        except Exception as exc:
            if _is_processer_rate_limit_error(exc):
                if _is_blacklist_rate_limit_error(exc):
                    _send_clean_blacklist_alert(exc)
                    log_error(f"检测到清洗 403 IP 黑名单拦截，程序退出: {exc}")
                    sys.exit(5)
                log_error(f"清洗阶段触发熔断，程序退出: {exc}")
                sys.exit(3)
            raise
        if nf == 0:
            CORE.send_task_completion_email(
                "wc-follow",
                "mode=local-only",
            )
        sys.exit(0 if nf == 0 else 1)

    user = os.environ.get("DOC360_USER", "").strip()
    password = os.environ.get("DOC360_PASS", "")
    if not user or not password:
        log_error("缺少环境变量 DOC360_USER 或 DOC360_PASS。")
        sys.exit(1)

    session = requests.Session()
    session.headers.update(CORE.HEADERS)
    try:
        CORE.login(session, user, password)
    except Exception as exc:
        CORE.send_alert_email(
            "follow-login-failed",
            "360doc 抓取告警：关注抓取登录失败",
            f"登录失败: {exc}\n\n{traceback.format_exc()}",
        )
        log_error(f"登录失败: {exc}")
        sys.exit(2)

    CORE.prime_browser_context(session)
    CORE.configure_email_from_environment()

    try:
        all_users = fetch_all_followed_users(session)
        try:
            selected_users = select_follow_users(
                all_users,
                user_ids_raw=args.user_id,
                user_names_raw=args.user_name,
            )
        except ValueError as exc:
            log_error(str(exc))
            sys.exit(1)
        if not selected_users:
            log_warn("筛选后无可处理关注用户；核对 --user-id / --user-name。")
            CORE.send_task_completion_email(
                "wc-follow",
                "mode=online\nresult=no-selected-users",
            )
            return

        _log_startup_follow_config(
            args,
            root,
            local_only=False,
            pacing_lo_ms=pacing_lo_ms,
            pacing_hi_ms=pacing_hi_ms,
            pacing_src=pacing_src,
            n_users=len(selected_users),
        )
        log_info(
            f"关注用户过滤: total={len(all_users)} selected={len(selected_users)}"
        )

        for u in selected_users:
            uid = str(u["id"])
            uname = str(u["name"])
            try:
                crawl_one_follow_user(
                    session,
                    u,
                    root,
                    category_filter_raw=args.follow_category,
                    force_html=args.force,
                )
            except Exception as exc:
                append_follow_error_line(f"user={uid}-{uname}\terr={exc}")
                log_warn(f"关注用户抓取失败 user={uid}-{uname} err={exc}")
                _follow_request_pacing_sleep()

        gen_word_effective = bool(args.gen_word or args.r_clean_only)
        if clean_disk or gen_word_effective:
            try:
                proc.run_clean_and_word_pass(
                    root,
                    session,
                    enable_clean=clean_disk,
                    gen_word=gen_word_effective,
                    force_clean=args.force,
                    force_docx=args.force,
                    remove_original=args.remove_original,
                    r_clean_only=args.r_clean_only,
                    remove_raw_when_word_only=args.remove_original and not clean_disk,
                    clean_article_pacing_sec=_FOLLOW_PACING_SEC,
                )
                if clean_disk:
                    replay_stats = proc.replay_resource_failures_from_logs(root, session)
                    if replay_stats.get("entries_total", 0) > 0:
                        log_info(
                            "log replay reclean: "
                            f"entries={replay_stats.get('entries_total', 0)} "
                            f"recoverable={replay_stats.get('entries_recoverable', 0)} "
                            f"recleaned_articles={replay_stats.get('articles_recleaned', 0)} "
                            f"removed_lines={replay_stats.get('lines_removed', 0)}"
                        )
            except Exception as exc:
                if _is_processer_rate_limit_error(exc):
                    raise RateLimitError(f"清洗阶段触发熔断: {exc}") from exc
                raise
        CORE.send_task_completion_email(
            "wc-follow",
            "mode=online",
        )
        return

    except KeyboardInterrupt:
        log_warn("收到键盘中断（KeyboardInterrupt），退出。")
    except RateLimitError as exc:
        if _is_blacklist_rate_limit_error(exc):
            _send_clean_blacklist_alert(exc)
            log_error(f"检测到清洗 403 IP 黑名单拦截，程序退出: {exc}")
            sys.exit(5)
        CORE.send_alert_email(
            "follow-rate-limit",
            "360doc 抓取告警：关注抓取疑似限流",
            f"程序因疑似限流中止: {exc}\n\n{traceback.format_exc()}",
            deduplicate=False,
        )
        log_error(f"疑似限流，程序退出: {exc}")
        sys.exit(3)
    except Exception as exc:
        CORE.send_alert_email(
            "follow-unexpected-stop",
            "360doc 抓取告警：关注抓取程序异常中止",
            f"异常信息: {exc}\n\n{traceback.format_exc()}",
        )
        log_error(f"程序异常中止: {exc}")
        sys.exit(4)


if __name__ == "__main__":
    run()
