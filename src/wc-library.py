"""
360doc 个人文库：登录后按分类分页抓取文章 HTML；可选清洗与 Word（library-processer）。

依赖：requests、beautifulsoup4、python-docx；环境变量与随笔脚本对齐（DOC360_USER、DOC360_PASS、
DOC360_MIN_TIME/MAX_TIME、邮件告警 DOC360_* 等，见 README）。

默认输出目录：<仓库根>/output-space/my-category。CLI：-d/-f/-w/--word-only/--start-page/--end-page 与随笔一致；
-c 启用清洗（写入 clean_ 前缀 HTML 与资源目录）；--start-c/--end-c 与 --c-id/--c-name 筛选分类；
--r、--r-c 控制是否保留 raw 与清洗产物。文章 URL 使用 showweb 模板；429 重试见 ARTICLE_* 常量。
"""

from __future__ import annotations

from _site_paths import ensure_this_file_in_script_dir, output_space_path

_REPO_ROOT, _SCRIPT_DIR = ensure_this_file_in_script_dir(__file__)

import hashlib
import importlib.util
import json
import os
import random
import re
import argparse
import ast
import smtplib
import ssl
import sys
import time
import traceback
from datetime import datetime
from email.message import EmailMessage
from pathlib import Path
from urllib.parse import unquote, urlparse

import requests
from requests import Response

BASE = "http://www.360doc.com"
# 列表接口里的 arturl 可能不可用；与浏览器一致用 showweb 固定模板（文章 id 即 artid）
SHOWWEB_ARTICLE_FMT = f"{BASE}/showweb/0/0/{{}}.aspx"
LOGIN_PAGE = f"{BASE}/login.aspx"
LOGIN_AJAX = f"{BASE}/ajax/login/login.ashx"
LOGIN_ALERT_HANDLER = f"{BASE}/ajax/LoginAlertHandler.ashx"
GET_MY_CATEGORY = f"{BASE}/ajax/getmyCategory.ashx"
GET_CATEGORY_ART = f"{BASE}/ajax/HomeIndex/getCategoryArt.ashx"
GET_MY_DRAFT = f"{BASE}/ajax/getMydraft.ashx"
GET_MY_RECYCLE = f"{BASE}/ajax/HomeIndex/getmyrecycleart.ashx"
MYFILES_REFERER = f"{BASE}/myfiles.aspx"
# 与普通分类列表一致的大页；草稿/回收站与浏览器抓包一致用 10
LIST_PAGE_SIZE_CATEGORY = 50
LIST_PAGE_SIZE_DRAFT_RECYCLE = 10

# 与浏览器抓包一致（redirect-page.txt），登录与列表 AJAX 共用；文章页另用 build_article_headers
BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36 Edg/146.0.0.0"
)

HEADERS = {
    "User-Agent": BROWSER_UA,
    "Referer": LOGIN_PAGE,
    "X-Requested-With": "XMLHttpRequest",
    "Accept": "*/*",
    "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
}

# 文章静态页：Referer 为站点根目录（非 login.aspx），否则易被网关当成异常流量返回 429
ARTICLE_REFERER = f"{BASE}/"
_ARTICLE_ACCEPT = (
    "text/html,application/xhtml+xml,application/xml;q=0.9,"
    "image/avif,image/webp,image/apng,*/*;q=0.8,"
    "application/signed-exchange;v=b3;q=0.7"
)


def _www360doc_host() -> str:
    return urlparse(BASE).netloc.lower()


def build_article_headers(art_url: str) -> dict[str, str]:
    # 构造文章页 GET 头：对齐浏览器新开文档页行为；Referer / X-Requested-With 与列表 AJAX 所用 Session 默认值不同。
    host = _www360doc_host()
    try:
        art_host = urlparse(art_url).netloc.lower()
    except Exception:
        art_host = ""
    if not art_host or art_host == host:
        site = "same-origin"
    elif art_host.endswith(".360doc.com") or art_host == "360doc.com":
        site = "same-site"
    else:
        site = "cross-site"
    return {
        "User-Agent": BROWSER_UA,
        "Accept": _ARTICLE_ACCEPT,
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
        "Accept-Encoding": "gzip, deflate",
        "Referer": ARTICLE_REFERER,
        "Cache-Control": "max-age=0",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": site,
        "Sec-Fetch-User": "?1",
        # Session 默认带 X-Requested-With: XMLHttpRequest；文章页需清空以免与文档请求不一致
        "X-Requested-With": "",
    }


INVALID_NAME_RE = re.compile(r'[<>:"/\\|?*\x00-\x1F]')
REQUEST_TIMEOUT = 30
MAX_RETRY = 3
RETRY_WAIT_SECONDS = 2
# 文章 GET 遇 429 时重试间隔（秒），直至成功；第 ARTICLE_429_ALERT_ATTEMPT 次仍 429 时发警告邮件
ARTICLE_429_RETRY_INTERVAL_SEC = 1
ARTICLE_429_RETRY_LOG_EVERY = 60
ARTICLE_429_ALERT_ATTEMPT = 120
MAX_FILE_STEM = 150

ALERT_RECIPIENT_ENV = "DOC360_ALERT_TO"
SMTP_SENDER_ENV = "DOC360_SENDER"
SMTP_KEY_ENV = "DOC360_KEY"
SMTP_HOST_ENV = "DOC360_SMTP_HOST"
SMTP_PORT_ENV = "DOC360_SMTP_PORT"
SMTP_STARTTLS_ENV = "DOC360_SMTP_STARTTLS"

REQUEST_PACING_MIN_MS_ENV = "DOC360_MIN_TIME"
REQUEST_PACING_MAX_MS_ENV = "DOC360_MAX_TIME"
REQUEST_PACING_DEFAULT_MIN_MS = 2000
REQUEST_PACING_DEFAULT_MAX_MS = 5000

RATE_LIMIT_STATUS_CODES = {403, 429, 503}
RATE_LIMIT_KEYWORDS = ("频繁", "rate limit", "too many", "稍后", "验证码", "limit")
_ALERT_ALREADY_SENT: set[str] = set()
_MAIL_ALERT: dict | None = None

_LIB_PACING_SEC: tuple[float, float] = (
    REQUEST_PACING_DEFAULT_MIN_MS / 1000.0,
    REQUEST_PACING_DEFAULT_MAX_MS / 1000.0,
)

_EMAIL_ADDR_RE = re.compile(
    r"^[a-zA-Z0-9.!#$%&'*+/=?^_`{|}~-]+@"
    r"[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?"
    r"(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$"
)

ERROR_URL_FILE = Path("library_error_url.txt")


# 平台疑似限流异常。
class RateLimitError(RuntimeError):
    pass


def md5_hex(password: str) -> str:
    return hashlib.md5(password.encode("utf-8")).hexdigest()


def log_info(message: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[INFO] {ts} {message}")


def log_warn(message: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[WARN] {ts} {message}")


def log_error(message: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[ERROR] {ts} {message}", file=sys.stderr)


def _env_truthy(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in ("1", "true", "yes", "on")


def _looks_like_email(addr: str) -> bool:
    s = (addr or "").strip()
    if not s or "@" not in s:
        return False
    return bool(_EMAIL_ADDR_RE.match(s))


def configure_email_from_environment() -> None:
    global _MAIL_ALERT
    to_addr = os.environ.get(ALERT_RECIPIENT_ENV, "").strip()
    if not to_addr:
        log_warn(
            "收件人邮箱有误或缺失（环境变量 DOC360_ALERT_TO 未设置或为空），"
            "无法发送告警邮件；异常信息仅通过控制台输出。"
        )
        _MAIL_ALERT = None
        return

    if not _looks_like_email(to_addr):
        log_warn(
            f"收件人邮箱有误或缺失（核对环境变量 {ALERT_RECIPIENT_ENV}），"
            "无法发送告警邮件。"
        )
        _MAIL_ALERT = None
        return

    sender = os.environ.get(SMTP_SENDER_ENV, "").strip()
    password = os.environ.get(SMTP_KEY_ENV, "").strip()
    host = os.environ.get(SMTP_HOST_ENV, "").strip()
    port_raw = os.environ.get(SMTP_PORT_ENV, "").strip()

    if not sender or not password or not host:
        log_warn(
            "发件邮箱信息有误或缺失：需配置 DOC360_SENDER、DOC360_KEY、"
            "DOC360_SMTP_HOST（及按需 DOC360_SMTP_PORT、DOC360_SMTP_STARTTLS）。"
            "无法发送告警邮件。"
        )
        _MAIL_ALERT = None
        return

    if not _looks_like_email(sender):
        log_warn(
            f"发件邮箱格式无效（环境变量 {SMTP_SENDER_ENV}），无法发送告警邮件。"
        )
        _MAIL_ALERT = None
        return

    if port_raw:
        try:
            port = int(port_raw)
            if not (1 <= port <= 65535):
                raise ValueError("range")
        except ValueError:
            log_warn(
                f"SMTP 端口无效（环境变量 {SMTP_PORT_ENV}），无法发送告警邮件。"
            )
            _MAIL_ALERT = None
            return
    else:
        port = 465

    use_starttls = _env_truthy(SMTP_STARTTLS_ENV)
    if not use_starttls and port == 587:
        use_starttls = True

    _MAIL_ALERT = {
        "from_addr": sender,
        "to_addr": to_addr,
        "password": password,
        "host": host,
        "port": port,
        "use_starttls": use_starttls,
    }


def send_alert_email(
    event_key: str, subject: str, body: str, *, deduplicate: bool = True
) -> None:
    if deduplicate and event_key in _ALERT_ALREADY_SENT:
        return
    cfg = _MAIL_ALERT
    if not cfg:
        log_warn(
            "告警邮件未发送：未配置完整邮箱环境（参见启动时的收件人/发件提示）。"
        )
        return

    msg = EmailMessage()
    msg["From"] = cfg["from_addr"]
    msg["To"] = cfg["to_addr"]
    msg["Subject"] = subject
    msg.set_content(body)
    try:
        context = ssl.create_default_context()
        if cfg["use_starttls"]:
            with smtplib.SMTP(
                cfg["host"], cfg["port"], timeout=REQUEST_TIMEOUT
            ) as server:
                server.starttls(context=context)
                server.login(cfg["from_addr"], cfg["password"])
                server.send_message(msg)
        else:
            with smtplib.SMTP_SSL(
                cfg["host"], cfg["port"], context=context, timeout=REQUEST_TIMEOUT
            ) as server:
                server.login(cfg["from_addr"], cfg["password"])
                server.send_message(msg)
        if deduplicate:
            _ALERT_ALREADY_SENT.add(event_key)
        log_info(f"告警邮件已发送到 {cfg['to_addr']}，事件: {event_key}")
    except Exception as exc:
        log_warn(f"告警邮件发送失败 event={event_key} err={exc}")


def _parse_nonneg_int_ms(raw: str) -> int | None:
    s = raw.strip()
    if not s:
        return None
    try:
        v = int(s, 10)
    except ValueError:
        return None
    if v < 0:
        return None
    return v


def resolve_request_pacing_ms() -> tuple[int, int, str]:
    # 解析 DOC360_MIN_TIME / DOC360_MAX_TIME（毫秒）；合法则采用，否则回退 2000–5000 ms。
    d_lo = os.environ.get(REQUEST_PACING_MIN_MS_ENV, "").strip()
    d_hi = os.environ.get(REQUEST_PACING_MAX_MS_ENV, "").strip()
    lo_d = REQUEST_PACING_DEFAULT_MIN_MS
    hi_d = REQUEST_PACING_DEFAULT_MAX_MS
    if not d_lo or not d_hi:
        return lo_d, hi_d, "默认值（未同时设置 DOC360_MIN_TIME 与 DOC360_MAX_TIME）"
    lo = _parse_nonneg_int_ms(d_lo)
    hi = _parse_nonneg_int_ms(d_hi)
    if lo is None or hi is None or lo > hi:
        return lo_d, hi_d, "默认值（解析失败：非负整数且 MIN≤MAX）"
    return lo, hi, "来自环境变量 DOC360_MIN_TIME / DOC360_MAX_TIME"


def _lib_request_pacing_sleep() -> None:
    time.sleep(random.uniform(*_LIB_PACING_SEC))


def _load_library_processer():
    path = Path(__file__).resolve().parent / "library-processer.py"
    spec = importlib.util.spec_from_file_location("_library_processer_impl", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"无法加载 library-processer: {path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def append_error_url_line(line: str) -> None:
    try:
        ERROR_URL_FILE.parent.mkdir(parents=True, exist_ok=True)
        with ERROR_URL_FILE.open("a", encoding="utf-8") as fp:
            fp.write(f"{line}\n")
    except Exception as exc:
        log_warn(f"写入错误 URL 文件失败 line={line!r} err={exc}")


def is_rate_limited_message(message: str) -> bool:
    msg = (message or "").lower()
    return any(word in msg for word in RATE_LIMIT_KEYWORDS)


def decode_text(value: str) -> str:
    if not value:
        return ""
    try:
        return unquote(value)
    except Exception:
        return value


def sanitize_name(name: str, fallback: str) -> str:
    clean = INVALID_NAME_RE.sub("_", name).strip().rstrip(".")
    return clean or fallback


def trim_name(name: str, max_len: int) -> str:
    if len(name) <= max_len:
        return name
    return name[:max_len].rstrip(" ._")


def extract_first_json_block(text: str) -> str:
    start_positions = [pos for pos in (text.find("["), text.find("{")) if pos != -1]
    if not start_positions:
        return text
    start = min(start_positions)
    open_ch = text[start]
    close_ch = "]" if open_ch == "[" else "}"
    depth = 0
    in_str = False
    escaped = False
    str_quote = ""
    for idx in range(start, len(text)):
        ch = text[idx]
        if in_str:
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == str_quote:
                in_str = False
            continue
        if ch == '"' or ch == "'":
            in_str = True
            str_quote = ch
            continue
        if ch == open_ch:
            depth += 1
        elif ch == close_ch:
            depth -= 1
            if depth == 0:
                return text[start : idx + 1]
    return text[start:]


def parse_json_lenient(raw_text: str):
    text = raw_text.strip().lstrip("\ufeff")
    if not text:
        raise ValueError("empty response")
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        block = extract_first_json_block(text)
        patched = re.sub(r'([{\[,]\s*)([A-Za-z_]\w*)(\s*:)', r'\1"\2"\3', block)
        patched = re.sub(r",(\s*[}\]])", r"\1", patched)
        try:
            return json.loads(patched)
        except json.JSONDecodeError:
            py_like = re.sub(r"\btrue\b", "True", patched, flags=re.IGNORECASE)
            py_like = re.sub(r"\bfalse\b", "False", py_like, flags=re.IGNORECASE)
            py_like = re.sub(r"\bnull\b", "None", py_like, flags=re.IGNORECASE)
            return ast.literal_eval(py_like)


def http_get(
    session: requests.Session,
    url: str,
    *,
    params: dict | None = None,
    headers: dict | None = None,
    timeout: int = REQUEST_TIMEOUT,
    retries: int = MAX_RETRY,
    stream: bool = False,
) -> Response:
    last_exc: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            resp = session.get(url, params=params, headers=headers, timeout=timeout, stream=stream)
            resp.raise_for_status()
            return resp
        except Exception as exc:
            if isinstance(exc, requests.HTTPError) and exc.response is not None:
                status = exc.response.status_code
                body_preview = (exc.response.text or "")[:200]
                if status in RATE_LIMIT_STATUS_CODES or is_rate_limited_message(body_preview):
                    raise RateLimitError(
                        f"疑似限流 url={url} status={status} body={body_preview!r}"
                    ) from exc
            last_exc = exc
            if attempt >= retries:
                break
            log_warn(f"请求重试 {attempt}/{retries - 1}: {url}")
            time.sleep(RETRY_WAIT_SECONDS)
    assert last_exc is not None
    raise last_exc


def showweb_article_url(art_id: str) -> str:
    if not art_id.isdigit():
        raise ValueError(f"artid 非数字，无法拼 showweb 地址: {art_id!r}")
    return SHOWWEB_ARTICLE_FMT.format(art_id)


def fetch_showweb_article_stream(session: requests.Session, art_url: str) -> Response:
    # 拉取 showweb 文章 HTML。
    # HTTP 429：关闭连接后等待 ARTICLE_429_RETRY_INTERVAL_SEC 再试，直到 200；
    # 第 ARTICLE_429_ALERT_ATTEMPT 次仍为 429 时发警告邮件。
    # 其它非 200：发警告邮件后 raise_for_status。
    # session.get 抛出异常时发警告邮件后原样抛出。
    headers = build_article_headers(art_url)
    attempt = 0
    while True:
        attempt += 1
        try:
            resp = session.get(
                art_url, headers=headers, timeout=REQUEST_TIMEOUT, stream=False
            )
        except Exception as exc:
            send_alert_email(
                "article-showweb-request-error",
                "360doc 抓取告警：文章页请求失败（网络/超时等）",
                f"url={art_url}\n{exc!r}\n\n{traceback.format_exc()}",
                deduplicate=False,
            )
            raise
        if resp.status_code == 200:
            return resp
        if resp.status_code == 429:
            resp.close()
            if attempt == ARTICLE_429_ALERT_ATTEMPT:
                send_alert_email(
                    f"article-429-{ARTICLE_429_ALERT_ATTEMPT}::{art_url}",
                    f"360doc 抓取告警：文章页连续 {ARTICLE_429_ALERT_ATTEMPT} 次 429",
                    f"url={art_url}\n已连续收到 {attempt} 次 HTTP 429，仍在每秒重试直至成功。",
                    deduplicate=False,
                )
            if attempt == 1 or attempt % ARTICLE_429_RETRY_LOG_EVERY == 0:
                log_warn(
                    f"文章 429，{ARTICLE_429_RETRY_INTERVAL_SEC}s 后重试 "
                    f"url={art_url} 已试 {attempt} 次"
                )
            time.sleep(ARTICLE_429_RETRY_INTERVAL_SEC)
            continue
        body_preview = (resp.text or "")[:500]
        send_alert_email(
            f"article-showweb-http-{resp.status_code}::{art_url}",
            "360doc 抓取告警：文章页 HTTP 非 200（非 429）",
            f"url={art_url}\nstatus={resp.status_code}\nbody_preview={body_preview!r}",
            deduplicate=False,
        )
        try:
            resp.raise_for_status()
        finally:
            resp.close()


def prime_browser_context(session: requests.Session) -> None:
    # 登录后访问首页，贴近浏览器从站内进入文章的链路（Cookie 与后续 Referer 一致）。
    try:
        http_get(
            session,
            f"{BASE}/",
            headers={
                "User-Agent": BROWSER_UA,
                "Accept": (
                    "text/html,application/xhtml+xml,application/xml;q=0.9,"
                    "image/avif,image/webp,image/apng,*/*;q=0.8"
                ),
                "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
                "Referer": LOGIN_PAGE,
                "Upgrade-Insecure-Requests": "1",
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
                "Sec-Fetch-Dest": "document",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "same-origin",
                "Sec-Fetch-User": "?1",
                "X-Requested-With": "",
            },
        )
        log_info("已访问站点首页以同步会话上下文")
    except Exception as exc:
        log_warn(f"访问站点首页失败（忽略）: {exc}")


def login(session: requests.Session, user: str, password: str) -> None:
    http_get(session, LOGIN_PAGE)
    params = {
        "email": user,
        "pws": md5_hex(password),
        "isr": 1,
        "login": 1,
        "code": "",
        "_": int(time.time() * 1000),
    }
    resp = http_get(session, LOGIN_AJAX, params=params)
    body = resp.text.strip()
    preview = body[:300] + ("..." if len(body) > 300 else "")
    log_info(f"login.ashx status={resp.status_code}, body={preview!r}")
    if resp.status_code != 200:
        raise RuntimeError(f"登录请求状态异常: {resp.status_code}")

    try:
        alert_resp = http_get(
            session,
            LOGIN_ALERT_HANDLER,
            params={"type": 1, "_": int(time.time() * 1000)},
        )
        log_info(f"LoginAlertHandler status={alert_resp.status_code}")
    except Exception as exc:
        log_warn(f"LoginAlertHandler 调用失败（忽略）: {exc}")


def _category_list_kind(cid: int) -> str:
    if cid == -3000:
        return "draft"
    if cid == -4000:
        return "recycle"
    return "category"


def fetch_categories(session: requests.Session) -> list[dict]:
    params = {"type": 3, "_": int(time.time() * 1000)}
    resp = http_get(session, GET_MY_CATEGORY, params=params)
    data = parse_json_lenient(resp.text)
    if not isinstance(data, list):
        raise ValueError("分类接口返回非列表")
    categories: list[dict] = []
    for item in data:
        if not isinstance(item, dict):
            continue
        cid = str(item.get("id", "")).strip()
        if not re.fullmatch(r"-?\d+", cid):
            continue
        nid = int(cid)
        if nid in (0, 1):
            continue
        raw_name = str(item.get("selftitle") or item.get("CategoryName") or item.get("name") or cid)
        name = decode_text(raw_name)
        safe_name = sanitize_name(name, f"category-{abs(nid)}")
        kind = _category_list_kind(nid)
        list_pagenum = (
            LIST_PAGE_SIZE_DRAFT_RECYCLE
            if kind in ("draft", "recycle")
            else LIST_PAGE_SIZE_CATEGORY
        )
        categories.append(
            {
                "id": nid,
                "name": name,
                "safe_name": safe_name,
                "list_kind": kind,
                "list_pagenum": list_pagenum,
            }
        )
    categories.sort(key=lambda x: x["id"])
    return categories


def fetch_category_page(session: requests.Session, cat: dict, curnum: int) -> dict:
    cid = cat["id"]
    kind = cat.get("list_kind", "category")
    pagenum = int(cat.get("list_pagenum", LIST_PAGE_SIZE_CATEGORY))
    ts = int(time.time() * 1000)

    if kind == "draft":
        params = {"pagenum": pagenum, "curnum": curnum, "_": ts}
        headers = {
            **HEADERS,
            "Referer": MYFILES_REFERER,
            "Accept": "text/html, */*; q=0.01",
        }
        resp = http_get(session, GET_MY_DRAFT, params=params, headers=headers)
    elif kind == "recycle":
        params = {"pagenum": pagenum, "curnum": curnum, "_": ts}
        headers = {
            **HEADERS,
            "Referer": MYFILES_REFERER,
            "Accept": "text/html, */*; q=0.01",
        }
        resp = http_get(session, GET_MY_RECYCLE, params=params, headers=headers)
    else:
        params = {
            "pagenum": pagenum,
            "curnum": curnum,
            "icid": cid,
            "ishowabstract": 1,
            "word": "",
            "isoriginal": 0,
            "sortarttype": 1,
            "arttype": "",
            "artpermission": "",
            "_": ts,
        }
        resp = http_get(session, GET_CATEGORY_ART, params=params)

    raw = (resp.text or "").strip()
    if not raw:
        return {"status": "1", "artlists": []}
    data = parse_json_lenient(raw)
    if not isinstance(data, dict):
        raise ValueError("文章列表接口返回非对象")
    return data


# 修改 1：增加 bool 返回值，True 表示已下载，False 表示被跳过
def save_article_html(
    session: requests.Session,
    article: dict,
    category_dir: Path,
    category_id: int,
    page_num: int,
    *,
    force_html: bool = False,
) -> bool:
    art_id = str(article.get("artid", "")).strip()
    raw_title = str(article.get("arttitle") or art_id or "untitled")
    title = decode_text(raw_title)
    safe_title = sanitize_name(title, art_id or "untitled")
    if not art_id.isdigit():
        raise ValueError(f"artid 无效（需数字）: {art_id!r}")
    safe_title = trim_name(safe_title, MAX_FILE_STEM)
    file_path = category_dir / f"{art_id}-{safe_title}.html"
    if file_path.exists() and not force_html:
        log_info(f"cat={category_id} page={page_num} skip={file_path.name}")
        # 已存在，返回 False
        return False

    art_url = showweb_article_url(art_id)
    html_resp = fetch_showweb_article_stream(session, art_url)
    html_bytes = html_resp.content

    encoding = html_resp.encoding or "utf-8"
    try:
        text_content = html_bytes.decode(encoding)
    except UnicodeDecodeError:
        text_content = html_bytes.decode("utf-8", errors="replace")

    file_path.write_text(text_content, encoding="utf-8")
    log_info(f"cat={category_id} page={page_num} saved={file_path.name}")
    # 成功下载，返回 True
    return True


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="360doc 文库：登录后按分类分页下载 HTML；可选清洗与 Word（library-processer）。"
    )
    parser.add_argument(
        "-d",
        "--d",
        dest="work_dir",
        default=None,
        metavar="DIR",
        help="输出根目录（相对或绝对）。省略则为 <仓库根>/output-space/my-category。",
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
        help="仅执行清洗/Word 步骤，不登录、不抓取（需输出目录已存在）。",
    )
    parser.add_argument(
        "--start-page",
        type=int,
        default=None,
        metavar="N",
        help="列表起始页码（从 1 起）。省略为 1。",
    )
    parser.add_argument(
        "--end-page",
        type=int,
        default=None,
        metavar="N",
        help="列表结束页码（含）。省略则无页码上界。",
    )
    parser.add_argument(
        "-c",
        dest="do_clean",
        action="store_true",
        help="启用数据清洗（写入 clean_ 前缀 HTML）；与 wc-essay 不同，此处 -c 表示清洗。",
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
        "--start-c",
        default=None,
        metavar="V",
        help="分类范围起点：与 --c-id 时为 id，与 --c-name 时为名称片段。",
    )
    parser.add_argument(
        "--end-c",
        default=None,
        metavar="V",
        help="分类范围终点：与 --c-id 时为 id，与 --c-name 时为名称片段。",
    )
    parser.add_argument(
        "--c-id",
        action="store_true",
        dest="c_by_id",
        help="--start-c/--end-c 解析为分类数字 id（默认，可与省略标志等同）。",
    )
    parser.add_argument(
        "--c-name",
        action="store_true",
        dest="c_by_name",
        help="--start-c/--end-c 按分类名称在接口返回列表中解析为 id 范围。",
    )
    return parser.parse_args()


def apply_category_range(categories: list[dict], start_id: int | None, end_id: int | None) -> list[dict]:
    if not categories:
        return categories
        
    real_min = min(cat["id"] for cat in categories)
    real_max = max(cat["id"] for cat in categories)
    
    low = real_min if start_id is None else max(real_min, start_id)
    high = real_max if end_id is None else min(real_max, end_id)
    
    if low > high:
        return []
    return [cat for cat in categories if low <= cat["id"] <= high]


def _find_category_by_name_hint(categories: list[dict], hint: str | None) -> dict | None:
    if not hint or not str(hint).strip():
        return None
    hint = str(hint).strip()
    for c in categories:
        if c["name"] == hint:
            return c
    for c in categories:
        if hint in c["name"]:
            return c
    return None


def apply_category_range_by_name(
    categories: list[dict], start_name: str | None, end_name: str | None
) -> list[dict]:
    if not categories:
        return []
    cats = sorted(categories, key=lambda x: x["id"])
    sc = _find_category_by_name_hint(cats, start_name)
    ec = _find_category_by_name_hint(cats, end_name)
    real_min = min(c["id"] for c in cats)
    real_max = max(c["id"] for c in cats)
    lo_id = sc["id"] if sc is not None else real_min
    hi_id = ec["id"] if ec is not None else real_max
    if lo_id > hi_id:
        lo_id, hi_id = hi_id, lo_id
    return [c for c in cats if lo_id <= c["id"] <= hi_id]


def resolve_selected_categories(
    categories: list[dict], args: argparse.Namespace
) -> list[dict]:
    if args.c_by_name and args.c_by_id:
        log_error("不能同时使用 --c-id 与 --c-name。")
        sys.exit(1)
    if args.c_by_name:
        return apply_category_range_by_name(categories, args.start_c, args.end_c)
    try:
        sid = int(str(args.start_c).strip(), 10) if args.start_c is not None else None
    except ValueError:
        log_error(f"--start-c 不是合法整数: {args.start_c!r}")
        sys.exit(1)
    try:
        eid = int(str(args.end_c).strip(), 10) if args.end_c is not None else None
    except ValueError:
        log_error(f"--end-c 不是合法整数: {args.end_c!r}")
        sys.exit(1)
    return apply_category_range(categories, sid, eid)


def _log_startup_library_config(
    args: argparse.Namespace,
    root: Path,
    *,
    word_only: bool,
    pacing_lo_ms: int,
    pacing_hi_ms: int,
    pacing_src: str,
    start_page: int,
    end_page: int | None,
    n_cats: int,
) -> None:
    # 打印本次运行将使用的目录、模式、频控与分页等配置。
    log_info("── 本次命令行配置（已生效）──")
    if word_only:
        log_info("模式: 仅清洗/Word（--word-only），不登录、不抓取")
    else:
        log_info("模式: 登录并抓取文库文章 HTML")
    if args.work_dir:
        log_info(f"输出根目录: {root.resolve()}（由 -d / --d 指定）")
    else:
        log_info(f"输出根目录: {root.resolve()}（默认 output-space/my-category）")
    log_info(
        f"请求频控: 每篇文章间隔随机等待 {pacing_lo_ms}–{pacing_hi_ms} ms（{pacing_src}）"
    )
    if not word_only:
        log_info(
            f"分类范围: 按「{'名称（--c-name）' if args.c_by_name else 'id（默认 / --c-id）'}」解析 --start-c / --end-c"
        )
        log_info(f"参与分类数: {n_cats}")
        rng = f"{start_page}–{end_page}" if end_page is not None else f"{start_page}–（无上限）"
        log_info(f"列表页码: {rng}")
    log_info(f"数据清洗 (-c): {'是' if args.do_clean else '否'}")
    log_info(f"Word (-w/--r-c): {'是' if (args.gen_word or args.r_clean_only) else '否'}")
    log_info(f"强制覆盖 (-f): {'是' if args.force else '否'}")
    log_info(f"删除原 HTML (--r): {'是' if args.remove_original else '否'}")
    log_info(f"仅保留 Word (--r-c): {'是' if args.r_clean_only else '否'}")
    log_info("── 以上配置确认后开始执行 ──")


def run() -> None:
    global ERROR_URL_FILE
    global _LIB_PACING_SEC

    args = parse_args()
    if args.r_clean_only:
        args.gen_word = True

    clean_disk = bool(args.do_clean and not args.r_clean_only)
    root = (
        Path(args.work_dir).expanduser()
        if args.work_dir
        else output_space_path("my-category")
    )
    root.mkdir(parents=True, exist_ok=True)
    ERROR_URL_FILE = root / "library_error_url.txt"

    pacing_lo_ms, pacing_hi_ms, pacing_src = resolve_request_pacing_ms()
    _LIB_PACING_SEC = (pacing_lo_ms / 1000.0, pacing_hi_ms / 1000.0)

    proc = _load_library_processer()
    proc.set_processer_loggers(log_info, log_warn)
    proc.set_clean_error_url_file(_REPO_ROOT / "clean_error_url.txt")

    if args.word_only:
        if not root.is_dir():
            log_error(f"目录不存在: {root}")
            sys.exit(1)
        if not clean_disk and not args.gen_word and not args.r_clean_only:
            log_error("word-only 模式下需同时使用 -c、-w 或 --r-c 之一。")
            sys.exit(1)
        _log_startup_library_config(
            args,
            root,
            word_only=True,
            pacing_lo_ms=pacing_lo_ms,
            pacing_hi_ms=pacing_hi_ms,
            pacing_src=pacing_src,
            start_page=1,
            end_page=None,
            n_cats=0,
        )
        configure_email_from_environment()
        session = requests.Session()
        session.headers.update(
            {
                "User-Agent": BROWSER_UA,
                "Accept": "*/*",
                "Accept-Language": "zh-CN,zh;q=0.9",
            }
        )
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
        )
        sys.exit(0 if nf == 0 else 1)

    start_page = 1 if args.start_page is None else args.start_page
    end_page: int | None = args.end_page
    if start_page < 1:
        log_error("--start-page 应为 >= 1 的整数")
        sys.exit(1)
    if end_page is not None:
        if end_page < 1:
            log_error("--end-page 应为 >= 1 的整数")
            sys.exit(1)
        if end_page < start_page:
            log_error("--end-page 不可小于 --start-page")
            sys.exit(1)

    user = os.environ.get("DOC360_USER", "").strip()
    password = os.environ.get("DOC360_PASS", "")
    if not user or not password:
        log_error("缺少环境变量 DOC360_USER 或 DOC360_PASS。")
        sys.exit(1)

    session = requests.Session()
    session.headers.update(HEADERS)
    try:
        login(session, user, password)
    except Exception as exc:
        send_alert_email(
            "login-failed",
            "360doc 抓取告警：登录失败",
            f"登录失败: {exc}\n\n{traceback.format_exc()}",
        )
        log_error(f"登录失败: {exc}")
        sys.exit(2)
    prime_browser_context(session)
    configure_email_from_environment()

    try:
        all_categories = fetch_categories(session)
        selected_categories = resolve_selected_categories(all_categories, args)
        if not selected_categories:
            log_warn("筛选后无可处理分类；核对 --start-c / --end-c 与 --c-id / --c-name。")
            return

        _log_startup_library_config(
            args,
            root,
            word_only=False,
            pacing_lo_ms=pacing_lo_ms,
            pacing_hi_ms=pacing_hi_ms,
            pacing_src=pacing_src,
            start_page=start_page,
            end_page=end_page,
            n_cats=len(selected_categories),
        )

        log_info(
            f"分类过滤: total={len(all_categories)} selected={len(selected_categories)} "
            f"range=[{selected_categories[0]['id']}, {selected_categories[-1]['id']}]"
        )

        base_dir = root
        for cat in selected_categories:
            cid = cat["id"]
            cname = cat["safe_name"]
            dir_id = abs(cid) if cid < 0 else cid
            page_size = int(cat.get("list_pagenum", LIST_PAGE_SIZE_CATEGORY))
            category_dir = base_dir / f"{dir_id}-{cname}"
            category_dir.mkdir(parents=True, exist_ok=True)
            log_info(f"开始分类: {cid}-{cat['name']} (保存目录 {dir_id}-{cname})")
            category_error_logged = False
            page = start_page
            while True:
                if end_page is not None and page > end_page:
                    log_info(
                        f"已达页码上界 --end-page={end_page}，结束本分类 "
                        f"cat={cid}-{cat['name']}"
                    )
                    break
                try:
                    data = fetch_category_page(session, cat, page)
                except Exception as exc:
                    log_warn(f"分类请求失败 cat={cid} page={page} err={exc}")
                    break

                status = str(data.get("status", "")).strip()
                if status != "1":
                    log_warn(f"分类状态异常 cat={cid} page={page} status={status!r}")
                    break

                artlists = data.get("artlists") or []
                if not isinstance(artlists, list) or not artlists:
                    log_info(f"分类结束 cat={cid} page={page}")
                    break

                for art in artlists:
                    try:
                        did_fetch = save_article_html(
                            session,
                            art,
                            category_dir,
                            cid,
                            page,
                            force_html=args.force,
                        )
                    except Exception as exc:
                        art_id = str(art.get("artid", "unknown"))
                        art_title = decode_text(
                            str(art.get("arttitle") or "").strip()
                        ) or "unknown"
                        art_url = str(art.get("arturl", "")).strip() or "unknown"
                        if not category_error_logged:
                            append_error_url_line(f"{cid}-{cat['name']}")
                            category_error_logged = True
                        append_error_url_line(f"{art_id}-{art_title}-{art_url}-{exc}")
                        log_warn(f"文章失败 cat={cid} page={page} art={art_id} err={exc}")
                        _lib_request_pacing_sleep()
                    else:
                        if did_fetch:
                            _lib_request_pacing_sleep()

                if len(artlists) < page_size:
                    break
                page += 1

        gen_word_effective = bool(args.gen_word or args.r_clean_only)
        if clean_disk or gen_word_effective:
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
            )
    except KeyboardInterrupt:
        log_warn("收到键盘中断（KeyboardInterrupt），退出。")
    except RateLimitError as exc:
        send_alert_email(
            "rate-limit",
            "360doc 抓取告警：疑似限流",
            f"程序因疑似限流中止: {exc}\n\n{traceback.format_exc()}",
            deduplicate=False,
        )
        log_error(f"疑似限流，程序退出: {exc}")
        sys.exit(3)
    except Exception as exc:
        send_alert_email(
            "unexpected-stop",
            "360doc 抓取告警：程序异常中止",
            f"异常信息: {exc}\n\n{traceback.format_exc()}",
        )
        log_error(f"程序异常中止: {exc}")
        sys.exit(4)


if __name__ == "__main__":
    run()