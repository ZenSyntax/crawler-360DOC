"""360doc 随笔抓取入口：按分类分页备份清洗 HTML，可选转换 Word。详见 README「wc-essay」。"""

from __future__ import annotations

from _site_paths import ensure_this_file_in_script_dir, output_space_path

_REPO_ROOT, _ = ensure_this_file_in_script_dir(__file__)

import argparse
import hashlib
import importlib.util
import os
import random
import re
import smtplib
import ssl
import sys
import time
import traceback
from collections.abc import Mapping, Sequence
from datetime import datetime
from email.message import EmailMessage
from html import escape, unescape
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from bs4.element import Tag

# 可选依赖 lxml 作为 BS4 解析器；未安装则回退 html.parser。
_BS_PARSER = "lxml" if importlib.util.find_spec("lxml") else "html.parser"
from requests import Response

BASE = "http://www.360doc.com"
LOGIN_PAGE = f"{BASE}/login.aspx"
LOGIN_AJAX = f"{BASE}/ajax/login/login.ashx"
LOGIN_ALERT_HANDLER = f"{BASE}/ajax/LoginAlertHandler.ashx"
MYFILES_REFERER = f"{BASE}/myfiles.aspx"

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

REQUEST_TIMEOUT = 30
MAX_RETRY = 3
RETRY_WAIT_SECONDS = 2

ARTICLE_429_RETRY_INTERVAL_SEC = 1
ARTICLE_429_RETRY_LOG_EVERY = 60
ARTICLE_429_ALERT_ATTEMPT = 120

RATE_LIMIT_STATUS_CODES = {403, 429, 503}
RATE_LIMIT_KEYWORDS = ("频繁", "rate limit", "too many", "稍后", "验证码", "limit")

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

_ALERT_ALREADY_SENT: set[str] = set()

_MAIL_ALERT: dict | None = None

# 随笔列表抓取：每推进一页后的随机等待（秒），由 run() 按环境变量写入
_ESSAY_PACING_SEC: tuple[float, float] = (
    REQUEST_PACING_DEFAULT_MIN_MS / 1000.0,
    REQUEST_PACING_DEFAULT_MAX_MS / 1000.0,
)

_EMAIL_ADDR_RE = re.compile(
    r"^[a-zA-Z0-9.!#$%&'*+/=?^_`{|}~-]+@"
    r"[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?"
    r"(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$"
)


def _env_truthy(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in ("1", "true", "yes", "on")


def _looks_like_email(addr: str) -> bool:
    s = (addr or "").strip()
    if not s or "@" not in s:
        return False
    return bool(_EMAIL_ADDR_RE.match(s))


def log_info(message: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[INFO] {ts} {message}")


def log_warn(message: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[WARN] {ts} {message}")


def log_error(message: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[ERROR] {ts} {message}", file=sys.stderr)


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
    # 解析 DOC360_MIN_TIME / DOC360_MAX_TIME（毫秒）。两者均为非负整数且 MIN≤MAX 时生效，否则回退 2000–5000 ms。
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


def _essay_request_pacing_sleep() -> None:
    time.sleep(random.uniform(*_ESSAY_PACING_SEC))


def _load_essay_to_word_impl():
    path = Path(__file__).resolve().parent / "essay-to-word.py"
    spec = importlib.util.spec_from_file_location("_essay_to_word_impl", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"无法加载 Word 子模块: {path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


_ESSAY_TO_WORD = _load_essay_to_word_impl()


def convert_essay_html_tree_to_docx(
    root: Path, *, force: bool, incremental_docx: bool = True
) -> int:
    return _ESSAY_TO_WORD.convert_essay_html_tree_to_docx(
        root,
        force=force,
        incremental_docx=incremental_docx,
        log_info=log_info,
        log_warn=log_warn,
    )


def configure_email_from_environment() -> None:
    # 按环境变量填充 _MAIL_ALERT；配置不全则告警邮件关闭，仅打日志。
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


# 平台疑似限流异常。
class RateLimitError(RuntimeError):
    pass


def md5_hex(password: str) -> str:
    return hashlib.md5(password.encode("utf-8")).hexdigest()


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


def is_rate_limited_message(message: str) -> bool:
    msg = (message or "").lower()
    return any(word in msg for word in RATE_LIMIT_KEYWORDS)


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
            resp = session.get(
                url, params=params, headers=headers, timeout=timeout, stream=stream
            )
            resp.raise_for_status()
            return resp
        except Exception as exc:
            if isinstance(exc, requests.HTTPError) and exc.response is not None:
                status = exc.response.status_code
                body_preview = (exc.response.text or "")[:200]
                if status in RATE_LIMIT_STATUS_CODES or is_rate_limited_message(
                    body_preview
                ):
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


ESSAY_HANDLER_NEW = f"{BASE}/ajax/EssayHandler_New.ashx"
ESSAY_HANDLER = f"{BASE}/ajax/EssayHandler.ashx"

_ESSAY_ERROR_LOG: Path = Path("logs/essay_error_url.txt")

ESSAY_CATEGORIES: tuple[tuple[int, str], ...] = (
    (2, "待分类"),
    (3, "日记"),
    (4, "普通随笔"),
)

_ESSAY_CATEGORY_IDS: frozenset[int] = frozenset(c[0] for c in ESSAY_CATEGORIES)

NOT_FOUND_TITLE_SNIPPET = "<title>你浏览的页面不存在</title>"

ESSAY_POST_NETWORK_RETRIES = 5
ESSAY_POST_NETWORK_RETRY_WAIT_SEC = 2.0

FIRST_DATE_RE = re.compile(
    r'class\s*=\s*["\']resavedatespan["\'][^>]*>([^<]+)',
    re.I,
)

CLEAN_PAGE_TEMPLATE = """<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>360doc随笔 · {category_id}-{category_label} · 第{page}页</title>
  <style>
    body {{
      font-family: "Microsoft YaHei", "PingFang SC", Arial, sans-serif;
      margin: 0;
      padding: 24px 20px 48px;
      background: #fafafa;
      color: #222;
    }}
    .wrap {{
      max-width: 720px;
      margin: 0 auto;
      background: #fff;
      padding: 28px 32px 36px;
      border-radius: 8px;
      box-shadow: 0 1px 3px rgba(0,0,0,.06);
    }}
    .page-meta {{
      font-size: 13px;
      color: #666;
      margin-bottom: 28px;
      padding-bottom: 16px;
      border-bottom: 1px solid #eee;
    }}
    .essay {{
      padding: 22px 0;
      border-bottom: 1px solid #eee;
    }}
    .essay:last-child {{ border-bottom: none; padding-bottom: 0; }}
    .essay-date {{
      font-size: 15px;
      font-weight: 600;
      color: #1a1a1a;
      margin-bottom: 14px;
      letter-spacing: 0.02em;
    }}
    .essay-date time {{ color: #333; }}
    .essay-body {{
      font-size: 16px;
      line-height: 1.8;
      white-space: pre-wrap;
      word-break: break-word;
    }}
    .no-data {{ color: #888; font-size: 15px; line-height: 1.7; }}
  </style>
</head>
<body>
  <div class="wrap">
    <header class="page-meta">
      分类 <strong>{category_id}</strong> · {category_label} · 第 <strong>{page}</strong> 页 · 共 <strong>{count}</strong> 条随笔
    </header>
    <main>
{articles}
    </main>
  </div>
</body>
</html>
"""


def _beautiful_soup(markup: str) -> BeautifulSoup:
    return BeautifulSoup(markup, _BS_PARSER)


def append_essay_error_url_line(line: str) -> None:
    try:
        _ESSAY_ERROR_LOG.parent.mkdir(parents=True, exist_ok=True)
        with _ESSAY_ERROR_LOG.open("a", encoding="utf-8") as fp:
            fp.write(f"{line}\n")
    except Exception as exc:
        log_warn(f"写入 essay_error_url.txt 失败 line={line!r} err={exc}")


EssayFormData = Sequence[tuple[str, str]] | Mapping[str, str]


def _essay_post_ctx(category_id: int | None, page_index: int | None) -> str:
    if category_id is not None and page_index is not None:
        return f"\n分类={category_id} page={page_index}"
    return ""


def _essay_post_rate_limit_sleep_and_log(
    attempt: int,
    post_url: str,
    *,
    category_id: int | None,
    page_index: int | None,
) -> None:
    if attempt == ARTICLE_429_ALERT_ATTEMPT:
        ctx = _essay_post_ctx(category_id, page_index)
        send_alert_email(
            f"essay-post-limit-{ARTICLE_429_ALERT_ATTEMPT}::{post_url}",
            f"360doc 随笔告警：列表 POST 连续 {ARTICLE_429_ALERT_ATTEMPT} 次疑似限流",
            f"url={post_url}{ctx}\n已连续 {attempt} 次，仍在每秒重试直至成功。",
            deduplicate=False,
        )
    if attempt == 1 or attempt % ARTICLE_429_RETRY_LOG_EVERY == 0:
        log_warn(
            f"随笔 POST 疑似限流，{ARTICLE_429_RETRY_INTERVAL_SEC}s 后重试 "
            f"url={post_url} cat={category_id} page={page_index} 已试 {attempt} 次"
        )
    time.sleep(ARTICLE_429_RETRY_INTERVAL_SEC)


def essay_post_until_success(
    session: requests.Session,
    post_url: str,
    *,
    data: EssayFormData,
    headers: dict | None = None,
    timeout: int = REQUEST_TIMEOUT,
    category_id: int | None = None,
    page_index: int | None = None,
) -> Response:
    # 随笔列表 POST：限流则重试；网络错误先本地重试再告警；其它 HTTP 错误发邮件后抛出。
    attempt = 0
    while True:
        attempt += 1
        resp: Response | None = None
        last_net_exc: Exception | None = None
        for net_try in range(1, ESSAY_POST_NETWORK_RETRIES + 1):
            try:
                resp = session.post(
                    post_url, data=data, headers=headers, timeout=timeout
                )
                last_net_exc = None
                break
            except Exception as exc:
                last_net_exc = exc
                if net_try < ESSAY_POST_NETWORK_RETRIES:
                    log_warn(
                        f"POST 网络异常 {net_try}/{ESSAY_POST_NETWORK_RETRIES}，"
                        f"{ESSAY_POST_NETWORK_RETRY_WAIT_SEC:g}s 后重试 "
                        f"url={post_url} err={exc!r}"
                    )
                    time.sleep(ESSAY_POST_NETWORK_RETRY_WAIT_SEC)
                else:
                    send_alert_email(
                        "essay-post-request-error",
                        "360doc 随笔告警：列表 POST 失败（网络/超时等）",
                        f"url={post_url}{_essay_post_ctx(category_id, page_index)}\n"
                        f"已重试 {ESSAY_POST_NETWORK_RETRIES} 次仍失败\n"
                        f"{last_net_exc!r}\n\n{traceback.format_exc()}",
                        deduplicate=False,
                    )
                    raise last_net_exc
        assert resp is not None
        text = resp.text or ""

        if resp.status_code == 200:
            if not is_rate_limited_message(text[:800]):
                return resp
            resp.close()
            _essay_post_rate_limit_sleep_and_log(
                attempt, post_url, category_id=category_id, page_index=page_index
            )
            continue

        if resp.status_code in (429, 503):
            resp.close()
            _essay_post_rate_limit_sleep_and_log(
                attempt, post_url, category_id=category_id, page_index=page_index
            )
            continue

        if resp.status_code == 403 and is_rate_limited_message(text[:500]):
            resp.close()
            _essay_post_rate_limit_sleep_and_log(
                attempt, post_url, category_id=category_id, page_index=page_index
            )
            continue

        body_preview = text[:500]
        send_alert_email(
            f"essay-post-http-{resp.status_code}::{post_url}",
            "360doc 随笔告警：随笔列表 POST 非 200（非可重试限流）",
            f"url={post_url}\nstatus={resp.status_code}\n"
            f"cat={category_id} page={page_index}\nbody_preview={body_preview!r}",
            deduplicate=False,
        )
        try:
            resp.raise_for_status()
        finally:
            resp.close()


def build_essay_post(category_id: int, page: int) -> tuple[str, list[tuple[str, str]]]:
    tail: list[tuple[str, str]] = [
        ("idxUserType", "1"),
        ("ishomeuser", "false"),
        ("pageindex", str(page)),
        ("categoryid", str(category_id)),
    ]
    if page <= 1:
        return ESSAY_HANDLER_NEW, [("op", "getEssayToUserid"), *tail]
    return ESSAY_HANDLER, [("getEssayToUserid", ""), *tail]


def prime_essay_list_context(session: requests.Session) -> None:
    headers = {
        **HEADERS,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Referer": f"{BASE}/",
    }
    http_get(session, MYFILES_REFERER, headers=headers)


def essay_response_html(resp: Response) -> str:
    ct = (resp.headers.get("Content-Type") or "").lower()
    if "charset=" not in ct:
        guessed = resp.apparent_encoding
        if guessed:
            resp.encoding = guessed
        else:
            resp.encoding = "utf-8"
    text = resp.text or ""
    if not text.strip() and resp.content:
        text = resp.content.decode("utf-8", errors="replace")
    return text


def diagnose_essay_list_response(resp: Response, html: str) -> str:
    raw = resp.content or b""
    raw_len = len(raw)
    text = html or ""
    text_len = len(text)
    text_stripped_len = len(text.strip())
    ct = (resp.headers.get("Content-Type") or "").strip()
    ce = (resp.headers.get("Content-Encoding") or "").strip()

    bits: list[str] = [
        f"status={resp.status_code}",
        f"raw_bytes={raw_len}",
        f"text_chars={text_len}",
        f"text_nonblank={text_stripped_len}",
        f"Content-Type={ct!r}",
    ]
    if ce:
        bits.append(f"Content-Encoding={ce!r}")

    if raw_len == 0:
        bits.append("判定=响应体无任何字节(多为已无下一页、会话失效或接口未返回内容)")
        return " | ".join(bits)

    if text_stripped_len == 0:
        bits.append("判定=有原始字节但解码后几乎为空(可检查压缩/编码)")
        bits.append(f"raw_hex_head={raw[:24].hex()}")
        return " | ".join(bits)

    compact = re.sub(r"\s+", " ", text.strip())[:400]
    bits.append(f"body_head={compact!r}")

    for kw in (
        "你浏览的页面不存在",
        "登录",
        "未登录",
        "重新登录",
        "错误",
        "失败",
        "异常",
        "频繁",
        "验证码",
        "系统繁忙",
    ):
        if kw in text:
            bits.append(f"keyword={kw}")

    if "newshuodiv" not in text and "essaycontent" not in text:
        bits.append("判定=有 HTML/文本但不含 newshuodiv/essaycontent(结构变更或非列表片段)")
    else:
        bits.append("判定=含部分随笔相关标记但未解析出块(选择器或子结构可能已变)")

    return " | ".join(bits)


def extract_first_essay_date_yy_mm_dd(html: str) -> str | None:
    m = FIRST_DATE_RE.search(html)
    if not m:
        return None
    raw = unescape(m.group(1))
    raw = raw.replace("\xa0", " ").replace("\u00a0", " ")
    raw = re.sub(r"\s+", "", raw)
    if re.fullmatch(r"\d{2}-\d{2}-\d{2}", raw):
        return raw
    return None


def _normalize_date_span_text(date_el: Tag | None) -> str | None:
    if date_el is None:
        return None
    raw = unescape(date_el.get_text())
    raw = raw.replace("\xa0", " ").replace("\u00a0", " ")
    raw = re.sub(r"\s+", "", raw)
    if re.fullmatch(r"\d{2}-\d{2}-\d{2}", raw):
        return raw
    return None


def _extract_essay_body_text(content_el: Tag | None) -> str:
    if content_el is None:
        return ""
    text = content_el.get_text("\n", strip=True)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def yy_mm_dd_to_iso_date(s: str) -> str:
    if not re.fullmatch(r"\d{2}-\d{2}-\d{2}", s):
        return ""
    yy, mm, dd = s.split("-")
    y = int(yy)
    year = 2000 + y if y <= 69 else 1900 + y
    return f"{year}-{mm}-{dd}"


def parse_essay_entries(raw_html: str) -> list[tuple[str, str]]:
    soup = _beautiful_soup(raw_html)
    blocks = soup.select("div.newshuodiv")
    out: list[tuple[str, str]] = []
    for block in blocks:
        if not isinstance(block, Tag):
            continue
        date_el = block.select_one("span.resavedatespan")
        date_s = _normalize_date_span_text(date_el)
        content_el = block.select_one('[name="essaycontent"]')
        body = _extract_essay_body_text(content_el)
        if not body and date_s is None:
            continue
        display_date = date_s if date_s is not None else "未知日期"
        out.append((display_date, body))
    return out


def _build_articles_fragment(entries: list[tuple[str, str]]) -> str:
    lines: list[str] = []
    for date_s, body in entries:
        iso = yy_mm_dd_to_iso_date(date_s) if re.fullmatch(r"\d{2}-\d{2}-\d{2}", date_s) else ""
        dt_attr = f' datetime="{escape(iso)}"' if iso else ""
        lines.append('      <article class="essay">')
        lines.append(
            f'        <div class="essay-date"><time{dt_attr}>{escape(date_s)}</time></div>'
        )
        lines.append(f'        <div class="essay-body">{escape(body)}</div>')
        lines.append("      </article>")
    return "\n".join(lines)


def build_clean_essay_page_html_from_entries(
    entries: list[tuple[str, str]],
    *,
    category_id: int,
    category_label: str,
    page: int,
) -> str:
    if not entries:
        log_warn(f"未解析到随笔块，保存占位页 cat={category_id} page={page}")
        articles = '      <p class="no-data">本页未能解析出随笔条目（接口 HTML 结构可能已变更）。</p>'
        count = 0
    else:
        articles = _build_articles_fragment(entries)
        count = len(entries)
    return CLEAN_PAGE_TEMPLATE.format(
        category_id=category_id,
        category_label=escape(category_label),
        page=page,
        count=count,
        articles=articles,
    )


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="360doc 随笔。完整说明见 README.md「wc-essay」。"
    )
    p.add_argument(
        "-c",
        "--c",
        dest="category_id",
        type=int,
        default=None,
        metavar="CAT_ID",
        help="只抓取该随笔分类 ID：2=待分类，3=日记，4=普通随笔。省略则抓取全部。",
    )
    p.add_argument(
        "--start-page",
        type=int,
        default=None,
        metavar="N",
        help="列表起始页码（从 1 起）。省略为 1。对所有参与抓取的分类生效。",
    )
    p.add_argument(
        "--end-page",
        type=int,
        default=None,
        metavar="N",
        help="列表结束页码（含）。省略则不因页码上界停止（仍会在无更多数据时结束）。",
    )
    p.add_argument(
        "-d",
        "--d",
        dest="work_dir",
        metavar="DIR",
        default=None,
        help="随笔 HTML 输出根目录（相对或绝对）。省略则为 <仓库根>/output-space/my-essay。",
    )
    p.add_argument(
        "-f",
        "--f",
        dest="force",
        action="store_true",
        help="强制覆盖已存在的 HTML；与 --w 同用时亦强制覆盖 .docx。",
    )
    p.add_argument(
        "-w",
        "--w",
        dest="gen_word",
        action="store_true",
        help="抓取结束后将输出目录下清洗 HTML 转为同名 .docx（默认 mtime 增量）。",
    )
    p.add_argument(
        "--word-only",
        dest="word_only",
        action="store_true",
        help="仅执行 Word 转换，不登录、不抓取（需输出目录已存在）。",
    )
    return p.parse_args()


def _validate_and_resolve_crawl_scope(
    args: argparse.Namespace,
) -> tuple[tuple[tuple[int, str], ...], int, int | None]:
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

    if args.category_id is not None:
        if args.category_id not in _ESSAY_CATEGORY_IDS:
            log_error(
                f"无效分类 ID {args.category_id}，有效值: "
                f"{sorted(_ESSAY_CATEGORY_IDS)}"
            )
            sys.exit(1)
        cats = tuple(c for c in ESSAY_CATEGORIES if c[0] == args.category_id)
    else:
        cats = ESSAY_CATEGORIES

    return cats, start_page, end_page


def _log_startup_effective_config(
    args: argparse.Namespace,
    root: Path,
    *,
    word_only: bool,
    categories_to_crawl: tuple[tuple[int, str], ...] | None = None,
    start_page: int | None = None,
    end_page: int | None = None,
    request_pacing_ms: tuple[int, int] | None = None,
    request_pacing_source: str | None = None,
) -> None:
    # 在正式执行前输出本次命令行生效项（供核对）。
    root_abs = root.resolve()
    log_info("── 本次命令行配置（已生效）──")
    if word_only:
        log_info("模式: 仅 Word 转换（--word-only），不登录、不抓取")
    else:
        log_info("模式: 登录并抓取随笔列表页 HTML")
    if args.work_dir:
        log_info(f"输出根目录: {root_abs}（由 -d / --d 指定）")
    else:
        log_info(f"输出根目录: {root_abs}（未指定 -d，使用默认路径）")
    if word_only:
        if args.force:
            log_info("Word: 强制覆盖已有 .docx（-f）")
        else:
            log_info("Word: 按修改时间增量更新 .docx（默认）")
    else:
        if args.force:
            log_info("HTML: 强制覆盖已存在的分页 HTML（-f）")
        else:
            log_info("HTML: 已存在的分页 HTML 将跳过（默认）")
        if args.gen_word:
            if args.force:
                log_info(
                    "Word: 抓取结束后转换 .docx；与 -f 同用时强制覆盖已有 .docx（-w）"
                )
            else:
                log_info("Word: 抓取结束后将 HTML 转为 .docx，按 mtime 增量（-w）")
        else:
            log_info("Word: 未使用 -w，抓取结束后不生成 .docx")
        assert categories_to_crawl is not None and start_page is not None
        range_desc = (
            f"{start_page}–{end_page}"
            if end_page is not None
            else f"{start_page}–（无上限，直至列表结束）"
        )
        cat_desc = (
            "全部（2/3/4 三类依次）"
            if args.category_id is None
            else f"{args.category_id}-"
            + next(l for i, l in ESSAY_CATEGORIES if i == args.category_id)
        )
        log_info(f"抓取分类: {cat_desc}")
        log_info(f"列表页码: {range_desc}")
        assert request_pacing_ms is not None and request_pacing_source is not None
        pmn, pmx = request_pacing_ms
        log_info(
            f"请求频控: 每页迭代随机等待 {pmn}–{pmx} ms（{request_pacing_source}）"
        )
    log_info("── 以上配置确认后开始执行 ──")


def run() -> None:
    global _ESSAY_ERROR_LOG
    global _ESSAY_PACING_SEC

    args = parse_args()

    if args.word_only:
        root = (
            Path(args.work_dir).expanduser()
            if args.work_dir
            else output_space_path("my-essay")
        )
        if not root.is_dir():
            log_error(f"目录不存在: {root}")
            sys.exit(1)
        _log_startup_effective_config(args, root, word_only=True)
        log_info(f"HTML 解析器: {_BS_PARSER}")
        failed = convert_essay_html_tree_to_docx(
            root, force=bool(args.force), incremental_docx=True
        )
        sys.exit(0 if failed == 0 else 1)

    root = (
        Path(args.work_dir).expanduser()
        if args.work_dir
        else output_space_path("my-essay")
    )
    root.mkdir(parents=True, exist_ok=True)
    logs_dir = _REPO_ROOT / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    _ESSAY_ERROR_LOG = logs_dir / "essay_error_url.txt"

    categories_to_crawl, start_page, end_page = _validate_and_resolve_crawl_scope(
        args
    )
    pacing_lo_ms, pacing_hi_ms, pacing_src = resolve_request_pacing_ms()
    _ESSAY_PACING_SEC = (pacing_lo_ms / 1000.0, pacing_hi_ms / 1000.0)
    _log_startup_effective_config(
        args,
        root,
        word_only=False,
        categories_to_crawl=categories_to_crawl,
        start_page=start_page,
        end_page=end_page,
        request_pacing_ms=(pacing_lo_ms, pacing_hi_ms),
        request_pacing_source=pacing_src,
    )

    log_info(f"HTML 解析器: {_BS_PARSER}")
    configure_email_from_environment()

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
            "essay-login-failed",
            "360doc 随笔告警：登录失败",
            f"登录失败: {exc}\n\n{traceback.format_exc()}",
        )
        log_error(f"登录失败: {exc}")
        sys.exit(2)

    post_headers = {
        **HEADERS,
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Origin": BASE,
        "Referer": MYFILES_REFERER,
        "Accept": "text/html, */*; q=0.01",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
    }

    force_html = bool(args.force)
    crawl_interrupted = False

    try:
        for category_id, category_label in categories_to_crawl:
            cat_dir = root / f"{category_id}-{category_label}"
            cat_dir.mkdir(parents=True, exist_ok=True)
            log_info(f"开始随笔分类: {category_id}-{category_label}")
            try:
                prime_essay_list_context(session)
            except Exception as exc:
                log_warn(f"切换分类前刷新 myfiles 失败 cat={category_id}: {exc}")

            page = start_page
            while True:
                if end_page is not None and page > end_page:
                    log_info(
                        f"已达页码上界 --end-page={end_page}，结束本分类 "
                        f"cat={category_id}-{category_label}"
                    )
                    break
                post_url, form = build_essay_post(category_id, page)
                try:
                    resp = essay_post_until_success(
                        session,
                        post_url,
                        data=form,
                        headers=post_headers,
                        category_id=category_id,
                        page_index=page,
                    )
                except Exception as exc:
                    append_essay_error_url_line(
                        f"{category_id}-{page}-{post_url}-{exc}"
                    )
                    log_warn(f"随笔请求失败 cat={category_id} page={page} err={exc}")
                    break

                html = essay_response_html(resp)
                if NOT_FOUND_TITLE_SNIPPET in html:
                    log_info(f"随笔分类结束（无更多页）cat={category_id} 最后尝试 page={page}")
                    break

                entries = parse_essay_entries(html)
                if (
                    page >= 2
                    and not entries
                    and NOT_FOUND_TITLE_SNIPPET not in html
                ):
                    log_warn(
                        f"随笔 cat={category_id} page={page} 首次解析为空，"
                        f"刷新 myfiles 后重试同一 POST"
                    )
                    try:
                        prime_essay_list_context(session)
                        resp = essay_post_until_success(
                            session,
                            post_url,
                            data=form,
                            headers=post_headers,
                            category_id=category_id,
                            page_index=page,
                        )
                        html = essay_response_html(resp)
                        if NOT_FOUND_TITLE_SNIPPET in html:
                            log_info(
                                f"随笔分类结束（无更多页）cat={category_id} "
                                f"重试后 page={page}"
                            )
                            break
                        entries = parse_essay_entries(html)
                    except Exception as retry_exc:
                        log_warn(f"重试失败: {retry_exc}")

                if not entries:
                    diag = diagnose_essay_list_response(resp, html)
                    log_warn(
                        f"未解析到随笔块 cat={category_id} page={page} — 诊断: {diag}"
                    )
                    safe_diag = diag.replace("\n", " ").replace("|", ";")[:900]
                    append_essay_error_url_line(
                        f"{category_id}-{page}-{post_url}-no-essay|{safe_diag}"
                    )
                    log_warn(
                        "结束本分类：无有效随笔列表（raw_bytes=0 多为末页或会话异常；"
                        "若诊断含 keyword=，对照返回内容核对登录态与限流）"
                    )
                    break

                first_norm = next(
                    (d for d, _ in entries if re.fullmatch(r"\d{2}-\d{2}-\d{2}", d)),
                    None,
                )
                date_part = first_norm or extract_first_essay_date_yy_mm_dd(html) or "unknown"
                file_name = f"{page}-{date_part}.html"
                out_path = cat_dir / file_name

                if out_path.exists() and not force_html:
                    log_info(f"cat={category_id} page={page} skip={file_name}")
                    page += 1
                    _essay_request_pacing_sleep()
                    continue

                full_doc = build_clean_essay_page_html_from_entries(
                    entries,
                    category_id=category_id,
                    category_label=category_label,
                    page=page,
                )
                out_path.write_text(full_doc, encoding="utf-8")
                log_info(f"已保存: {out_path.relative_to(root)}")

                page += 1
                _essay_request_pacing_sleep()

    except KeyboardInterrupt:
        crawl_interrupted = True
        log_warn("收到键盘中断（KeyboardInterrupt），退出。")
    except RateLimitError as exc:
        send_alert_email(
            "essay-rate-limit-global",
            "360doc 随笔告警：疑似限流（全局）",
            f"{exc}\n\n{traceback.format_exc()}",
            deduplicate=False,
        )
        log_error(f"疑似限流: {exc}")
        sys.exit(3)
    except Exception as exc:
        send_alert_email(
            "essay-unexpected",
            "360doc 随笔告警：程序异常中止",
            f"{exc}\n\n{traceback.format_exc()}",
        )
        log_error(f"程序异常: {exc}")
        sys.exit(4)

    if crawl_interrupted:
        return

    if args.gen_word:
        failed = convert_essay_html_tree_to_docx(
            root, force=force_html, incremental_docx=True
        )
        if failed:
            sys.exit(1)


if __name__ == "__main__":
    run()
