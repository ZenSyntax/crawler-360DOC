"""
文库 HTML 清洗与 Word（.docx）转换。由 wc-library 加载；入口为仓库根或 src 下的 wc-library.py。

清洗产物：clean_<原名>.html；结构为 article 卡片 + 标题区 / 元信息 / 正文 #content（不再保留 #artContent 外壳）。
资源目录与清洗 HTML 主文件名相同（无 .html 后缀）。
"""

from __future__ import annotations

import copy
import os
import random
import re
import shutil
import sys
import tempfile
import time
from collections.abc import Callable
from pathlib import Path
from typing import NamedTuple
from urllib.parse import urljoin, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup
from bs4.element import NavigableString, Tag
from docx import Document
from docx.enum.text import WD_ALIGN_PARAGRAPH, WD_LINE_SPACING, WD_UNDERLINE
from docx.text.paragraph import Paragraph
from docx.opc.constants import RELATIONSHIP_TYPE as RT
from docx.oxml import OxmlElement
from docx.oxml.ns import nsdecls, qn
from docx.oxml.parser import parse_xml
from docx.shared import Inches, Pt, RGBColor

BASE_URL = "http://www.360doc.com"
ALLOWED_HOSTS = ("360doc.com", "360doc.cn")
TIMEOUT = 20
MAX_RETRY = 2
RESOURCE_REQUEST_SLEEP_SEC = (0.2, 0.55)
AFTER_ARTICLE_WITH_RESOURCES_SLEEP_SEC = (0.35, 0.9)
INVALID_NAME_RE = re.compile(r'[<>:"/\\|?*\x00-\x1F]')
DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}")
WORD_META_WORDURL_RE = re.compile(r"wordurl\s*=\s*['\"]([^'\"]+)['\"]", re.I)
WORD_META_PAGENUM_RE = re.compile(r"pageNume\s*=\s*(\d+)", re.I)
WORD_PREVIEW_PAGE_SLEEP_SEC = (0.25, 0.65)
MAX_WORD_PREVIEW_PAGES = 200

# clean_ 前缀：清洗结果 HTML；扫描时排除，与 raw 区分
CLEAN_HTML_PREFIX = "clean_"

# 小三号 ≈ 15pt；五号 ≈ 10.5pt
TITLE_PT = Pt(15)
META_PT = Pt(10.5)
DEFAULT_BODY_PT = Pt(10.5)
# 正文与标题区统一：固定行距 20 磅，段前段后 0
FIXED_LINE_SPACING_PT = Pt(20)

# 本地图片扩展名（src/href 判定与缺失 src 时的补全）
_LOCAL_IMAGE_HREF_EXTS = (
    ".jpg",
    ".jpeg",
    ".png",
    ".gif",
    ".webp",
    ".bmp",
)
# 抓取正文根节点若为下列 id，清洗时展开子节点，避免多包一层 div
_CONTENT_WRAPPER_IDS = frozenset({"artContent", "printArticle"})

CLEAN_ERROR_URL_FILE = Path("clean_error_url.txt")

_log_info = print
_log_warn = print


def set_processer_loggers(log_info_fn, log_warn_fn) -> None:
    global _log_info, _log_warn
    _log_info = log_info_fn
    _log_warn = log_warn_fn


def set_clean_error_url_file(path: Path) -> None:
    global CLEAN_ERROR_URL_FILE
    CLEAN_ERROR_URL_FILE = path


def log_info(msg: str) -> None:
    _log_info(msg)


def log_warn(msg: str) -> None:
    _log_warn(msg)


def append_clean_error_url_line(line: str) -> None:
    try:
        CLEAN_ERROR_URL_FILE.parent.mkdir(parents=True, exist_ok=True)
        with CLEAN_ERROR_URL_FILE.open("a", encoding="utf-8") as fp:
            fp.write(f"{line}\n")
    except Exception as exc:
        log_warn(f"写入 clean_error_url.txt 失败 line={line!r} err={exc}")


def sanitize_name(name: str, fallback: str) -> str:
    name = INVALID_NAME_RE.sub("_", name).strip().rstrip(".")
    return name or fallback


def should_skip_html(path: Path) -> bool:
    n = path.name.lower()
    if not n.endswith(".html"):
        return False
    return n.startswith(CLEAN_HTML_PREFIX.lower())


def _is_html_inside_clean_resource_subdir(path: Path) -> bool:
    # 位于 clean_<stem>/ 资源目录内的 .html 为下载碎片，不参与文库文章扫描。
    for parent in path.parents:
        pn = parent.name
        if pn.startswith(CLEAN_HTML_PREFIX) and not pn.lower().endswith(".html"):
            return True
    return False


def iter_library_article_html_files(root: Path) -> list[Path]:
    # 待处理的文库文章 HTML：数字 id 开头的 raw，或「仅有 clean_、无同名 raw」的孤儿清洗文件。
    # 排除 clean_* 资源子目录内的页面（如 res_2.html）。
    out: list[Path] = []
    pfx = CLEAN_HTML_PREFIX
    lpfx = pfx.lower()
    for p in root.rglob("*.html"):
        if not p.is_file() or _is_html_inside_clean_resource_subdir(p):
            continue
        name = p.name
        ln = name.lower()
        if ln.startswith(lpfx):
            raw_name = name[len(pfx) :]
            if p.with_name(raw_name).is_file():
                continue
            out.append(p)
            continue
        if re.match(r"^\d+-", name):
            out.append(p)
    out.sort()
    return out


def normalize_url(raw: str, base_url: str) -> str:
    if not raw:
        return ""
    raw = raw.strip()
    if raw.startswith(("javascript:", "#", "mailto:", "tel:", "data:")):
        return ""
    if raw.startswith("//"):
        return "http:" + raw
    return urljoin(base_url, raw)


def is_localizable_url(url: str) -> bool:
    if not url:
        return False
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        return False
    host = parsed.netloc.lower()
    return any(host.endswith(domain) for domain in ALLOWED_HOSTS)


def request_with_retry(
    session: requests.Session, url: str, headers: dict | None = None
) -> requests.Response:
    last_exc: Exception | None = None
    for _ in range(MAX_RETRY + 1):
        try:
            resp = session.get(url, timeout=TIMEOUT, headers=headers)
            resp.raise_for_status()
            return resp
        except Exception as exc:
            last_exc = exc
    assert last_exc is not None
    raise last_exc


def extract_article_meta(soup: BeautifulSoup) -> tuple[str, str, str]:
    title = ""
    title_node = soup.select_one("#GLTitile")
    if title_node:
        title = title_node.get_text(strip=True)
    if not title:
        h1 = soup.select_one("h1#titiletext")
        if h1:
            title = h1.get_text(" ", strip=True)
    if not title and soup.title:
        title = soup.title.get_text(strip=True)

    author = ""
    author_node = soup.select_one("#savernickname")
    if author_node:
        author = author_node.get_text(strip=True)

    publish_date = ""
    top_data = soup.select_one(".art_topdata")
    if top_data:
        date_match = DATE_RE.search(top_data.get_text(" ", strip=True))
        if date_match:
            publish_date = date_match.group(0)

    return title or "无标题", author or "未知发布者", publish_date or ""


def extract_body_tag_standard(soup: BeautifulSoup) -> Tag | None:
    content_node = soup.select_one("#artContent")
    if content_node is not None:
        return content_node
    n = soup.select_one("#printArticle")
    if n is not None:
        return n
    return soup.select_one("#content")


def parse_word_document_meta(raw_html: str) -> tuple[str, int] | None:
    m_url = WORD_META_WORDURL_RE.search(raw_html)
    m_pages = WORD_META_PAGENUM_RE.search(raw_html)
    if not m_url or not m_pages:
        return None
    base = m_url.group(1).strip().rstrip("/")
    n = int(m_pages.group(1))
    if n < 1 or n > MAX_WORD_PREVIEW_PAGES:
        return None
    return base, n


def fetch_word_preview_body(
    session: requests.Session,
    word_base: str,
    page_count: int,
    source_url: str,
    article_id: str,
) -> Tag | None:
    wrapper_soup = BeautifulSoup(
        '<div class="word-document-preview"></div>', "html.parser"
    )
    wrapper = wrapper_soup.div
    if wrapper is None:
        return None

    ua = session.headers.get("User-Agent", "Mozilla/5.0")
    headers = {"Referer": source_url, "User-Agent": ua}
    any_page_ok = False

    for p in range(1, page_count + 1):
        page_url = f"{word_base}_{p}.html"
        page_div = wrapper_soup.new_tag(
            "div", attrs={"class": "word-preview-page", "data-page": str(p)}
        )
        try:
            resp = request_with_retry(session, page_url, headers=headers)
            sub = BeautifulSoup(resp.text, "html.parser")
            for bad in sub.select("script,style,noscript,iframe"):
                bad.decompose()
            src_body = sub.body
            if src_body:
                for ch in list(src_body.children):
                    page_div.append(ch.extract())
            else:
                root = sub.find(["div", "img", "section", "article", "p", "table"])
                if root is not None:
                    page_div.append(root.extract())
            if page_div.get_text(strip=True) or page_div.find(["img", "a", "table"]):
                any_page_ok = True
            wrapper.append(page_div)
        except Exception as exc:
            log_warn(f"Word 预览页拉取失败 art={article_id} url={page_url} err={exc}")
            append_clean_error_url_line(f"{article_id}-{page_url}-{exc}")
            wrapper.append(page_div)
        time.sleep(random.uniform(*WORD_PREVIEW_PAGE_SLEEP_SEC))

    if not any_page_ok:
        return None
    return wrapper


def build_clean_soup(title: str, author: str, publish_date: str, content_node: Tag) -> BeautifulSoup:
    # 清洗页 DOM：article 卡片 → 标题区 / 元信息 / 正文 #content（共 4 块），正文内不再保留 artContent 外壳。
    clean_html = f"""<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{title}</title>
  <style>
    body{{margin:0;padding:12px 14px;font-family:Arial,"Microsoft YaHei",sans-serif;line-height:1.75;background:#f3f4f6;}}
    article.doc360-card{{max-width:980px;margin:0 auto;background:#fff;border-radius:0.5px;box-shadow:0 2px 14px rgba(0,0,0,0.07);padding:22px 24px 28px;box-sizing:border-box;display:flex;flex-direction:column;align-items:stretch;}}
    .doc360-title{{align-self:center;width:fit-content;max-width:100%;text-align:center;}}
    .doc360-title h1{{margin:0;font-size:30px;line-height:1.35;font-weight:normal;}}
    .doc360-meta{{align-self:center;width:fit-content;max-width:100%;margin:10px 0 18px;color:#555;font-size:14px;text-align:center;}}
    #content.doc360-main{{width:100%;max-width:100%;box-sizing:border-box;}}
    #content.doc360-main img{{max-width:100%;height:auto;display:block;margin-left:auto;margin-right:auto;}}
    #content.doc360-main table{{max-width:100%;margin:0;}}
  </style>
</head>
<body>
  <article class="doc360-card">
    <div class="doc360-title"><h1 id="title"></h1></div>
    <div class="doc360-meta"><span id="author"></span> <span id="date"></span></div>
    <div class="doc360-main" id="content"></div>
  </article>
</body>
</html>"""
    out = BeautifulSoup(clean_html, "html.parser")
    t_el = out.select_one("#title")
    if t_el:
        t_el.string = title
    a_el = out.select_one("#author")
    if a_el:
        a_el.string = author
    d_el = out.select_one("#date")
    if d_el:
        d_el.string = publish_date
    content_target = out.select_one("#content")
    if content_target is None:
        raise ValueError("清洗模板缺少 #content")
    content_copy = BeautifulSoup(str(content_node), "html.parser")
    root_content = content_copy.find(True) or content_copy
    for bad in root_content.select("script,style,iframe,noscript"):
        bad.decompose()
    if isinstance(root_content, Tag) and (root_content.name or "").lower() in ("td", "th"):
        root_content.name = "div"
    if isinstance(root_content, Tag):
        rid = (root_content.get("id") or "").strip()
        if rid in _CONTENT_WRAPPER_IDS:
            for ch in list(root_content.children):
                content_target.append(ch)
        else:
            content_target.append(root_content)
    return out


_IMG_PLACEHOLDER_SRC_RE = re.compile(
    r"(?:space|blank|spacer|transparent|1x1)\.(?:gif|png)|pixel\.gif$", re.I
)


def _prefer_working_360doc_image_host(url: str) -> str:
    # 360doc 存图：data360-src 常为 checki*.360doc.com，直链 GET 会跳 gohost 404；image*. 同路径可下。
    if not url:
        return url
    try:
        parts = urlparse(url)
        host = (parts.netloc or "").lower()
        if "checki" in host and host.endswith("360doc.com"):
            new_netloc = host.replace("checki", "image", 1)
            return urlunparse(
                (
                    parts.scheme or "http",
                    new_netloc,
                    parts.path,
                    parts.params,
                    parts.query,
                    parts.fragment,
                )
            )
    except Exception:
        pass
    return url


def _img_delegated_to_parent_download_anchor(img: Tag) -> bool:
    # 父级 <a href> 已为 360doc 正文图链（image* + DownloadImg）时跳过 img 节点，避免 data360-src（checki*）重复请求失败。
    p = img.parent
    if not isinstance(p, Tag) or (p.name or "").lower() != "a":
        return False
    href = str(p.get("href", "")).strip()
    if not href or href.startswith(("javascript:", "#")):
        return False
    hlow = href.lower()
    return "360doc.com" in hlow and "downloadimg" in hlow


def _img_download_attr_name(tag: Tag) -> str | None:
    # 优先可用 src，再懒加载属性；data360-src 放后（常与 href 域名不一致）。
    for attr in ("src", "data-src", "data-original", "data360-src"):
        raw = str(tag.get(attr, "")).strip()
        if not raw or raw.startswith("data:"):
            continue
        if attr == "src" and _IMG_PLACEHOLDER_SRC_RE.search(raw):
            continue
        return attr
    return None


def collect_resource_nodes(soup: BeautifulSoup) -> list[tuple[Tag, str, str]]:
    # (tag, 读取远程 URL 的属性名, 写入本地化相对路径的属性名)。img 统一写入 src 以便浏览器与 Word 加载。
    nodes: list[tuple[Tag, str, str]] = []
    root = soup.select_one("#content")
    if root is None:
        return nodes
    for tag in root.find_all(["a", "img", "source"], recursive=True):
        if not isinstance(tag, Tag):
            continue
        name = (tag.name or "").lower()
        if name == "a" and tag.has_attr("href"):
            nodes.append((tag, "href", "href"))
        elif name == "source" and tag.has_attr("src"):
            nodes.append((tag, "src", "src"))
        elif name == "img":
            if _img_delegated_to_parent_download_anchor(tag):
                continue
            ra = _img_download_attr_name(tag)
            if ra:
                nodes.append((tag, ra, "src"))
    return nodes


def suffix_from_url(url: str, fallback: str) -> str:
    path = urlparse(url).path
    if "." in path:
        ext = path.rsplit(".", 1)[-1].lower()
        ext = re.sub(r"[^a-z0-9]", "", ext)
        if ext:
            return "." + ext[:8]
    return fallback


def localize_resources(
    clean_soup: BeautifulSoup,
    source_url: str,
    clean_output_path: Path,
    session: requests.Session,
    *,
    article_id: str,
) -> int:
    resource_nodes = collect_resource_nodes(clean_soup)
    if not resource_nodes:
        return 0

    plan: list[tuple[Tag, str, str, str]] = []
    for tag, read_attr, write_attr in resource_nodes:
        raw = str(tag.get(read_attr, "")).strip()
        abs_url = normalize_url(raw, source_url)
        abs_url = _prefer_working_360doc_image_host(abs_url)
        if not is_localizable_url(abs_url):
            continue
        plan.append((tag, read_attr, write_attr, abs_url))

    if not plan:
        return 0

    res_dir = clean_output_path.with_suffix("")
    downloaded = 0
    url_to_local: dict[str, str] = {}
    # 按「首次出现的可下载 URL」连续编号 res_1、res_2…，与 plan 下标脱钩。
    # 否则同一 URL 在多个 <a>/<img> 上重复时，用 enumerate 会得到 res_4、res_7 等跳号，
    # 且遇已存在文件时生成 res_1_1.jpg，易与残留 res_1.jpg 错位，浏览器与 Word 均无法加载。
    file_seq = 0
    for tag, read_attr, write_attr, abs_url in plan:
        if abs_url in url_to_local:
            tag[write_attr] = url_to_local[abs_url]
            continue
        try:
            resp = request_with_retry(
                session,
                abs_url,
                headers={
                    "Referer": source_url,
                    "User-Agent": session.headers.get("User-Agent", "Mozilla/5.0"),
                },
            )
            if not res_dir.exists():
                res_dir.mkdir(parents=True, exist_ok=True)
            fallback_ext = ".html" if write_attr == "href" else ".bin"
            ext = suffix_from_url(abs_url, fallback_ext)
            file_seq += 1
            local_name = sanitize_name(
                f"res_{file_seq}{ext}", f"res_{file_seq}{fallback_ext}"
            )
            local_path = res_dir / local_name
            local_path.write_bytes(resp.content)
            rel_ref = f"{res_dir.name}/{local_name}"
            tag[write_attr] = rel_ref
            url_to_local[abs_url] = rel_ref
            downloaded += 1
        except Exception as exc:
            log_warn(f"资源下载失败 {abs_url} err={exc}")
            append_clean_error_url_line(f"{article_id}-{abs_url}-{exc}")
        finally:
            time.sleep(random.uniform(*RESOURCE_REQUEST_SLEEP_SEC))
    _heal_imgs_missing_src_from_parent_anchor(clean_soup)
    return downloaded


def _heal_imgs_missing_src_from_parent_anchor(soup: BeautifulSoup) -> None:
    # img 无 src 时从外层 <a href=本地图> 补 src。
    root = soup.select_one("#content")
    if root is None:
        return
    for img in root.find_all("img"):
        if not isinstance(img, Tag):
            continue
        raw = str(img.get("src", "")).strip()
        if raw and not raw.startswith("data:"):
            continue
        p = img.parent
        if not isinstance(p, Tag) or (p.name or "").lower() != "a":
            continue
        h = str(p.get("href", "")).strip()
        hl = h.lower()
        if h and any(hl.endswith(ext) for ext in _LOCAL_IMAGE_HREF_EXTS):
            img["src"] = h


def extract_article_id(path: Path) -> str:
    match = re.match(r"(\d+)-", path.name)
    if match:
        return match.group(1)
    return "unknown"


def guess_source_url(path: Path) -> str:
    artid = extract_article_id(path)
    if artid != "unknown":
        return f"{BASE_URL}/showweb/0/0/{artid}.aspx"
    return BASE_URL


def _parse_css_color(s: str) -> RGBColor | None:
    s = s.strip()
    m = re.search(r"#([0-9a-fA-F]{3}|[0-9a-fA-F]{6})\b", s)
    if m:
        h = m.group(1)
        if len(h) == 3:
            r, g, b = (int(h[i] + h[i], 16) for i in range(3))
        else:
            r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
        return RGBColor(r, g, b)
    m = re.search(
        r"rgba?\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)", s, re.I
    )
    if m:
        return RGBColor(int(m.group(1)), int(m.group(2)), int(m.group(3)))
    return None


def _parse_css_font_size_pt(style: str) -> float | None:
    m = re.search(r"font-size\s*:\s*([\d.]+)\s*(px|pt)", style, re.I)
    if not m:
        return None
    val = float(m.group(1))
    unit = m.group(2).lower()
    if unit == "pt":
        return val
    if unit == "px":
        return val * 0.75
    return None


def _set_run_east_asia(run, font_name: str) -> None:
    run.font.name = font_name
    r = run._element.rPr
    if r is not None and r.rFonts is not None:
        r.rFonts.set(qn("w:eastAsia"), font_name)


def _init_doc_typography(doc: Document) -> None:
    normal = doc.styles["Normal"]
    npf = normal.paragraph_format
    npf.space_before = Pt(0)
    npf.space_after = Pt(0)
    npf.line_spacing_rule = WD_LINE_SPACING.EXACTLY
    npf.line_spacing = FIXED_LINE_SPACING_PT


def _apply_body_paragraph_format(p) -> None:
    pf = p.paragraph_format
    pf.space_before = Pt(0)
    pf.space_after = Pt(0)
    pf.line_spacing_rule = WD_LINE_SPACING.EXACTLY
    pf.line_spacing = FIXED_LINE_SPACING_PT


def _paragraph_has_visible_content(p: Paragraph) -> bool:
    raw = (p.text or "").replace("\xa0", " ").replace("\u200b", "")
    if raw:
        return True
    for run in p.runs:
        el = run._element
        if el.find(qn("w:drawing")) is not None:
            return True
        if el.find(qn("w:pict")) is not None:
            return True
    return False


def _remove_empty_paragraphs(doc: Document) -> None:
    body_el = doc._body._body
    for child in list(body_el):
        if child.tag != qn("w:p"):
            continue
        para = Paragraph(child, doc._body)
        if not _paragraph_has_visible_content(para):
            body_el.remove(child)


def _apply_paragraph_format_in_table(tbl) -> None:
    for row in tbl.rows:
        for cell in row.cells:
            for p in cell.paragraphs:
                _apply_body_paragraph_format(p)
            for nested in cell.tables:
                _apply_paragraph_format_in_table(nested)


def _apply_body_paragraph_format_to_all(doc: Document) -> None:
    for p in doc.paragraphs:
        _apply_body_paragraph_format(p)
    for tbl in doc.tables:
        _apply_paragraph_format_in_table(tbl)


class _RunCtx(NamedTuple):
    bold: bool = False
    italic: bool = False
    underline: bool = False
    strike: bool = False
    subscript: bool = False
    superscript: bool = False
    color: RGBColor | None = None
    font_pt: float | None = None
    font_name: str | None = None


_LINK_BLUE = RGBColor(0x05, 0x63, 0xC1)


def _rgb_word_hex(rgb: RGBColor) -> str:
    return "%02X%02X%02X" % (rgb[0], rgb[1], rgb[2])


def _apply_ctx_to_run(run, ctx: _RunCtx, default_pt: Pt) -> None:
    fn = ctx.font_name or "等线"
    _set_run_east_asia(run, fn)
    if ctx.font_pt is not None:
        run.font.size = Pt(ctx.font_pt)
    else:
        run.font.size = default_pt
    run.bold = ctx.bold
    run.italic = ctx.italic
    run.font.underline = (
        WD_UNDERLINE.SINGLE if ctx.underline else WD_UNDERLINE.NONE
    )
    run.font.strike = ctx.strike
    run.font.subscript = bool(ctx.subscript)
    run.font.superscript = bool(ctx.superscript)
    if ctx.color is not None:
        run.font.color.rgb = ctx.color


def _span_ctx_from_tag(tag: Tag, base: _RunCtx) -> _RunCtx:
    st = str(tag.get("style") or "")
    c = _parse_css_color(st)
    fs = _parse_css_font_size_pt(st)
    if c is None and tag.has_attr("color"):
        co = str(tag.get("color", ""))
        if co.startswith("#"):
            c = _parse_css_color(co)
    if c is not None or fs is not None:
        return base._replace(
            color=c if c is not None else base.color,
            font_pt=fs if fs is not None else base.font_pt,
        )
    return base


def _append_ox_text_run(
    ox_parent,
    text: str,
    ctx: _RunCtx,
    default_pt: Pt,
    *,
    link_style: bool,
) -> None:
    r = OxmlElement("w:r")
    r_pr = OxmlElement("w:rPr")
    if ctx.bold:
        r_pr.append(OxmlElement("w:b"))
    if ctx.italic:
        r_pr.append(OxmlElement("w:i"))
    color = ctx.color
    if color is None and link_style:
        color = _LINK_BLUE
    if color is not None:
        co = OxmlElement("w:color")
        co.set(qn("w:val"), _rgb_word_hex(color))
        r_pr.append(co)
    if ctx.underline or link_style:
        u = OxmlElement("w:u")
        u.set(qn("w:val"), "single")
        r_pr.append(u)
    if ctx.strike:
        r_pr.append(OxmlElement("w:strike"))
    if ctx.subscript:
        va = OxmlElement("w:vertAlign")
        va.set(qn("w:val"), "subscript")
        r_pr.append(va)
    if ctx.superscript:
        va = OxmlElement("w:vertAlign")
        va.set(qn("w:val"), "superscript")
        r_pr.append(va)
    pt_val = ctx.font_pt if ctx.font_pt is not None else float(default_pt.pt)
    half = str(int(round(pt_val * 2)))
    sz = OxmlElement("w:sz")
    sz.set(qn("w:val"), half)
    r_pr.append(sz)
    sz_cs = OxmlElement("w:szCs")
    sz_cs.set(qn("w:val"), half)
    r_pr.append(sz_cs)
    r_fonts = OxmlElement("w:rFonts")
    fn = ctx.font_name or "等线"
    r_fonts.set(qn("w:ascii"), fn)
    r_fonts.set(qn("w:hAnsi"), fn)
    r_fonts.set(qn("w:eastAsia"), fn)
    r_pr.append(r_fonts)
    r.append(r_pr)
    t_el = OxmlElement("w:t")
    t_el.text = text
    t_el.set("{http://www.w3.org/XML/1998/namespace}space", "preserve")
    r.append(t_el)
    ox_parent.append(r)


def _walk_inline_to_hyperlink_ox(
    h_el,
    node,
    ctx: _RunCtx,
    default_pt: Pt,
    *,
    link_style: bool,
    br_state: list[bool] | None = None,
) -> None:
    if br_state is None:
        br_state = [False]
    if isinstance(node, NavigableString):
        t = _normalize_navigable_text_for_docx(str(node))
        if not t:
            return
        if not t.strip():
            if "\n" in t or "\r" in t:
                return
            br_state[0] = False
            _append_ox_text_run(h_el, t, ctx, default_pt, link_style=link_style)
            return
        br_state[0] = False
        _append_ox_text_run(h_el, t, ctx, default_pt, link_style=link_style)
        return
    if not isinstance(node, Tag):
        return
    name = (node.name or "").lower()
    if name == "br":
        if br_state[0]:
            return
        h_el.append(OxmlElement("w:br"))
        br_state[0] = True
        return
    if name in ("strong", "b"):
        for ch in node.children:
            _walk_inline_to_hyperlink_ox(
                h_el,
                ch,
                ctx._replace(bold=True),
                default_pt,
                link_style=link_style,
                br_state=br_state,
            )
        return
    if name in ("em", "i"):
        for ch in node.children:
            _walk_inline_to_hyperlink_ox(
                h_el,
                ch,
                ctx._replace(italic=True),
                default_pt,
                link_style=link_style,
                br_state=br_state,
            )
        return
    if name == "u":
        for ch in node.children:
            _walk_inline_to_hyperlink_ox(
                h_el,
                ch,
                ctx._replace(underline=True),
                default_pt,
                link_style=link_style,
                br_state=br_state,
            )
        return
    if name in ("s", "strike", "del"):
        for ch in node.children:
            _walk_inline_to_hyperlink_ox(
                h_el,
                ch,
                ctx._replace(strike=True),
                default_pt,
                link_style=link_style,
                br_state=br_state,
            )
        return
    if name == "sub":
        for ch in node.children:
            _walk_inline_to_hyperlink_ox(
                h_el,
                ch,
                ctx._replace(subscript=True, superscript=False),
                default_pt,
                link_style=link_style,
                br_state=br_state,
            )
        return
    if name == "sup":
        for ch in node.children:
            _walk_inline_to_hyperlink_ox(
                h_el,
                ch,
                ctx._replace(superscript=True, subscript=False),
                default_pt,
                link_style=link_style,
                br_state=br_state,
            )
        return
    if name in ("span", "font"):
        nc = _span_ctx_from_tag(node, ctx)
        for ch in node.children:
            _walk_inline_to_hyperlink_ox(
                h_el, ch, nc, default_pt, link_style=link_style, br_state=br_state
            )
        return
    if name in ("code",):
        for ch in node.children:
            _walk_inline_to_hyperlink_ox(
                h_el,
                ch,
                ctx._replace(font_name="Consolas"),
                default_pt,
                link_style=link_style,
                br_state=br_state,
            )
        return
    for ch in node.children:
        _walk_inline_to_hyperlink_ox(
            h_el, ch, ctx, default_pt, link_style=link_style, br_state=br_state
        )


# 浮动图 z-order，避免多个 anchor 相对高度重复导致叠盖
_ANCHOR_RELATIVE_HEIGHT_NEXT = 251_658_240


def _next_anchor_relative_height() -> int:
    global _ANCHOR_RELATIVE_HEIGHT_NEXT
    _ANCHOR_RELATIVE_HEIGHT_NEXT += 1
    return _ANCHOR_RELATIVE_HEIGHT_NEXT


def _strip_line_breaks_after_drawing_in_run(run) -> None:
    # 去掉紧跟在图片后的软换行，避免与上下型环绕叠加产生多余空行。
    r_el = run._element
    seen_drawing = False
    for child in list(r_el):
        if child.tag == qn("w:drawing"):
            seen_drawing = True
            continue
        if seen_drawing and child.tag == qn("w:br"):
            r_el.remove(child)


def _convert_run_inline_picture_to_top_bottom_wrap(run) -> None:
    # 将 add_picture 生成的 wp:inline 改为 wp:anchor，并设为上下型文字环绕（类似 Word「上下型」）。
    r_el = run._element
    drawing = r_el.find(qn("w:drawing"))
    if drawing is None:
        return
    inline = drawing.find(qn("wp:inline"))
    if inline is None:
        return
    extent = inline.find(qn("wp:extent"))
    effect = inline.find(qn("wp:effectExtent"))
    doc_pr = inline.find(qn("wp:docPr"))
    cnv = inline.find(qn("wp:cNvGraphicFramePr"))
    graphic = inline.find(qn("a:graphic"))
    if extent is None or graphic is None or doc_pr is None:
        return

    z = _next_anchor_relative_height()
    extent_c = copy.deepcopy(extent)
    if effect is not None:
        effect_c = copy.deepcopy(effect)
    else:
        effect_c = parse_xml(
            '<wp:effectExtent %s l="0" t="0" r="0" b="0"/>' % nsdecls("wp")
        )
    doc_pr_c = copy.deepcopy(doc_pr)
    cnv_c = copy.deepcopy(cnv) if cnv is not None else None
    graphic_c = copy.deepcopy(graphic)

    decl = nsdecls("wp", "a", "pic", "r")
    anchor = parse_xml(
        "<wp:anchor %s distT=\"0\" distB=\"0\" distL=\"0\" distR=\"0\" "
        'simplePos="0" relativeHeight="%d" behindDoc="0" locked="0" '
        'layoutInCell="1" allowOverlap="0"></wp:anchor>' % (decl, z)
    )
    sp = parse_xml('<wp:simplePos %s x="0" y="0"/>' % nsdecls("wp"))
    pos_h = parse_xml(
        "<wp:positionH %s relativeFrom=\"column\">"
        "<wp:align>center</wp:align></wp:positionH>" % nsdecls("wp")
    )
    pos_v = parse_xml(
        "<wp:positionV %s relativeFrom=\"paragraph\">"
        "<wp:posOffset>0</wp:posOffset></wp:positionV>" % nsdecls("wp")
    )
    wrap_tb = parse_xml("<wp:wrapTopAndBottom %s/>" % nsdecls("wp"))

    anchor.append(sp)
    anchor.append(pos_h)
    anchor.append(pos_v)
    anchor.append(extent_c)
    anchor.append(effect_c)
    anchor.append(wrap_tb)
    anchor.append(doc_pr_c)
    if cnv_c is not None:
        anchor.append(cnv_c)
    anchor.append(graphic_c)

    drawing.clear()
    drawing.append(anchor)


def _add_picture_top_bottom_wrap(paragraph, image_path: str, width) -> None:
    run = paragraph.add_run()
    run.add_picture(str(image_path), width=width)
    _convert_run_inline_picture_to_top_bottom_wrap(run)
    _strip_line_breaks_after_drawing_in_run(run)


def _paragraph_add_external_hyperlink(
    paragraph,
    url: str,
    anchor: Tag,
    ctx: _RunCtx,
    default_pt: Pt,
    *,
    br_state: list[bool] | None = None,
) -> None:
    if br_state is None:
        br_state = [False]
    part = paragraph.part
    r_id = part.relate_to(url, RT.HYPERLINK, is_external=True)
    h_el = OxmlElement("w:hyperlink")
    h_el.set(qn("r:id"), r_id)
    paragraph._p.append(h_el)
    _walk_inline_to_hyperlink_ox(
        h_el, anchor, ctx, default_pt, link_style=True, br_state=br_state
    )


def _add_inline_image_to_paragraph(
    paragraph,
    img: Tag,
    base_dir: Path,
    *,
    article_clean_html: Path | None = None,
    br_state: list[bool] | None = None,
) -> None:
    if br_state is not None:
        br_state[0] = False
    src = _img_src_for_local_resolve(img)
    local = _resolve_local_media_path(
        src, base_dir, article_clean_html=article_clean_html
    )
    if local and local.is_file():
        try:
            _add_picture_top_bottom_wrap(paragraph, str(local), Inches(5.5))
        except Exception as exc:
            log_warn(f"插入图片失败 {local}: {exc}")
            r2 = paragraph.add_run(f"[图片 {src}]")
            _set_run_east_asia(r2, "等线")
            r2.font.size = DEFAULT_BODY_PT
    else:
        r2 = paragraph.add_run(f"[图片 {src}]")
        _set_run_east_asia(r2, "等线")


def _tag_class_list(tag: Tag) -> list[str]:
    c = tag.get("class") or []
    if isinstance(c, str):
        return c.split()
    return list(c)


def _normalize_navigable_text_for_docx(raw: str) -> str:
    # 压缩站点里常见的 NBSP/空格填充，避免 Word 里出现「每页一行」的假稀疏排版。
    if not raw:
        return raw
    t = raw.replace("\u00a0", " ").replace("\u2003", " ").replace("\u2002", " ")
    t = re.sub(r"[ \t]+", " ", t)
    t = re.sub(r" +\n", "\n", t)
    t = re.sub(r"\n{3,}", "\n\n", t)
    return t


def _walk_inline_to_paragraph(
    paragraph,
    node,
    ctx: _RunCtx,
    default_pt: Pt,
    *,
    base_dir: Path | None = None,
    article_clean_html: Path | None = None,
    br_state: list[bool] | None = None,
    local_media_href_dedupe: set[str] | None = None,
) -> None:
    if br_state is None:
        br_state = [False]

    def _w(ch, cctx: _RunCtx) -> None:
        _walk_inline_to_paragraph(
            paragraph,
            ch,
            cctx,
            default_pt,
            base_dir=base_dir,
            article_clean_html=article_clean_html,
            br_state=br_state,
            local_media_href_dedupe=local_media_href_dedupe,
        )

    if isinstance(node, NavigableString):
        t = _normalize_navigable_text_for_docx(str(node))
        if not t:
            return
        if not t.strip():
            if "\n" in t or "\r" in t:
                return
            br_state[0] = False
            run = paragraph.add_run(t)
            _apply_ctx_to_run(run, ctx, default_pt)
            return
        br_state[0] = False
        run = paragraph.add_run(t)
        _apply_ctx_to_run(run, ctx, default_pt)
        return
    if not isinstance(node, Tag):
        return
    name = (node.name or "").lower()
    if name == "br":
        if br_state[0]:
            return
        paragraph.add_run().add_break()
        br_state[0] = True
        return
    if name == "img":
        if base_dir is not None:
            src = _img_src_for_local_resolve(node)
            key = src.replace("\\", "/") if src else ""
            if key and local_media_href_dedupe is not None:
                if key in local_media_href_dedupe:
                    return
                local_media_href_dedupe.add(key)
            _add_inline_image_to_paragraph(
                paragraph,
                node,
                base_dir,
                article_clean_html=article_clean_html,
                br_state=br_state,
            )
        return
    if name in ("strong", "b"):
        for ch in node.children:
            _w(ch, ctx._replace(bold=True))
        return
    if name in ("em", "i"):
        for ch in node.children:
            _w(ch, ctx._replace(italic=True))
        return
    if name == "u":
        for ch in node.children:
            _w(ch, ctx._replace(underline=True))
        return
    if name in ("s", "strike", "del"):
        for ch in node.children:
            _w(ch, ctx._replace(strike=True))
        return
    if name == "sub":
        for ch in node.children:
            _w(ch, ctx._replace(subscript=True, superscript=False))
        return
    if name == "sup":
        for ch in node.children:
            _w(ch, ctx._replace(superscript=True, subscript=False))
        return
    if name == "a":
        href = str(node.get("href") or "").strip()
        hl = href.lower()
        is_local_image_href = (
            base_dir is not None
            and href
            and not href.startswith(("#", "//"))
            and not hl.startswith(
                ("javascript:", "mailto:", "tel:", "http://", "https://", "data:")
            )
            and any(hl.endswith(ext) for ext in _LOCAL_IMAGE_HREF_EXTS)
        )
        if is_local_image_href:
            if node.find("img"):
                for ch in node.children:
                    _w(ch, ctx)
                return
            key = href.replace("\\", "/")
            if local_media_href_dedupe is not None and key in local_media_href_dedupe:
                return
            if local_media_href_dedupe is not None:
                local_media_href_dedupe.add(key)
            local = _resolve_local_media_path(
                href, base_dir, article_clean_html=article_clean_html
            )
            if local and local.is_file():
                if br_state is not None:
                    br_state[0] = False
                try:
                    _add_picture_top_bottom_wrap(paragraph, str(local), Inches(5.5))
                except Exception as exc:
                    log_warn(f"插入图片失败 {local}: {exc}")
            return
        if href and not href.startswith("#") and not hl.startswith(
            ("javascript:", "mailto:", "tel:")
        ):
            _paragraph_add_external_hyperlink(
                paragraph, href, node, ctx, default_pt, br_state=br_state
            )
        else:
            for ch in node.children:
                _w(ch, ctx)
        return
    if name in ("span", "font"):
        nc = _span_ctx_from_tag(node, ctx)
        for ch in node.children:
            _w(ch, nc)
        return
    if name in ("code",):
        for ch in node.children:
            _w(ch, ctx._replace(font_name="Consolas"))
        return
    if name in ("mark",):
        for ch in node.children:
            _w(ch, ctx)
        return
    if name in ("p", "div", "section", "article", "header", "footer", "center"):
        if "word-preview-page" in _tag_class_list(node):
            if base_dir is not None:
                for im in node.find_all("img"):
                    if isinstance(im, Tag):
                        _add_inline_image_to_paragraph(
                            paragraph,
                            im,
                            base_dir,
                            article_clean_html=article_clean_html,
                            br_state=br_state,
                        )
            return
        for ch in node.children:
            _w(ch, ctx)
        return
    for ch in node.children:
        _w(ch, ctx)


def _add_text_runs_to_paragraph(
    paragraph,
    node,
    *,
    default_pt,
    base_dir: Path | None = None,
    article_clean_html: Path | None = None,
) -> None:
    _walk_inline_to_paragraph(
        paragraph,
        node,
        _RunCtx(),
        default_pt,
        base_dir=base_dir,
        article_clean_html=article_clean_html,
    )


def _img_src_for_local_resolve(img: Tag) -> str:
    # img 无可用 src 时，回退读取外层 <a href> 中的本地图片路径（历史清洗 HTML 可能仅有链接无 src）。
    s = str(img.get("src", "")).strip()
    if s and not s.startswith("data:"):
        return s
    p = img.parent
    if isinstance(p, Tag) and (p.name or "").lower() == "a":
        h = str(p.get("href", "")).strip()
        hl = h.lower()
        if any(hl.endswith(ext) for ext in _LOCAL_IMAGE_HREF_EXTS):
            return h
    return s


def _res_basename_without_collision_suffix(bare: str) -> str | None:
    # res_10_4.jpg -> res_10.jpg（历史避撞命名与主文件并存时供回退查找）。
    m = re.match(r"^(res_\d+)_\d+(\.[^.]+)$", bare, re.I)
    if m:
        return m.group(1) + m.group(2)
    return None


def _media_rel_key_for_dedupe(tag: Tag) -> str | None:
    # 同一段内多个空 <a href=同一本地图> 与带图 <a> 去重，避免 Word 里同一张图插三遍。
    im = tag.find("img")
    if isinstance(im, Tag):
        s = _img_src_for_local_resolve(im)
        if s:
            return s.replace("\\", "/")
    if (tag.name or "").lower() == "a":
        h = str(tag.get("href", "")).strip()
        if not h or h.lower().startswith(("http://", "https://", "data:")):
            return None
        hl = h.lower()
        if any(hl.endswith(ext) for ext in _LOCAL_IMAGE_HREF_EXTS):
            return h.replace("\\", "/")
    return None


def _is_block_media_tag(tag: Tag) -> bool:
    if tag.name and tag.name.lower() in ("img", "video", "audio", "embed", "iframe"):
        return True
    if tag.name and tag.name.lower() == "a":
        href = str(tag.get("href", "")).lower()
        if any(href.endswith(ext) for ext in _LOCAL_IMAGE_HREF_EXTS + (".mp4", ".mp3")):
            return True
        if tag.find("img"):
            return True
    return False


def _path_under_article_base(path: Path, base_resolved: Path) -> bool:
    # 判断 path 是否在 base 目录内；Windows 下 resolve/长短路径可能导致 strict relative_to 失败。
    try:
        path.resolve().relative_to(base_resolved)
        return True
    except ValueError:
        try:
            return os.path.commonpath(
                [os.path.abspath(path), os.path.abspath(base_resolved)]
            ) == os.path.abspath(base_resolved)
        except (ValueError, OSError):
            return False


def _resolve_local_media_path(
    src: str,
    base_dir: Path,
    *,
    article_clean_html: Path | None = None,
) -> Path | None:
    src = src.strip()
    if not src or src.startswith(("http://", "https://", "data:")):
        return None
    base_resolved = base_dir.resolve()
    p = (base_dir / src).resolve()
    if p.is_file() and _path_under_article_base(p, base_resolved):
        return p
    if article_clean_html is None or not article_clean_html.is_file():
        return None
    bare = Path(src.replace("\\", "/")).name
    if not bare:
        return None
    rd = res_dir_for_clean(article_clean_html)
    p2 = (rd / bare).resolve()
    if p2.is_file() and _path_under_article_base(p2, base_resolved):
        return p2
    alt = _res_basename_without_collision_suffix(bare)
    if alt:
        p3 = (rd / alt).resolve()
        if p3.is_file() and _path_under_article_base(p3, base_resolved):
            return p3
    return None


def _append_centered_picture_to_paragraph(
    p,
    im: Tag,
    base_dir: Path,
    *,
    article_clean_html: Path | None,
) -> None:
    src = _img_src_for_local_resolve(im)
    local = _resolve_local_media_path(
        src, base_dir, article_clean_html=article_clean_html
    )
    if local and local.is_file():
        try:
            _add_picture_top_bottom_wrap(p, str(local), Inches(5.5))
        except Exception as exc:
            log_warn(f"插入图片失败 {local}: {exc}")
            r2 = p.add_run(f"[图片 {src}]")
            _set_run_east_asia(r2, "等线")
    else:
        r2 = p.add_run(f"[图片 {src}]")
        _set_run_east_asia(r2, "等线")


def _fill_paragraph_with_media(
    p,
    tag: Tag,
    base_dir: Path,
    *,
    article_clean_html: Path | None = None,
    new_paragraph: Callable[[], Paragraph] | None = None,
) -> None:
    if tag.name and tag.name.lower() == "img":
        imgs = [tag] if isinstance(tag, Tag) else []
    else:
        imgs = [im for im in tag.find_all("img") if isinstance(im, Tag)]
    if not imgs:
        if (tag.name or "").lower() == "a":
            href = str(tag.get("href", "")).strip()
            hl = href.lower()
            if href and not href.startswith(
                ("http://", "https://", "data:")
            ) and any(hl.endswith(ext) for ext in _LOCAL_IMAGE_HREF_EXTS):
                local = _resolve_local_media_path(
                    href, base_dir, article_clean_html=article_clean_html
                )
                if local and local.is_file():
                    p.alignment = WD_ALIGN_PARAGRAPH.CENTER
                    _apply_body_paragraph_format(p)
                    try:
                        _add_picture_top_bottom_wrap(p, str(local), Inches(5.5))
                    except Exception as exc:
                        log_warn(f"插入图片失败 {local}: {exc}")
                        r2 = p.add_run(f"[图片 {href}]")
                        _set_run_east_asia(r2, "等线")
                    return
        p.alignment = WD_ALIGN_PARAGRAPH.CENTER
        _apply_body_paragraph_format(p)
        run = p.add_run(tag.get_text(strip=True) or "[媒体]")
        _set_run_east_asia(run, "等线")
        run.font.size = DEFAULT_BODY_PT
        return

    if len(imgs) > 1 and new_paragraph is not None:
        p.alignment = WD_ALIGN_PARAGRAPH.CENTER
        _apply_body_paragraph_format(p)
        _append_centered_picture_to_paragraph(
            p, imgs[0], base_dir, article_clean_html=article_clean_html
        )
        for im in imgs[1:]:
            np = new_paragraph()
            np.alignment = WD_ALIGN_PARAGRAPH.CENTER
            _apply_body_paragraph_format(np)
            _append_centered_picture_to_paragraph(
                np, im, base_dir, article_clean_html=article_clean_html
            )
        return

    p.alignment = WD_ALIGN_PARAGRAPH.CENTER
    _apply_body_paragraph_format(p)
    for im in imgs:
        _append_centered_picture_to_paragraph(
            p, im, base_dir, article_clean_html=article_clean_html
        )


def _add_media_paragraph(
    doc: Document,
    tag: Tag,
    base_dir: Path,
    *,
    article_clean_html: Path | None = None,
) -> None:
    if tag.name and tag.name.lower() == "img":
        img_list = [tag]
    else:
        img_list = [im for im in tag.find_all("img") if isinstance(im, Tag)]
    if not img_list:
        p = doc.add_paragraph()
        _fill_paragraph_with_media(
            p,
            tag,
            base_dir,
            article_clean_html=article_clean_html,
            new_paragraph=lambda: doc.add_paragraph(),
        )
        return
    for im in img_list:
        p = doc.add_paragraph()
        _fill_paragraph_with_media(
            p, im, base_dir, article_clean_html=article_clean_html
        )


def _paragraph_has_content(p) -> bool:
    return bool((p.text or "").strip()) or len(p.runs) > 0


def _table_row_cells(tr: Tag) -> list[Tag]:
    return [
        c
        for c in tr.children
        if isinstance(c, Tag) and (c.name or "").lower() in ("td", "th")
    ]


def _emit_table_cell_content(
    cell,
    cell_tag: Tag,
    base_dir: Path,
    *,
    article_clean_html: Path | None = None,
) -> None:
    cell.text = ""
    p = cell.paragraphs[0]
    children = list(cell_tag.children)
    for i, ch in enumerate(children):
        if isinstance(ch, NavigableString):
            if str(ch).strip():
                _walk_inline_to_paragraph(
                    p,
                    ch,
                    _RunCtx(),
                    DEFAULT_BODY_PT,
                    base_dir=base_dir,
                    article_clean_html=article_clean_html,
                )
            continue
        if not isinstance(ch, Tag):
            continue
        if _is_block_media_tag(ch):
            if _paragraph_has_content(p):
                p = cell.add_paragraph()
            _fill_paragraph_with_media(
                p,
                ch,
                base_dir,
                article_clean_html=article_clean_html,
                new_paragraph=lambda: cell.add_paragraph(),
            )
            continue
        cn = (ch.name or "").lower()
        if cn in ("p", "div", "section"):
            if i > 0 and _paragraph_has_content(p):
                p.add_run().add_break()
            for c2 in ch.children:
                _walk_inline_to_paragraph(
                    p,
                    c2,
                    _RunCtx(),
                    DEFAULT_BODY_PT,
                    base_dir=base_dir,
                    article_clean_html=article_clean_html,
                )
        else:
            _walk_inline_to_paragraph(
                p,
                ch,
                _RunCtx(),
                DEFAULT_BODY_PT,
                base_dir=base_dir,
                article_clean_html=article_clean_html,
            )


def _emit_html_table(
    doc: Document,
    table_tag: Tag,
    base_dir: Path,
    *,
    article_clean_html: Path | None = None,
) -> None:
    rows = [r for r in table_tag.find_all("tr") if isinstance(r, Tag)]
    if not rows:
        return
    ncols = max((len(_table_row_cells(tr)) for tr in rows), default=1)
    tbl = doc.add_table(rows=0, cols=ncols)
    tbl.style = "Table Grid"
    for tr in rows:
        tags = _table_row_cells(tr)
        if not tags:
            continue
        row = tbl.add_row()
        for ci in range(ncols):
            cell = row.cells[ci]
            if ci < len(tags):
                _emit_table_cell_content(
                    cell, tags[ci], base_dir, article_clean_html=article_clean_html
                )


def _list_indent_for_level(level: int) -> Pt | None:
    if level <= 0:
        return None
    return Pt(min(10 + level * 14, 120))


def _emit_ul(
    doc: Document,
    ul: Tag,
    base_dir: Path,
    *,
    list_level: int = 0,
    article_clean_html: Path | None = None,
) -> None:
    for li in ul.children:
        if not isinstance(li, Tag) or (li.name or "").lower() != "li":
            continue
        p = doc.add_paragraph(style="List Bullet")
        ind = _list_indent_for_level(list_level)
        if ind is not None:
            p.paragraph_format.left_indent = ind
        for sub in li.children:
            if isinstance(sub, NavigableString):
                if str(sub).strip():
                    _walk_inline_to_paragraph(
                        p,
                        sub,
                        _RunCtx(),
                        DEFAULT_BODY_PT,
                        base_dir=base_dir,
                        article_clean_html=article_clean_html,
                    )
                continue
            if not isinstance(sub, Tag):
                continue
            sn = (sub.name or "").lower()
            if sn == "ul":
                _emit_ul(
                    doc,
                    sub,
                    base_dir,
                    list_level=list_level + 1,
                    article_clean_html=article_clean_html,
                )
            elif sn == "ol":
                _emit_ol(
                    doc,
                    sub,
                    base_dir,
                    list_level=list_level + 1,
                    article_clean_html=article_clean_html,
                )
            elif sn in ("p", "div"):
                for c2 in sub.children:
                    _walk_inline_to_paragraph(
                        p,
                        c2,
                        _RunCtx(),
                        DEFAULT_BODY_PT,
                        base_dir=base_dir,
                        article_clean_html=article_clean_html,
                    )
            else:
                _walk_inline_to_paragraph(
                    p,
                    sub,
                    _RunCtx(),
                    DEFAULT_BODY_PT,
                    base_dir=base_dir,
                    article_clean_html=article_clean_html,
                )


def _emit_ol(
    doc: Document,
    ol: Tag,
    base_dir: Path,
    *,
    list_level: int = 0,
    article_clean_html: Path | None = None,
) -> None:
    for li in ol.children:
        if not isinstance(li, Tag) or (li.name or "").lower() != "li":
            continue
        p = doc.add_paragraph(style="List Number")
        ind = _list_indent_for_level(list_level)
        if ind is not None:
            p.paragraph_format.left_indent = ind
        for sub in li.children:
            if isinstance(sub, NavigableString):
                if str(sub).strip():
                    _walk_inline_to_paragraph(
                        p,
                        sub,
                        _RunCtx(),
                        DEFAULT_BODY_PT,
                        base_dir=base_dir,
                        article_clean_html=article_clean_html,
                    )
                continue
            if not isinstance(sub, Tag):
                continue
            sn = (sub.name or "").lower()
            if sn == "ul":
                _emit_ul(
                    doc,
                    sub,
                    base_dir,
                    list_level=list_level + 1,
                    article_clean_html=article_clean_html,
                )
            elif sn == "ol":
                _emit_ol(
                    doc,
                    sub,
                    base_dir,
                    list_level=list_level + 1,
                    article_clean_html=article_clean_html,
                )
            elif sn in ("p", "div"):
                for c2 in sub.children:
                    _walk_inline_to_paragraph(
                        p,
                        c2,
                        _RunCtx(),
                        DEFAULT_BODY_PT,
                        base_dir=base_dir,
                        article_clean_html=article_clean_html,
                    )
            else:
                _walk_inline_to_paragraph(
                    p,
                    sub,
                    _RunCtx(),
                    DEFAULT_BODY_PT,
                    base_dir=base_dir,
                    article_clean_html=article_clean_html,
                )


# td/th/tr 等：当作容器展开子节点，避免 Word 把整段正文压成单段。
_BLOCK_UNWRAP = frozenset(
    {
        "article",
        "main",
        "header",
        "footer",
        "aside",
        "nav",
        "tbody",
        "thead",
        "tfoot",
        "hgroup",
        "center",
        "td",
        "th",
        "tr",
    }
)


def _emit_content_node(
    doc: Document,
    node: Tag,
    base_dir: Path,
    *,
    list_level: int = 0,
    article_clean_html: Path | None = None,
) -> None:
    name = (node.name or "").lower()
    if name in ("script", "style"):
        return
    if name == "br":
        return
    wprev = node.select_one(".word-document-preview")
    if wprev is not None:
        for img in wprev.find_all("img"):
            if isinstance(img, Tag):
                _add_media_paragraph(
                    doc, img, base_dir, article_clean_html=article_clean_html
                )
        return
    if name in ("div", "section") and "word-preview-page" in _tag_class_list(node):
        for img in node.find_all("img"):
            if isinstance(img, Tag):
                _add_media_paragraph(
                    doc, img, base_dir, article_clean_html=article_clean_html
                )
        return
    if name in _BLOCK_UNWRAP:
        for ch in node.children:
            if isinstance(ch, NavigableString):
                if str(ch).strip():
                    bp = doc.add_paragraph()
                    _walk_inline_to_paragraph(
                        bp,
                        ch,
                        _RunCtx(),
                        DEFAULT_BODY_PT,
                        base_dir=base_dir,
                        article_clean_html=article_clean_html,
                    )
            elif isinstance(ch, Tag):
                _emit_content_node(
                    doc,
                    ch,
                    base_dir,
                    list_level=list_level,
                    article_clean_html=article_clean_html,
                )
        return
    if _is_block_media_tag(node):
        _add_media_paragraph(
            doc, node, base_dir, article_clean_html=article_clean_html
        )
        return
    if name in ("h1", "h2", "h3", "h4", "h5", "h6"):
        lvl = int(name[1])
        style = f"Heading {min(lvl, 9)}"
        hp = doc.add_paragraph(style=style)
        for ch in node.children:
            _walk_inline_to_paragraph(
                hp,
                ch,
                _RunCtx(),
                DEFAULT_BODY_PT,
                base_dir=base_dir,
                article_clean_html=article_clean_html,
            )
        return
    if name == "ul":
        _emit_ul(
            doc,
            node,
            base_dir,
            list_level=list_level,
            article_clean_html=article_clean_html,
        )
        return
    if name == "ol":
        _emit_ol(
            doc,
            node,
            base_dir,
            list_level=list_level,
            article_clean_html=article_clean_html,
        )
        return
    if name == "table":
        _emit_html_table(
            doc, node, base_dir, article_clean_html=article_clean_html
        )
        return
    if name == "hr":
        hp = doc.add_paragraph()
        _apply_body_paragraph_format(hp)
        r = hp.add_run("―" * 28)
        _set_run_east_asia(r, "等线")
        r.font.size = DEFAULT_BODY_PT
        r.font.color.rgb = RGBColor(0x99, 0x99, 0x99)
        return
    if name == "blockquote":
        for ch in node.children:
            if isinstance(ch, NavigableString):
                if str(ch).strip():
                    bp = doc.add_paragraph()
                    bp.paragraph_format.left_indent = Inches(0.28)
                    _walk_inline_to_paragraph(
                        bp,
                        ch,
                        _RunCtx(),
                        DEFAULT_BODY_PT,
                        base_dir=base_dir,
                        article_clean_html=article_clean_html,
                    )
            elif isinstance(ch, Tag):
                cn = (ch.name or "").lower()
                if cn in ("p", "div", "section"):
                    if cn in ("div", "section") and "word-preview-page" in _tag_class_list(
                        ch
                    ):
                        for im in ch.find_all("img"):
                            if isinstance(im, Tag):
                                _add_media_paragraph(
                                    doc,
                                    im,
                                    base_dir,
                                    article_clean_html=article_clean_html,
                                )
                        continue
                    bp = doc.add_paragraph()
                    bp.paragraph_format.left_indent = Inches(0.28)
                    for c2 in ch.children:
                        _walk_inline_to_paragraph(
                            bp,
                            c2,
                            _RunCtx(),
                            DEFAULT_BODY_PT,
                            base_dir=base_dir,
                            article_clean_html=article_clean_html,
                        )
                else:
                    bp = doc.add_paragraph()
                    bp.paragraph_format.left_indent = Inches(0.28)
                    _walk_inline_to_paragraph(
                        bp,
                        ch,
                        _RunCtx(),
                        DEFAULT_BODY_PT,
                        base_dir=base_dir,
                        article_clean_html=article_clean_html,
                    )
        return
    if name == "figure":
        for ch in node.children:
            if isinstance(ch, Tag) and (ch.name or "").lower() != "figcaption":
                _emit_content_node(
                    doc,
                    ch,
                    base_dir,
                    list_level=list_level,
                    article_clean_html=article_clean_html,
                )
        for ch in node.children:
            if isinstance(ch, Tag) and (ch.name or "").lower() == "figcaption":
                fp = doc.add_paragraph()
                fp.alignment = WD_ALIGN_PARAGRAPH.CENTER
                for c2 in ch.children:
                    _walk_inline_to_paragraph(
                        fp,
                        c2,
                        _RunCtx(),
                        META_PT,
                        base_dir=base_dir,
                        article_clean_html=article_clean_html,
                    )
        return
    if name in ("p", "div", "section"):
        if node.find(["img", "video", "audio"]):
            seen_media_keys: set[str] = set()
            for sub in node.children:
                if isinstance(sub, Tag) and _is_block_media_tag(sub):
                    mk = _media_rel_key_for_dedupe(sub)
                    if mk is not None and mk in seen_media_keys:
                        continue
                    if mk is not None:
                        seen_media_keys.add(mk)
                    _add_media_paragraph(
                        doc,
                        sub,
                        base_dir,
                        article_clean_html=article_clean_html,
                    )
                elif isinstance(sub, Tag):
                    sn = (sub.name or "").lower()
                    if sn in ("div", "section") and "word-preview-page" in _tag_class_list(
                        sub
                    ):
                        for im in sub.find_all("img"):
                            if isinstance(im, Tag):
                                _add_media_paragraph(
                                    doc,
                                    im,
                                    base_dir,
                                    article_clean_html=article_clean_html,
                                )
                        continue
                    bp = doc.add_paragraph()
                    _walk_inline_to_paragraph(
                        bp,
                        sub,
                        _RunCtx(),
                        DEFAULT_BODY_PT,
                        base_dir=base_dir,
                        article_clean_html=article_clean_html,
                        local_media_href_dedupe=set(),
                    )
                elif isinstance(sub, NavigableString) and str(sub).strip():
                    bp = doc.add_paragraph()
                    _walk_inline_to_paragraph(
                        bp,
                        sub,
                        _RunCtx(),
                        DEFAULT_BODY_PT,
                        base_dir=base_dir,
                        article_clean_html=article_clean_html,
                    )
            return
        bp = doc.add_paragraph()
        for ch in node.children:
            _walk_inline_to_paragraph(
                bp,
                ch,
                _RunCtx(),
                DEFAULT_BODY_PT,
                base_dir=base_dir,
                article_clean_html=article_clean_html,
                local_media_href_dedupe=set(),
            )
        return
    if name == "li":
        bp = doc.add_paragraph(style="List Bullet")
        ind = _list_indent_for_level(list_level)
        if ind is not None:
            bp.paragraph_format.left_indent = ind
        for ch in node.children:
            _walk_inline_to_paragraph(
                bp,
                ch,
                _RunCtx(),
                DEFAULT_BODY_PT,
                base_dir=base_dir,
                article_clean_html=article_clean_html,
            )
        return
    bp = doc.add_paragraph()
    for ch in node.children:
        _walk_inline_to_paragraph(
            bp,
            ch,
            _RunCtx(),
            DEFAULT_BODY_PT,
            base_dir=base_dir,
            article_clean_html=article_clean_html,
        )


def convert_clean_html_file_to_docx(
    clean_html_path: Path,
    docx_path: Path,
    *,
    force: bool,
) -> bool:
    if docx_path.exists() and not force:
        return False
    text = clean_html_path.read_text(encoding="utf-8", errors="replace")
    soup = BeautifulSoup(text, "html.parser")
    title_el = soup.select_one("#title")
    title = title_el.get_text(strip=True) if title_el else "无标题"
    author_el = soup.select_one("#author")
    author = author_el.get_text(strip=True) if author_el else ""
    date_el = soup.select_one("#date")
    pub = date_el.get_text(strip=True) if date_el else ""
    content = soup.select_one("#content")
    if content is None:
        raise ValueError("清洗 HTML 缺少 #content")

    doc = Document()
    _init_doc_typography(doc)
    base_dir = clean_html_path.parent

    tp = doc.add_paragraph()
    tp.alignment = WD_ALIGN_PARAGRAPH.CENTER
    tr = tp.add_run(title)
    _set_run_east_asia(tr, "黑体")
    tr.font.size = TITLE_PT
    tr.bold = False

    mp = doc.add_paragraph()
    mp.alignment = WD_ALIGN_PARAGRAPH.CENTER
    meta_line = "  ".join(x for x in (author, pub) if x)
    mr = mp.add_run(meta_line or " ")
    _set_run_east_asia(mr, "宋体")
    mr.font.size = META_PT

    for child in list(content.children):
        if isinstance(child, NavigableString):
            if str(child).strip():
                bp = doc.add_paragraph()
                _walk_inline_to_paragraph(
                    bp,
                    child,
                    _RunCtx(),
                    DEFAULT_BODY_PT,
                    base_dir=base_dir,
                    article_clean_html=clean_html_path,
                )
            continue
        if isinstance(child, Tag):
            _emit_content_node(
                doc,
                child,
                base_dir,
                list_level=0,
                article_clean_html=clean_html_path,
            )

    _apply_body_paragraph_format_to_all(doc)
    _remove_empty_paragraphs(doc)

    doc.save(str(docx_path))
    return True


def docx_path_for_article_html(raw_html_path: Path) -> Path:
    return raw_html_path.with_suffix(".docx")


def clean_html_path_for_raw(raw_html_path: Path) -> Path:
    return raw_html_path.with_name(f"{CLEAN_HTML_PREFIX}{raw_html_path.name}")


def article_raw_and_clean_paths(path: Path) -> tuple[Path, Path]:
    # 若 path 为孤儿 clean_ 文件则 (raw, clean)；否则 (raw, clean_html_path_for_raw(raw))。
    name = path.name
    if name.lower().startswith(CLEAN_HTML_PREFIX.lower()):
        raw = path.with_name(name[len(CLEAN_HTML_PREFIX) :])
        return raw, path
    return path, clean_html_path_for_raw(path)


def res_dir_for_clean(clean_path: Path) -> Path:
    return clean_path.with_suffix("")


def _remove_article_sidecars(raw_path: Path) -> None:
    cc = clean_html_path_for_raw(raw_path)
    if cc.is_file():
        cc.unlink(missing_ok=True)
    rd = res_dir_for_clean(cc)
    if rd.is_dir():
        shutil.rmtree(rd, ignore_errors=True)


def process_one_article(
    path: Path,
    session: requests.Session,
    *,
    force_clean: bool,
    remove_original: bool,
    r_clean_only: bool,
    gen_docx: bool,
    force_docx: bool,
) -> tuple[str, bool]:
    # 返回 (status, did_clean_write)。
    # status: skipped | processed | failed | skipped_docx
    raw_path, clean_path = article_raw_and_clean_paths(path)
    docx_path = docx_path_for_article_html(raw_path)
    article_id = extract_article_id(raw_path)
    source_url = guess_source_url(raw_path)

    if r_clean_only:
        if not gen_docx:
            log_warn(f"--r-c 要求同时启用 -w 以生成 Word；跳过 {path.name}")
            return "failed", False
        if clean_path.is_file():
            try:
                if convert_clean_html_file_to_docx(
                    clean_path, docx_path, force=force_docx
                ):
                    log_info(f"[docx] {docx_path}")
                    _remove_article_sidecars(raw_path)
                    if raw_path.is_file():
                        raw_path.unlink(missing_ok=True)
                    return "processed", False
                return "skipped", False
            except Exception as exc:
                log_warn(f"docx 失败 {clean_path}: {exc}")
                return "failed", False

        try:
            src_html = raw_path if raw_path.is_file() else clean_path
            if not src_html.is_file():
                log_warn(f"--r-c 无可用 HTML（缺 raw 与 clean）: {path.name}")
                return "failed", False
            text = src_html.read_text(encoding="utf-8", errors="ignore")
            soup = BeautifulSoup(text, "html.parser")
            title, author, publish_date = extract_article_meta(soup)
            content = extract_body_tag_standard(soup)
            if content is None:
                word_meta = parse_word_document_meta(text)
                if word_meta:
                    word_base, page_count = word_meta
                    content = fetch_word_preview_body(
                        session, word_base, page_count, source_url, article_id
                    )
            if content is None:
                raise ValueError("未找到正文容器")
            clean_soup = build_clean_soup(title, author, publish_date, content)
            with tempfile.TemporaryDirectory() as tmp:
                tdir = Path(tmp)
                tmp_clean = tdir / f"{CLEAN_HTML_PREFIX}article.html"
                n_resources = localize_resources(
                    clean_soup, source_url, tmp_clean, session, article_id=article_id
                )
                tmp_clean.write_text(str(clean_soup), encoding="utf-8")
                if n_resources > 0:
                    time.sleep(random.uniform(*AFTER_ARTICLE_WITH_RESOURCES_SLEEP_SEC))
                if convert_clean_html_file_to_docx(
                    tmp_clean, docx_path, force=True
                ):
                    log_info(f"[docx] {docx_path}")
                    _remove_article_sidecars(raw_path)
                    if raw_path.is_file():
                        raw_path.unlink(missing_ok=True)
                    return "processed", False
                return "failed", False
        except Exception as exc:
            log_warn(f"清洗/转换失败 {path.name}: {exc}")
            return "failed", False

    # 常规：写入磁盘上的 clean_ 前缀 HTML
    if not force_clean and clean_path.is_file():
        if gen_docx:
            try:
                if convert_clean_html_file_to_docx(
                    clean_path, docx_path, force=force_docx
                ):
                    log_info(f"[docx] {docx_path}")
                    if remove_original and raw_path.is_file():
                        raw_path.unlink(missing_ok=True)
                    return "processed", False
            except Exception as exc:
                log_warn(f"docx 失败 {clean_path}: {exc}")
        return "skipped", False

    try:
        src_html = raw_path if raw_path.is_file() else clean_path
        if not src_html.is_file():
            log_warn(f"无可用 HTML: {path.name}")
            return "failed", False
        text = src_html.read_text(encoding="utf-8", errors="ignore")
        soup = BeautifulSoup(text, "html.parser")
        title, author, publish_date = extract_article_meta(soup)
        content = extract_body_tag_standard(soup)
        if content is None:
            word_meta = parse_word_document_meta(text)
            if word_meta:
                word_base, page_count = word_meta
                log_info(
                    f"Word 预览正文 art={article_id} pages={page_count} base={word_base}"
                )
                content = fetch_word_preview_body(
                    session, word_base, page_count, source_url, article_id
                )
        if content is None:
            raise ValueError("未找到正文容器")
        clean_soup = build_clean_soup(title, author, publish_date, content)
        n_resources = localize_resources(
            clean_soup, source_url, clean_path, session, article_id=article_id
        )
        clean_path.write_text(str(clean_soup), encoding="utf-8")
        if n_resources > 0:
            time.sleep(random.uniform(*AFTER_ARTICLE_WITH_RESOURCES_SLEEP_SEC))
        log_info(f"已清洗: {raw_path.name} -> {clean_path.name}")
        wrote = True
        if remove_original and raw_path.is_file():
            raw_path.unlink(missing_ok=True)
        if gen_docx:
            try:
                if convert_clean_html_file_to_docx(
                    clean_path, docx_path, force=force_docx
                ):
                    log_info(f"[docx] {docx_path}")
            except Exception as exc:
                log_warn(f"docx 失败 {clean_path}: {exc}")
        return "processed", wrote
    except Exception as exc:
        log_warn(f"清洗失败 {path.name}: {exc}")
        return "failed", False


def docx_from_raw_html_via_temp(
    path: Path,
    session: requests.Session,
    *,
    force_docx: bool,
    remove_original: bool = False,
) -> str:
    # 不写 clean_ 前缀 HTML 到输出目录，在临时目录中清洗并生成与 raw 同名的 .docx。
    docx_path = docx_path_for_article_html(path)
    if docx_path.exists() and not force_docx:
        return "skipped"
    article_id = extract_article_id(path)
    source_url = guess_source_url(path)
    try:
        text = path.read_text(encoding="utf-8", errors="ignore")
        soup = BeautifulSoup(text, "html.parser")
        title, author, publish_date = extract_article_meta(soup)
        content = extract_body_tag_standard(soup)
        if content is None:
            word_meta = parse_word_document_meta(text)
            if word_meta:
                word_base, page_count = word_meta
                content = fetch_word_preview_body(
                    session, word_base, page_count, source_url, article_id
                )
        if content is None:
            raise ValueError("未找到正文容器")
        clean_soup = build_clean_soup(title, author, publish_date, content)
        with tempfile.TemporaryDirectory() as tmp:
            tdir = Path(tmp)
            tmp_clean = tdir / f"{CLEAN_HTML_PREFIX}article.html"
            n_resources = localize_resources(
                clean_soup, source_url, tmp_clean, session, article_id=article_id
            )
            tmp_clean.write_text(str(clean_soup), encoding="utf-8")
            if n_resources > 0:
                time.sleep(random.uniform(*AFTER_ARTICLE_WITH_RESOURCES_SLEEP_SEC))
            if convert_clean_html_file_to_docx(tmp_clean, docx_path, force=True):
                log_info(f"[docx] {docx_path}")
                if remove_original and path.is_file():
                    path.unlink(missing_ok=True)
                return "processed"
            return "failed"
    except Exception as exc:
        log_warn(f"仅 Word 转换失败 {path}: {exc}")
        return "failed"


def run_clean_and_word_pass(
    root: Path,
    session: requests.Session,
    *,
    enable_clean: bool,
    gen_word: bool,
    force_clean: bool,
    force_docx: bool,
    remove_original: bool,
    r_clean_only: bool,
    limit: int = 0,
    remove_raw_when_word_only: bool = False,
) -> int:
    if not enable_clean and not gen_word:
        return 0
    files = iter_library_article_html_files(root)
    if limit > 0:
        files = files[:limit]
    if not files:
        log_warn("未找到待处理的文库 HTML。")
        return 0
    ok = fail = skip = 0
    for idx, fp in enumerate(files, start=1):
        if r_clean_only and gen_word:
            st, _ = process_one_article(
                fp,
                session,
                force_clean=force_clean,
                remove_original=False,
                r_clean_only=True,
                gen_docx=True,
                force_docx=force_docx,
            )
            if st == "processed":
                ok += 1
            elif st == "skipped":
                skip += 1
            else:
                fail += 1
        elif enable_clean:
            st, _ = process_one_article(
                fp,
                session,
                force_clean=force_clean,
                remove_original=remove_original,
                r_clean_only=r_clean_only,
                gen_docx=gen_word,
                force_docx=force_docx,
            )
            if st == "processed":
                ok += 1
            elif st == "skipped":
                skip += 1
            else:
                fail += 1
        elif gen_word:
            raw_fp, clean_fp = article_raw_and_clean_paths(fp)
            docx_path = docx_path_for_article_html(raw_fp)
            if clean_fp.is_file():
                try:
                    if convert_clean_html_file_to_docx(
                        clean_fp, docx_path, force=force_docx
                    ):
                        log_info(f"[docx] {docx_path}")
                        if remove_raw_when_word_only and raw_fp.is_file():
                            raw_fp.unlink(missing_ok=True)
                        ok += 1
                    else:
                        skip += 1
                except Exception as exc:
                    log_warn(f"docx 失败 {clean_fp}: {exc}")
                    fail += 1
            elif raw_fp.is_file():
                st = docx_from_raw_html_via_temp(
                    raw_fp,
                    session,
                    force_docx=force_docx,
                    remove_original=remove_raw_when_word_only,
                )
                if st == "processed":
                    ok += 1
                elif st == "skipped":
                    skip += 1
                else:
                    fail += 1
            else:
                log_warn(f"跳过（无 raw 与 clean HTML）: {fp}")
                fail += 1
        if idx % 20 == 0:
            log_info(f"清洗/Word 进度: {idx}/{len(files)}")
    log_info(
        f"清洗/Word 完成: 处理 {ok} / 跳过 {skip} / 失败 {fail} / 共 {len(files)}"
    )
    return fail


def iter_raw_html_files(root: Path) -> list[Path]:
    return iter_library_article_html_files(root)


if __name__ == "__main__":
    raise SystemExit("入口：仓库根 wc-library.py 或 src/wc-library.py（本文件为库模块，不作为主程序）。")
