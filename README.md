# crawler-360DOC 使用说明

## 合规声明（重要）
鉴于 360 个人图书馆平台曾发布公告拟关闭网站，编写此项目，**仅用于本人学习，侵删。**本项目仅用于个人学习、技术验证与个人备份研究。

通过本工具下载、转换或保存的任何内容，其著作权与使用条件以原站点及权利方为准。使用者须自行遵守法律法规、平台用户协议与著作权相关规定；**不得以本工具实施爬取他人未授权作品、传播盗版、商用侵权等行为**。若以学习、技术验证为目的使用本仓库，请在完成学习或验证后 **24 小时内** 删除不再需要的本地副本（或仅保留法律允许范围内的个人备份），并勿对外再分发所抓取的完整内容。因使用本软件而产生的行政、民事或刑事责任，**由使用方自行承担**，详见仓库根目录 [`LICENSE`](https://github.com/ZenSyntax/crawler-360DOC/blob/master/LICENSE) 文件。

## 1. 项目概述
本项目用于在**已登录 360doc 账号**前提下，备份个人文库、随笔与关注用户文章数据，并提供清洗、资源本地化、Word 导出、失败回放复洗与告警能力。

主要能力：
- 文库抓取：按分类/分页抓取文章 HTML。
- 关注抓取：按“关注用户 -> 分类”抓取文章 HTML。
- 数据清洗：输出 `clean_*.html`，并重组正文结构。
- 资源本地化：将图片/附件下载到本地目录并回写相对路径。
- Word 导出：将清洗后的 HTML 转换为 `.docx`。
- 仅处理本地文件模式：`--word-only` / `--clean-only` / `--local-only`。
- 回放复洗：根据清洗失败日志自动重试并复洗可恢复文章。
- 邮件告警：登录失败、限流、黑名单拦截等异常触发告警和熔断。

## 2. 目录结构
```text
.
├─ src/
│  ├─ _site_paths.py
│  ├─ wc-library.py         # 文章抓取模块
│  ├─ wc-essay.py           # 随笔抓取模块
│  ├─ wc-follow.py          # 关注用户抓取模块
│  ├─ library-processer.py  # 文章清洗模块
│  ├─ essay-to-word.py      # 随笔转 Word 文档模块
│  └─ replay_clean_logs.py  # 清洗失败复洗模块（依赖相关日志文档）
├─ logs/                    # 异常日志输出目录
├─ wc-library.py            # 根目录入口，转发到 src/wc-library.py
├─ wc-essay.py              # 根目录入口，转发到 src/wc-essay.py
├─ wc-follow.py             # 根目录入口，转发到 src/wc-follow.py
└─ .env-keep                # 环境变量，配置系统运行信息
```

## 3. 依赖安装
```bash
pip install requests beautifulsoup4 python-docx python-dotenv pillow
```

说明：
- `python-dotenv` 为可选，但强烈建议安装；安装后会自动尝试加载仓库根 `.env`。
- `lxml` 也是可选；`wc-essay` 在有 `lxml` 时优先使用，否则回退 `html.parser`。
- `pillow` 用于 Word 插图兜底：当资源真实格式为 WEBP 等、`python-docx` 不能直接识别时会自动转 PNG 后插入。

## 4. 环境变量
可以直接删除 `.env-keep`的`-keep`后缀并使用，但更建议将 `.env-keep` 复制为 `.env` 后填写真实值。

### 4.1 必填
- `DOC360_USER`：360doc 登录账号
- `DOC360_PASS`：360doc 登录密码

### 4.2 频控与并发
- `DOC360_MIN_TIME`：随机等待下限（毫秒），默认 `5000`
- `DOC360_MAX_TIME`：随机等待上限（毫秒），默认 `10000`
- `DOC360_MAX_WORKERS`：单篇文章资源下载并发上限（正整数），默认 `50`

行为说明（与代码一致）：
- `DOC360_MIN_TIME` / `DOC360_MAX_TIME` 同时用于抓取阶段与清洗阶段。
- 清洗阶段支持“文章与文章之间”的随机频控。
- 资源线程数按 `min(资源数, DOC360_MAX_WORKERS)` 动态计算。
- `--word-only` 本地并行转换使用 `DOC360_MAX_WORKERS` 作为最大线程数。

### 4.3 邮件告警（可选）
- `DOC360_ALERT_TO`
- `DOC360_SENDER`
- `DOC360_KEY`
- `DOC360_SMTP_HOST`
- `DOC360_SMTP_PORT`（默认 465）
- `DOC360_SMTP_STARTTLS`（`1/true/yes/on`）

## 5. 启动方式
两种入口都可用：
- 根目录入口（推荐）：
  - `python wc-library.py`
  - `python wc-essay.py`
  - `python wc-follow.py`
- 直接运行 `src`：
  - `python src/wc-library.py`
  - `python src/wc-essay.py`
  - `python src/wc-follow.py`

## 6. 文库脚本（wc-library.py）

### 6.1 常用命令
抓取 + 清洗：
```bash
python wc-library.py -c
```

抓取 + 清洗 + Word 生成：
```bash
python wc-library.py -c -w
```

仅清洗本地已有（不抓取）：
```bash
python wc-library.py --clean-only
```

仅将本地清洗文件转换为 Word（不抓取）：
```bash
python wc-library.py --word-only
```
说明：`--word-only` 仅转换本地 `clean_*.html`；若缺失对应 clean 文件则跳过该文章。

本地模式，即只对本地已有的文章文件进行清洗并转换为 word（按参数组合执行）：
```bash
python wc-library.py --local-only -c -w
```

分类+页码范围：
```bash
python wc-library.py --start-c 5 --end-c 10 --c-id --start-page 1 --end-page 3
```

仅保留 docx（自动触发 Word 管线）：
```bash
python wc-library.py --r-c
```

### 6.2 启动参数一览
- `-d/--d DIR`：输出根目录
- `-f/--f`：强制覆盖
- `-c`：启用清洗
- `-w/--w`：导出 Word
- `--word-only`：仅对本地已有的 `clean_*.html` 转 Word
- `--clean-only`：仅对本地已有的`<atr_id>-exampleArticle.html` 进行清洗
- `--local-only`：仅处理本地已有数据，配合 `-c/-w` 使用
- `--start-page N` / `--end-page N`：列表页码范围
- `--start-c V` / `--end-c V`：分类范围
- `--c-id`：按分类 ID 解析范围
- `--c-name`：按分类名称片段解析范围
- `--r`：清洗完成后删除原始`<atr_id>-exampleArticle.html`（**不推荐**开启）
- `--r-c`：仅保留 docx（删除 raw/clean HTML 与资源目录，**不推荐**，因转换效果及在少数情况下存在问题）

### 6.3 关键行为说明（代码对齐）
- `--r-c` 会在代码中自动启用 Word 管线，不要求必须显式加 `-w`。
- `--local-only` 仅在需要网络资源的流程（如清洗或 `--r-c`）且存在账号密码时才自动登录；纯 `--word-only` 不登录。
- 本地 `--word-only` 在未加 `-f` 且目标 `.docx` 已存在时直接跳过；无额外等待节奏。
- 当启用清洗（`clean_disk=True`）时，清洗结束后会自动执行一次日志回放复洗（`replay_resource_failures_from_logs`）。
- 文章页抓取遇到 403 且判定黑名单拦截会立即告警并退出。
- 清洗阶段遇到 403 熔断；若异常文本包含 `blacklist`，会发邮件后退出（状态码 5）。

### 6.4 `--r` / `--r-c` 产物行为
- `--r`：成功后删除原始 raw HTML；`clean_*.html` 与资源目录仍保留。
- `--r-c`：目标是仅保留 docx，raw HTML、`clean_*.html`、清洗资源目录都会被清理。
- `--r-c` 模式下清洗会走临时文件中转，不以最终 `clean_*.html` 落盘。

## 7. 随笔脚本（wc-essay.py）

### 7.1 常用命令
抓取全部随笔：
```bash
python wc-essay.py
```

只抓某个分类（2/3/4）：
```bash
python wc-essay.py -c 3
```

抓取后导出 Word：
```bash
python wc-essay.py -w
```

仅执行 Word（不抓取）：
```bash
python wc-essay.py --word-only
```

### 7.2 参数一览
- `-c/--c CAT_ID`：仅抓指定随笔分类（有效值：2=待分类，3=日记，4=普通随笔）
- `--start-page N` / `--end-page N`：页码范围
- `-d/--d DIR`：输出目录（默认 `output-space/my-essay`）
- `-f/--f`：强制覆盖 HTML；配合 `-w` 时也强制覆盖 `.docx`
- `-w/--w`：抓取后转换为 `.docx`
- `--word-only`：仅转换本地 HTML，不登录不抓取

### 7.3 Word 转换规则
- `wc-essay --word-only` 需要目标目录已存在，否则直接退出。
- 未加 `-f` 时按 mtime 增量更新 docx；加 `-f` 时强制全量覆盖。

## 8. 关注脚本（wc-follow.py）

### 8.1 常用命令
抓取全部关注用户文章（按用户与分类遍历）：
```bash
python wc-follow.py
```

抓取 + 清洗：
```bash
python wc-follow.py -c
```

抓取 + 清洗 + Word：
```bash
python wc-follow.py -c -w
```

仅清洗本地已有（不抓取）：
```bash
python wc-follow.py --clean-only
```

仅将本地清洗文件转换为 Word（不抓取）：
```bash
python wc-follow.py --word-only
```

按关注用户过滤（ID / 昵称）：
```bash
python wc-follow.py --user-id 12345678,87654321
python wc-follow.py --user-name 张三,李四
```

按关注用户分类过滤（ID 或名称片段）：
```bash
python wc-follow.py --c 2,养生
```

### 8.2 参数一览
- `-d/--d DIR`：输出根目录（默认 `output-space/my-follow`）
- `-f/--f`：强制覆盖
- `-c`：启用清洗
- `-w/--w`：导出 Word
- `--word-only`：仅对本地已有的 `clean_*.html` 转 Word
- `--clean-only`：仅对本地已有 HTML 执行清洗
- `--local-only`：仅处理本地已有数据，配合 `-c/-w/--r-c` 使用
- `--user-id ID`：仅抓指定关注用户 ID（可逗号分隔多个）
- `--user-name NAME`：仅抓指定关注用户名（精确或包含匹配，可逗号分隔多个）
- `--c CAT`：按关注用户分类过滤（分类 ID 或名称片段，可逗号分隔多个）
- `--r`：清洗完成后删除原始 HTML（仅 Word 时删除 raw 保留 clean）
- `--r-c`：仅保留 docx（删除 raw/clean HTML 与资源目录，需同时使用 `-w`）

### 8.3 关键行为说明（代码对齐）
- 关注抓取阶段复用 `wc-library` 的登录、会话与告警能力。
- 本地 `--clean-only` 与 `--r-c` 可能触发资源请求，存在账号密码时会尝试自动登录。
- 关注抓取目录层级为：`<输出根>/<用户ID-用户名>/<分类ID-分类名>/<artid-title>.html`。
- 清洗/Word 处理复用 `library-processer.py`，行为与 `wc-library` 保持一致。

## 9. 回放脚本（replay_clean_logs.py）
脚本路径：
```bash
python src/replay_clean_logs.py --root output-space/my-category
```

可选参数：
- `--root`：清洗输出根目录
- `--no-login`：不使用 `DOC360_USER/DOC360_PASS` 自动登录

功能：
- 读取 `resources_not_found_warning.txt` 和 `clean_error_url.txt`
- 逐条探测资源是否可恢复
- 对可恢复文章执行强制复洗
- 复洗成功后移除对应日志行

补充：该脚本主要用于测试，且在主程序模块内自动引用，**一般不需要单独使用**。

## 10. 日志文件
日志统一输出到 `logs/`：
- `library_error_url.txt`：文库文章抓取失败
- `library_not_found_warning.txt`：文库文章 404 告警（不计失败）
- `clean_error_url.txt`：清洗资源失败明细（兼容旧格式并附 `article/dir`）
- `clean_article_error.txt`：单篇文章清洗失败汇总
- `resources_not_found_warning.txt`：资源 404 告警（不计失败）
- `essay_error_url.txt`：随笔抓取异常
- `follow_error_url.txt`：关注抓取异常

## 11. 清洗与资源处理策略
- 404与424排除：
  - 文章/资源 404 / 424 记为 warning，不计为清洗失败。
- 清洗失败回滚：
  - 单篇失败会删除该篇 `clean_*.html` 与同名资源目录。
- 网关瞬态错误：
  - `502/503/504` 按瞬态错误退避重试。
- 403 熔断：
  - 清洗资源请求出现非过期类 403 会触发熔断。
- 私有资源降级：
  - 调用 `Ajax/imgurl.ashx?op=changeurl` 获取带签名的真实资源 URL。

# 逆向分析

## 12. 登录与会话建立
关键接口：
- `GET /login.aspx`
- `GET /ajax/login/login.ashx?email=...&pws=md5(pass)&isr=1&login=1&code=&_=timestamp`
- `GET /ajax/LoginAlertHandler.ashx?type=1&_=timestamp`
- `GET /`（首页上下文同步）

实现细节（`src/wc-library.py`）：
- 使用 `requests.Session` 维持 Cookie。
- 登录后追加 `LoginAlertHandler` 和首页访问，提升后续接口稳定性。
- 文库文章页请求头刻意贴近浏览器文档请求（含 `Sec-Fetch-*`、`Referer`、清空 `X-Requested-With`）。

## 13. 文库抓取 API 结构
分类接口：
- `GET /ajax/getmyCategory.ashx?type=3&_={ms}`
- 关键字段：`id`、`artnum`、`CategoryName/selftitle`

列表接口：
- 普通分类：`GET /ajax/HomeIndex/getCategoryArt.ashx`
- 草稿箱：`GET /ajax/getMydraft.ashx`
- 回收站：`GET /ajax/HomeIndex/getmyrecycleart.ashx`

文章 HTML：
- `GET /showweb/0/0/{artid}.aspx`
- 代码统一使用 `showweb` 模板，不依赖列表 `arturl`。

## 14. 清洗与资源抓取链路
清洗核心模块：`src/library-processer.py`
- 正文提取：标准正文 + Word 预览 + PPT 预览兜底
- 清洗模板：输出 `clean_*.html`
- 资源本地化：收集 `img/a/source`，下载后回写为相对路径

资源候选构造来源：
- 标签自身属性：`src` / `data-src` / `data-original` / `data360-src`
- 文章源页面映射：`data360-src -> src`（常含 `Expires/Signature/domain`）
- `changeurl` 返回映射缓存
- 同路径多主机族（`checku/checki/imgu/imgi`）补偿尝试

## 15. 图片 URL 转换签名算法（核心）
对应代码函数：
- `_img_change_sign`
- `_request_changeurl_signed_images`

### 15.1 请求接口
- `POST /Ajax/imgurl.ashx?op=changeurl&_={ms}`

表单参数：
- `imgurl`：原始资源 URL，支持逗号拼接多个
- `sign`：签名值

请求头关键字段：
- `Referer: source_url`
- `X-Requested-With: XMLHttpRequest`
- `User-Agent: Session UA`

### 15.2 sign 生成算法（与代码完全一致）
输入参数集合：`{"op": "changeurl", "imgurl": "<csv>"}`  
步骤：
1. 参数转 `k=v`（空值不参与）  
2. 按字典序排序  
3. **不加分隔符直接拼接**  
4. UTF-8 编码后做 `SHA1`  
5. 十六进制字符串转大写

伪代码：
```python
parts = []
for k, v in params.items():
    if str(v) != "":
        parts.append(f"{k}={v}")
parts.sort()
joined = "".join(parts)
sign = sha1(joined.encode("utf-8")).hexdigest().upper()
```

说明：
- 当前实现不是 HMAC，而是直接 SHA1 摘要。
- 在当前参数键集合下，排序后通常是 `imgurl=...op=changeurl` 再做 SHA1。

### 15.3 响应解析
典型返回：
- `status == "1"`
- `imgurl`（URL 编码后的逗号串）

解析流程：
1. JSON 反序列化  
2. 取 `imgurl` 字段  
3. `unquote` 一次并将 `\/` 还原为 `/`  
4. 按逗号拆分 URL  
5. 过滤域名与有效性  
6. 若 `Expires <= now + 15s`，直接丢弃，避免“拿到即过期”

## 16. domain / Expires / Signature 的处理
- `domain` 的提示值来自分类接口 `artnum`，通过目录名前缀分类 ID 映射到文章处理上下文。
- 签名过期或即将过期时，会触发惰性刷新：
  - 再调 `changeurl` 获取新签名 URL
  - 必要时基于当前时间重组参数并补 `domain` 提示再试

## 17. 异常分类、熔断与告警
`request_with_retry` 的语义（清洗资源请求）：
- `404` 或正文含 404 特征：`ResourceNotFoundError`（warning，不计失败）
- 403 且命中过期特征：`ResourceExpiredError`（刷新签名重试）
- `502/503/504`：`ResourceGatewayError`（退避重试）
- 其他 403：`CleanRateLimitError`（熔断）

黑名单特别处理（`src/wc-library.py`）：
- 若清洗熔断异常文本同时含 `status=403` 与 `blacklist`：
  - 先发送告警邮件
  - 再以状态码 5 退出

## 18. 并发与频控
- 资源并发：`workers = min(unique_urls, DOC360_MAX_WORKERS)`
- 任务启动抖动：每个资源任务在发起前随机短暂停顿
- 抓取频控：文章间/页间按 `DOC360_MIN_TIME ~ DOC360_MAX_TIME` 随机等待
- 清洗频控：文章与文章之间同区间随机等待

## 19. 与代码一致的实现结论
- 项目已经形成“抓取 -> 清洗 -> 本地化 -> 导出 -> 回放修复”闭环。
- `changeurl` 签名转换 + 过期刷新是私有资源可用性的关键。
- 404 与失败分流、403 黑名单告警、回放复洗机制共同提升可恢复性与可维护性。

- 覆盖文库、随笔、关注用户三类抓取入口，处理链路保持一致。
