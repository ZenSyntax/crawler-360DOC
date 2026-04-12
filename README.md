# 说明文档

## 项目功能

鉴于 360 个人图书馆平台曾发布公告拟关闭网站，编写此项目，**仅用于本人学习，侵删。**

**个人电子邮箱：moreantx@gmail.com**。

本仓库用于在**已登录账号**的前提下，将 [360doc](http://www.360doc.com) 个人空间中的部分内容批量备份到本地，并可通过启动命令选择性的进行数据格式化清洗与 Word 归档。

**后续与合规说明**：通过本工具下载、转换或保存的任何内容，其著作权与使用条件以原站点及权利方为准。使用者须自行遵守法律法规、平台用户协议与著作权相关规定；**不得以本工具实施爬取他人未授权作品、传播盗版、商用侵权等行为**。若以学习、技术验证为目的使用本仓库，请在完成学习或验证后 **24 小时内** 删除不再需要的本地副本（或仅保留法律允许范围内的个人备份），并勿对外再分发所抓取的完整内容。因使用本软件而产生的行政、民事或刑事责任，**由使用方自行承担**，详见仓库根目录 [`LICENSE`](LICENSE) 文件。

- **随笔**（`src/wc-essay.py`）：按分类分页下载随笔列表对应的网页，清洗为本地 HTML，并可生成同名 **docx**（逻辑见 `src/essay-to-word.py`）。
- **文库**（`src/wc-library.py`）：按文库分类与列表页抓取文章 HTML；可选 **清洗**（同目录下 `clean_` 前缀 HTML 与配套资源目录）、**转 Word**（`src/library-processer.py`），并可通过 `--r` / `--r-c` 控制在成功后是否删除原始或清洗产物，仅保留 docx 等。
- **共用能力**：依赖环境变量中的账号登录；支持请求间隔（频控）、失败与异常时可写入错误记录、可选 SMTP 邮件告警；输出目录、页码范围、是否强制覆盖（`-f`）等均由命令行指定。

下文「环境与共用配置」与各脚本章节给出依赖安装、`.env` 与具体参数说明。

## 环境与共用配置

下列依赖、环境变量与启动路径对**随笔**（wc-essay）与**文库**（wc-library）脚本共用；后文各节仅写各自差异。

### 依赖

必需：

```
pip install requests beautifulsoup4 python-docx
```

可选（BeautifulSoup 更快解析；随笔默认会优先使用）：

```
pip install lxml
```

可选（从仓库根加载环境变量；未安装则仅用系统或 Shell。已在 Shell 中设置的键值对不会被覆盖）：

```
pip install python-dotenv
```

### 环境变量文件（.env）

仓库根提供仅含示例值的占位文件（可提交到 Git）：

```
.env-keep
```

若要启用基于文件的配置：**推荐**将占位文件**复制**为下列文件名，再于新文件中填入真实账号与邮件等配置（未改名的占位文件仍留在仓库根，便于对照或日后更新）：

```
.env
```

**备选**：也可将占位文件**直接重命名**为上述文件名（即从原名称中删去紧挨在 env 前的连字符与 keep 四字母）；工作区将不再保留未重命名前的占位文件，需要时可从版本库再次检出或复制。保存真实密钥的该文件已被 Git 忽略，请勿提交。

### 脚本路径与仓库根

业务脚本须位于仓库根下的：

```
src/
```

校验逻辑在：

```
src/_site_paths.py
```

仓库根通过向上查找 .env 或 .git 确定。

推荐在仓库根使用根目录**启动器**（与下文各节的 `python wc-essay.py` / `python wc-library.py` 一致），也可直接执行 `src/` 内对应 `.py` 文件。

### 账号环境变量（必须）

```
DOC360_USER
```

登录账号（与浏览器一致）。

```
DOC360_PASS
```

明文密码（脚本内 MD5 后作为登录参数）。

### 请求频控（可选）

由下列两项指定随机等待的上下限（**毫秒**，非负整数）。须**同时**设置且满足 MIN≤MAX；缺任一项、无法解析为整数或 MIN>MAX 时，采用默认 **2000–5000 ms**。可与 Shell / 系统环境变量或仓库根下列持久配置文件（与账号等写在同一文件，需 python-dotenv）配合使用：

```
.env
```

```
DOC360_MIN_TIME
```

随机等待下限（毫秒）。

```
DOC360_MAX_TIME
```

随机等待上限（毫秒）。

随笔与文库均在启动时于「本次命令行配置（已生效）」日志块中输出当前采用的区间及来源；**不抓取、仅后处理**的模式下文分别说明是否输出该项。

### 邮件告警环境变量（可选）

```
DOC360_ALERT_TO
```

告警收件人；未设置或无效则不发邮件。

```
DOC360_SENDER
```

发件人（通常与 SMTP 登录一致）。

```
DOC360_KEY
```

SMTP 密码或授权码（QQ 为授权码等）。

```
DOC360_SMTP_HOST
```

示例值：

```
smtp.qq.com
```

```
DOC360_SMTP_PORT
```

默认端口：

```
465
```

```
DOC360_SMTP_STARTTLS
```

QQ 使用 465 时可留空；下列任一值表示启用 STARTTLS（常见于 587），每行一个取值：

```
1
true
yes
on
```

配置不完整或错误时仅输出 WARN 级别日志，不中断程序；未完整配置时不会发送告警邮件。

### PowerShell 下配置示例

优先级高于 .env，但比较麻烦，**不推荐**。

在同一 PowerShell 窗口中先切换到仓库根，再执行下列赋值（路径与账号等请改为你的真实值；可直接整段复制后修改敏感项）：

```
Set-Location 'D:\repos\crawler-360DOC'
$env:DOC360_USER = "your_360doc_login"
$env:DOC360_PASS = "your_plain_password"
$env:DOC360_ALERT_TO = "alert_recipient@example.com"
$env:DOC360_SENDER = "smtp_login@example.com"
$env:DOC360_KEY = "your_smtp_app_password_or_secret"
$env:DOC360_SMTP_HOST = "smtp.example.com"
$env:DOC360_SMTP_PORT = "465"
$env:DOC360_SMTP_STARTTLS = ""
$env:DOC360_MIN_TIME = "2000"
$env:DOC360_MAX_TIME = "5000"
```

若使用 587 等需 STARTTLS 的端口，将最后一行改为例如：

```
$env:DOC360_SMTP_STARTTLS = "1"
```

上述赋值仅对**当前** PowerShell 会话有效；关闭窗口后需重新执行，可以改用仓库根持久配置文件（见上文）及系统环境变量。

---

## wc-essay

360doc 随笔备份：登录后分页拉取列表、清洗为本地 HTML，可选转为 Word（docx）。登录、HTTP、邮件告警与 Word 转换均在下列文件内实现，不依赖 library 模块：

```
src/wc-essay.py
```

Word 转换子模块（由 wc-essay 加载，勿直接作为主程序运行）：

```
src/essay-to-word.py
```

依赖、环境变量文件、账号、频控、邮件与 PowerShell 示例见上文**环境与共用配置**。

### 路径与启动

推荐在仓库根执行：

```
python wc-essay.py
```

上述命令由仓库根启动器转调 src 内脚本。也可直接：

```
python src/wc-essay.py
```

默认随笔输出根目录（相对仓库根，不存在则自动创建）：

```
output-space/my-essay
```

### 随笔专用：频控日志

下列模式不进行列表抓取，启动日志中**不输出**「请求频控」区间行：

```
--word-only
```

### 命令行用法

在仓库根执行；若当前不在仓库根，请先切换目录或使用启动器脚本的绝对路径。

抓取随笔；HTML 默认写入 output-space/my-essay：

```
python wc-essay.py
```

指定输出根（相对或绝对）；省略则用默认 output-space/my-essay：

```
python wc-essay.py --d <目录>
```

强制覆盖已有 HTML；与 -w 同用时亦强制覆盖 docx：

```
python wc-essay.py -f
```

抓取结束后将清洗 HTML 转为同名 docx（默认按 mtime 增量）：

```
python wc-essay.py -w
```

仅 Word 转换，不登录、不抓取（输出目录须已存在）：

```
python wc-essay.py --word-only
```

只抓指定分类：2 待分类，3 日记，4 普通随笔；省略则三类都抓：

```
python wc-essay.py -c <ID>
```

列表起始页（从 1 起）；省略为 1：

```
python wc-essay.py --start-page N
```

列表结束页（含）；省略则不因页码上界停止：

```
python wc-essay.py --end-page M
```

**组合示例**

输出到 test/foo（相对当前工作目录），覆盖 HTML，并全量重写 docx：

```
python wc-essay.py --d test/foo -f -w
```

默认目录下增量生成或更新 docx：

```
python wc-essay.py -w
```

仅转换默认输出目录下 HTML ：

```
python wc-essay.py --word-only -w
```

只抓「日记」第 2～5 页：

```
python wc-essay.py -c 3 --start-page 2 --end-page 5
```

三类各抓第 10～12 页：

```
python wc-essay.py --start-page 10 --end-page 12
```

指定目录、单分类、分页并覆盖 HTML 与 Word：

```
python wc-essay.py -d backup/essay -c 4 --start-page 1 --end-page 3 -f -w
```

### 运行期行为

错误 URL 行追加到输出根目录下文件：

```
essay_error_url.txt
```

下列为 **Python 源码中的常量**（**不是**环境变量），可在 `src/wc-essay.py` 中查看或修改：

- `ESSAY_POST_NETWORK_RETRIES`、`ESSAY_POST_NETWORK_RETRY_WAIT_SEC`：随笔列表 POST 遇网络/超时时的本地重试次数与间隔（秒）；用尽后仍失败会走告警邮件等逻辑。
- `ARTICLE_429_RETRY_INTERVAL_SEC`、`ARTICLE_429_RETRY_LOG_EVERY`、`ARTICLE_429_ALERT_ATTEMPT`：随笔列表 POST 遇 HTTP 429 或正文疑似限流时的退避间隔、日志间隔与告警触发次数。

---

## wc-library

360doc 个人文库：登录后按分类分页抓取文章 HTML；可选数据清洗与 Word（docx）。实现文件如下（清洗与 Word 由 library-processer 提供，由 wc-library 加载）：

```
src/wc-library.py
```

```
src/library-processer.py
```

依赖、环境变量文件、账号、频控、邮件与 PowerShell 示例见上文**环境与共用配置**。

### 路径与启动

推荐在仓库根执行：

```
python wc-library.py
```

上述命令由仓库根启动器转调 src 内脚本。也可直接：

```
python src/wc-library.py
```

默认文库输出根目录（相对仓库根，不存在则自动创建）：

```
output-space/my-category
```

### 文库专用：与随笔的差异

随笔中 `-c` 表示**分类 ID**；文库中 `-c` 表示**启用数据清洗**（在与原文同目录生成 `clean_` 前缀的 HTML，例如 `123-a.html` 对应 `clean_123-a.html`，资源放在与清洗文件主名一致的子目录 `clean_123-a/`）。文库分类范围请用 `--start-c` / `--end-c` 配合 `--c-id`（默认，按数字 id）或 `--c-name`（按接口返回名称匹配 id）。扫描时跳过以 `clean_` 开头的 HTML（已清洗结果），不参与抓取后处理。

清洗后的 HTML 由 `library-processer` 内嵌样式模板生成：正文在 `#content` 中，**对齐与缩进尽量沿用源 HTML** 中仍保留的 `style`、`align` 与结构；模板 CSS 对正文内的 **`img` 统一水平居中**（`display:block` + 左右 `auto` margin），表格不强制居中。

下列模式不登录、不抓取时，须至少配合 `-c`、`-w` 或 `--r-c` 之一：

```
--word-only
```

随笔在 `--word-only` 下启动日志不打印「请求频控」行；文库在 `--word-only` 下仍会打印频控区间（与抓取共用同一套说明块）。

### 命令行用法

在仓库根执行；若当前不在仓库根，请先切换目录或使用启动器脚本的绝对路径。

抓取文库文章；HTML 默认写入 output-space/my-category：

```
python wc-library.py
```

指定输出根（相对或绝对）；省略则用默认 output-space/my-category：

```
python wc-library.py --d <目录>
```

强制覆盖已有文章 HTML；与清洗、`-w` 同用时亦强制覆盖 `clean_` 前缀 HTML 与 docx：

```
python wc-library.py -f
```

抓取结束后将正文转为与**原始** HTML **同名**的 docx（默认按 mtime 增量；无 `clean_` 前缀 HTML 时由 `library-processer` 在临时目录清洗再导出，**不必**同时开 `-c`）：

```
python wc-library.py -w
```

仅执行清洗与/或 Word，不登录、不抓取（输出目录须已存在，且须带 `-c`、`-w` 或 `--r-c`；**仅 `--r-c` 时程序会自动启用 Word 管线**，无需再写 `-w`）：

```
python wc-library.py --word-only
```

启用数据清洗（写入 `clean_` 前缀 HTML）：

```
python wc-library.py -c
```

列表起始页（从 1 起）；省略为 1：

```
python wc-library.py --start-page N
```

列表结束页（含）；省略则无页码上界：

```
python wc-library.py --end-page M
```

分类范围起点（与 `--c-id` 时为 id，与 `--c-name` 时为名称片段）：

```
python wc-library.py --start-c <值>
```

分类范围终点：

```
python wc-library.py --end-c <值>
```

显式指定 `--start-c` / `--end-c` 为数字分类 id（默认即 id 模式时可省略本标志）：

```
python wc-library.py --c-id
```

按分类名称在接口返回的目录中解析 id 范围：

```
python wc-library.py --c-name
```

清洗或 Word 成功后删除**原始**下载 HTML：

```
python wc-library.py --r
```

仅保留 docx，不保留原始 HTML、`clean_` 前缀 HTML 及对应本地媒体目录（隐含按 Word 处理，等价于需要 `-w`；脚本对 `--r-c` 会自动按生成 Word 处理）：

```
python wc-library.py --r-c
```

**组合示例**

指定输出目录、分类 id 范围、分页、清洗并导出 Word（不加 `-f` 时不覆盖已有 HTML、`clean_` 与 docx，走增量）：

```
python wc-library.py --d backup/category --start-c 2 --end-c 100 --c-id --start-page 1 --end-page 3 -c -w
```

同上路径与范围，但 `-f` 强制重下 HTML、重写 `clean_` 前缀 HTML 与 docx：

```
python wc-library.py --d backup/category --start-c 2 --end-c 100 --c-id --start-page 1 --end-page 3 -c -w -f
```

默认目录下仅增量清洗：

```
python wc-library.py -c
```

仅 Word（目录内已有原始 HTML 或 `clean_` 前缀 HTML）：

```
python wc-library.py --word-only -w
```

仅 Word 且强制按正文重新生成 docx（覆盖已有 docx）：

```
python wc-library.py --word-only -w -f
```

按分类名称限定范围并抓取首页：

```
python wc-library.py --start-c 日记 --end-c 日记 --c-name --end-page 1
```

同上，并强制覆盖该分类首页已下载的 HTML：

```
python wc-library.py --start-c 日记 --end-c 日记 --c-name --end-page 1 -f
```

抓取并清洗、导出 Word，成功后**只删原始下载 HTML**，仍保留 `clean_` 前缀文件、资源子目录与 docx：

```
python wc-library.py -c -w --r
```

指定目录、不抓取，仅清洗 + Word，成功后删原始 HTML（须目录内已有 raw）：

```
python wc-library.py --word-only -c -w --r
```

只开 `-w`（不写磁盘清洗）并 `--r`：从已有 `clean_` 或内存临时清洗生成 docx，成功后删除 raw（无 `--r-c` 时一般不删 `clean_`）：

```
python wc-library.py -w --r
```

仅处理、同上逻辑：

```
python wc-library.py --word-only -w --r
```

**`--r-c`（仅保留 docx）**：脚本会隐含按生成 Word 处理；成功后删除原始 HTML、`clean_` 前缀 HTML 及对应本地媒体目录。与 `-c` 同时出现时，后处理阶段**不会**再把清洗结果写入磁盘（`clean_disk` 被关闭），等价于「不落盘清洗 + 只出 docx」管线。

抓取结束后只留 docx（优先用已有 `clean_`，否则临时清洗再转 Word）：

```
python wc-library.py -w --r-c
```

输出目录已有一批 raw/`clean_`，不登录只跑一轮「只留 docx」：

```
python wc-library.py --word-only --r-c
```

指定目录并强制重写 docx、且删净 HTML 与清洗产物：

```
python wc-library.py --d backup/category --word-only --r-c -f
```

### 运行期行为

错误 URL 行追加到**当前输出根目录**下文件：

```
library_error_url.txt
```

清洗资源失败等记录追加到**仓库根**下文件：

```
clean_error_url.txt
```

文章页 GET 遇 HTTP 429 等重试与告警策略由 `src/wc-library.py` 内常量控制（如 `ARTICLE_429_RETRY_INTERVAL_SEC`、`ARTICLE_429_ALERT_ATTEMPT` 等，**非环境变量**；随笔侧列表 POST 的限流常量见上文「wc-essay / 运行期行为」）。
