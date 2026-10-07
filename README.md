# Python 实时对话运行器

用 Flask + 原生 HTML/CSS/JavaScript 实现的网页：粘贴 Python 代码后在独立子进程中执行，与它实时对话，并取回程序写入工作目录的输出文件。

## 启动（单条命令）

```bash
docker compose up --build
```

容器启动时会自动安装依赖（Flask），然后启动服务。

- 网页地址： <http://localhost:8000>
- 端口： `8000`

停止：`Ctrl+C` 或 `docker compose down`。

不使用 Docker 时也可以本地运行：

```bash
pip install -r requirements.txt
python app.py
```

## 功能

- **粘贴即运行**：代码写入临时文件，在独立子进程（`python -u`）中执行，启动后立即返回运行标识 `run_id`。
- **实时对话**：可向子进程标准输入发送一行文本，或关闭标准输入（EOF）；关闭后再次写入会被拒绝（HTTP 409）。
- **自动对话模式**：提交代码、步骤 JSON 数组和总时限（秒），后端按步骤自动等待指定 stdout/stderr 文本、发送单行、关闭标准输入，立即返回 `run_id`；详见下文「自动对话模式」。
- **取回输出文件**：`POST /run` 与 `POST /auto` 可带 `outputs` 文件名数组（省略为 `[]`）；运行结束时按声明顺序收集程序写入工作目录的文件（仅非符号链接的普通文件，每个最多 1 MiB），逐项报告可下载 / 缺失 / 非普通文件 / 超限 / 读取失败，页面列出状态并提供下载链接（不预览内容）；详见下文「取回输出文件」。
- **双路分栏输出**：stdout / stderr 分别按 UTF-8 **增量解码**并分栏增量呈现——未换行的片段也立即显示，跨读边界的多字节字符会被补齐，不丢字、不乱码。
- **互不阻塞**：两路输出各由独立线程读取；程序等待输入或一路无输出时，另一路输出、状态查询和终止请求仍可正常处理。
- **全站单运行**：同时仅允许一个未结束运行（含自动对话运行及其收尾阶段）；运行中再次提交会收到 409。
- **退出码最后公布**：正常退出与主动终止都会先排空两路输出、`wait()` 回收进程，之后才公布退出码并允许重新运行。
- **立即终止**：终止使用 SIGKILL，不等程序自行退出。
- **标识隔离**：旧 `run_id` 对新进程一律无效（409）；前端用轮次号 + run_id 双重校验，迟到回包不会混入新一轮页面。
- **状态即内存**：所有状态保存在内存中，服务重启即清空。

## 内置示例

页面提供五个一键载入的示例：

1. **等待输入** —— 回声程序，逐行读取并回复，输入 `quit` 退出。
2. **双路无换行输出** —— stdout / stderr 交替输出无换行片段，验证增量呈现与 UTF-8 拼接。
3. **EOF** —— `sys.stdin.read()` 阻塞到 EOF，点击「关闭标准输入」后统计收到的内容。
4. **终止** —— 无限循环每秒打印，只能通过「终止」结束。
5. **自动对话** —— 同时载入示例代码与步骤 JSON，点击「启动自动对话」即可看到完整自动交互。

## 自动对话模式

在「自动对话模式」区域填写**步骤 JSON 数组**和**总时限（秒）**，点击「启动自动对话」（或 `POST /auto`），后端启动子进程后立即返回 `run_id`，随后由脚本线程自动执行各步骤。

### 步骤格式

`steps` 为 JSON 数组，每个元素是一个对象，`action` 取值三种：

| action | 参数 | 说明 |
| --- | --- | --- |
| `wait` | `stream`: `"stdout"` 或 `"stderr"`；`text`: 非空字符串 | 等待指定一路输出中出现 `text` |
| `send` | `line`: 字符串（不含换行） | 向标准输入发送该行（自动附加换行） |
| `close` | 无 | 关闭标准输入（EOF） |

等待语义：

- **跨片段匹配**：输出事件可能把一句话切成多个片段，先在每路各自的缓冲里拼回再匹配，跨片段也能命中；
- **每路从上次匹配末尾继续**：同一 `stream` 上后一个 `wait` 从上一个 `wait` 匹配结束的位置继续找，不会重复命中同一段文本；
- **兼顾已到达及后续内容**：步骤开始前已经输出的文本（只要尚未被上次匹配越过）与等待期间新到的文本都参与匹配；
- **不消耗页面事件**：脚本只读取事件流，页面轮询 `/events` 不受影响，刷新页面可完整回放。

运行规则：

- 自动运行**占用同一单运行槽位**：期间 `POST /run`、`POST /auto` 返回 409，手动 `POST /input`、`POST /stdin/close` 也返回 409；
- 手动 `POST /terminate` 会**取消脚本**（停止后续动作）并 SIGKILL 终止进程，结果记为「已取消」；
- 发送阻塞（如超过管道容量的长行等待子进程读取）同样受**总时限**与**终止**控制；
- **成功**：全部步骤完成且进程退出码为 0；
- **失败**：总时限超时、写入失败（含孤立代理等无法 UTF-8 编码的行，以及发送等待异常——如服务打开文件过多使 stdin 描述符超出 `select` 的 1024 上限）、进程非零退出，或输出排空后仍有未完成步骤——结果注明失败位置（第几步，下标从 0 开始）与原因，并终止尚存进程；
- 结果与重跑都等到**输出排空、进程回收、脚本停止**之后：结果通过 `/events` 中 `stream: "auto"` 的事件公布（`phase: "step"` 为步骤进展，`phase: "result"` 为最终结果），此前槽位不释放；旧脚本只持有旧运行对象，操作不到新运行。

### 可执行样例

代码（粘贴到代码框）：

```python
import sys

print("READY", flush=True)
name = sys.stdin.readline().strip()
print(f"你好，{name}！", flush=True)
print("提示：stderr 上的文本也能等待", file=sys.stderr, flush=True)
for line in sys.stdin:
    line = line.rstrip("\n")
    if line == "quit":
        print("BYE", flush=True)
        break
    print("ECHO " + line, flush=True)
```

步骤 JSON（填入「步骤 JSON 数组」框，总时限 30 秒）：

```json
[
  {"action": "wait", "stream": "stdout", "text": "READY"},
  {"action": "send", "line": "世界"},
  {"action": "wait", "stream": "stdout", "text": "你好，世界"},
  {"action": "wait", "stream": "stderr", "text": "stderr 上的文本也能等待"},
  {"action": "send", "line": "hello"},
  {"action": "wait", "stream": "stdout", "text": "ECHO hello"},
  {"action": "send", "line": "quit"},
  {"action": "wait", "stream": "stdout", "text": "BYE"}
]
```

点击「启动自动对话」，预期页面依次显示各步骤进展，最终显示「自动对话成功：全部 8 步完成，退出码 0」。页面「自动对话」示例按钮可一键载入本样例。

用 `curl` 等价复现：

```bash
RUN=$(curl -s localhost:8000/auto -H 'Content-Type: application/json' -d '{
  "code": "import sys\nprint(\"READY\", flush=True)\nname = sys.stdin.readline().strip()\nprint(f\"你好，{name}！\", flush=True)",
  "steps": [
    {"action": "wait", "stream": "stdout", "text": "READY"},
    {"action": "send", "line": "世界"},
    {"action": "wait", "stream": "stdout", "text": "你好，世界"}
  ],
  "timeout": 30
}' | python3 -c "import sys,json;print(json.load(sys.stdin)['run_id'])")

# 轮询事件，直到看到 stream=auto、phase=result 的成功事件
curl -s "localhost:8000/events?run_id=$RUN&after=0"
```

配合「EOF」示例代码可演示 `close` 动作：

```json
[
  {"action": "wait", "stream": "stdout", "text": "请发送若干行"},
  {"action": "send", "line": "第一行"},
  {"action": "send", "line": "第二行"},
  {"action": "close"},
  {"action": "wait", "stream": "stdout", "text": "共收到"}
]
```

## 取回输出文件

程序写入**工作目录**（子进程的当前目录，即临时目录）的文件可以在运行结束后取回：启动时在 `outputs` 中声明文件名，结束后通过 HTTP 查清单、下载原始字节；页面「输出文件」区域也会列出每项状态与下载链接（不预览内容）。

### 声明规则

- `POST /run` 与 `POST /auto` 都可带 `outputs` 数组，省略为 `[]`；
- 最多 **10 个**互异的非空字符串；名称不得为 `.` 或 `..`，不得含斜杠、反斜杠及 NUL；
- 非法请求返回 **400**，不会启动也不会替换当前运行。

### 收集与状态

- 运行结束时（两路输出排空、进程回收之后，删除临时目录之前）按**声明顺序**收集原始字节；自动对话运行还要等脚本收尾公布结果后，才可下载或重跑——正常、失败和终止均适用；
- 仅收集**非符号链接的普通文件**，每个最多 **1 MiB**；
- 逐项状态：`ok` 可下载 / `missing` 缺失 / `not_regular` 非普通文件 / `too_large` 超过 1 MiB / `unreadable` 读取失败；文件问题**不影响**运行结果（退出码与自动对话结果不变）。

### 调用示例：生成二进制文件并取回

```bash
# 程序在工作目录写出 256 字节的二进制文件 result.bin
RUN=$(curl -s localhost:8000/run -H 'Content-Type: application/json' -d '{
  "code": "with open(\"result.bin\", \"wb\") as f:\n    f.write(bytes(range(256)))\nprint(\"written\", flush=True)",
  "outputs": ["result.bin"]
}' | python3 -c "import sys,json;print(json.load(sys.stdin)['run_id'])")

# 等运行结束：看到 meta 退出事件时文件已收集（自动对话运行须等到 auto 结果事件）
curl -s "localhost:8000/events?run_id=$RUN&after=0"

# 查看清单：每项的名称、状态与大小
curl -s "localhost:8000/files?run_id=$RUN"
# => {"run_id": "…", "files": [{"name": "result.bin", "status": "ok", "size": 256}]}

# 下载可用项（原始字节，按 Content-Disposition 存为 result.bin）
curl -sOJ "localhost:8000/files/result.bin?run_id=$RUN"
python3 -c "assert open('result.bin','rb').read() == bytes(range(256)); print('OK')"
```

未完成（含自动脚本收尾中）查询或下载返回 **409**；当前运行未声明或不可下载的名称返回 **404**；旧 `run_id` 一律返回 **409**。

## 长行复现（回归验证）

子进程**先持续写出大量 stdout、再读取 stdin** 时，发送一条超过管道容量（Linux 默认 64 KiB）的合法单行，可以复现输入同步问题：修复前发送请求与输出互相等待，两路输出停止刷新、`GET /events` 不再返回。

复现步骤：

1. 启动服务并打开 <http://localhost:8000>，粘贴运行下面的代码（输出总量远超管道容量，持续约 25 秒，随后进入读输入）：

   ```python
   import sys
   import time

   # 先持续写出大量 stdout：总量远超 64 KiB 管道容量
   for i in range(500):
       print(f"burst {i}: " + "y" * 300, flush=True)
       time.sleep(0.05)

   # 再读取 stdin：在刷输出期间发送一条超过 64 KiB 的长行
   print("输出完毕，等待长行…", flush=True)
   line = sys.stdin.readline()
   print(f"收到 {len(line.rstrip(chr(10)))} 个字符", flush=True)
   ```

2. 在另一个终端生成一条 200 KiB 的长行并复制：

   ```bash
   python3 -c "print('x' * 200000)"
   ```

3. 在程序仍在刷输出时，把长行粘贴到页面输入框并发送。

修复后的预期行为：

- 发送等待期间，stdout / stderr 两路输出持续被读取、页面持续刷新，`GET /events` 正常返回；
- 程序开始读输入后收到**完整的整行及末尾换行**——页面最终打印 `收到 200000 个字符`，不截断、不限长、不丢输出；
- 发送尚在等待时点击「终止」，子进程被立即 SIGKILL，等待中的发送按约定返回 409 结束；
- 发送成功后点击「关闭标准输入 (EOF)」，该行完整送达后 EOF 才生效，此后再写入返回 409；
- 运行结束后两路输出先排空、进程回收，再公布退出码并允许重跑。

## 发送异常复现与重跑（回归验证）

要发送的单行若含**孤立代理字符**（lone surrogate，在 JSON 中以转义形式出现，如 `"\ud800"`），无法编码为 UTF-8。修复前脚本线程会因编码异常直接退出：结果始终为空，子进程与单运行槽位无人回收，超时或「终止」后槽位也不释放，再也无法重跑。修复后该步骤按发送失败处理：标记当前步骤失败（`index` 从 0 开始，附原因）、停止后续动作、终止尚存进程，排空输出并回收进程后才公布一次失败结果、释放槽位。

复现步骤（服务已启动，见上文「启动」）：

1. 启动自动对话：程序打印 `READY` 后阻塞在读输入（不会自行退出）；步骤等待 `READY`、发送孤立代理单行、再等待永远收不到的 `GOT`：

   ```bash
   RUN=$(curl -s localhost:8000/auto -H 'Content-Type: application/json' -d '{
     "code": "import sys\nprint(\"READY\", flush=True)\nline = sys.stdin.readline()\nprint(\"GOT \" + line.strip(), flush=True)",
     "steps": [
       {"action": "wait", "stream": "stdout", "text": "READY"},
       {"action": "send", "line": "\ud800"},
       {"action": "wait", "stream": "stdout", "text": "GOT"}
     ],
     "timeout": 30
   }' | python3 -c "import sys,json;print(json.load(sys.stdin)['run_id'])")
   ```

2. 轮询事件，直到出现 `stream=auto`、`phase=result` 的失败事件：

   ```bash
   curl -s "localhost:8000/events?run_id=$RUN&after=0"
   ```

   预期行为：

   - 步骤进展停在第 2 步（`index: 1`，从 0 开始），后续「等待 `GOT`」不再执行；
   - 子进程被 SIGKILL 终止（`exit_code: -9`）；stdout/stderr 排空、进程回收（`meta` 退出事件在先）之后，才公布**一次**失败结果：

     ```json
     {"phase": "result", "state": "failed", "index": 1, "total": 3,
      "reason": "写入失败：行内容无法编码为 UTF-8（含孤立代理字符）", "exit_code": -9}
     ```

   - 即使进程恰在发送失败后自行正常退出（退出码 0），结果仍为失败，不会改判成功。

3. 槽位已释放，可立即重跑：

   ```bash
   curl -s localhost:8000/status        # busy 为 false

   # 把孤立代理换成正常行重跑：返回新 run_id，最终成功
   RUN2=$(curl -s localhost:8000/auto -H 'Content-Type: application/json' -d '{
     "code": "import sys\nprint(\"READY\", flush=True)\nline = sys.stdin.readline()\nprint(\"GOT \" + line.strip(), flush=True)",
     "steps": [
       {"action": "wait", "stream": "stdout", "text": "READY"},
       {"action": "send", "line": "hello"},
       {"action": "wait", "stream": "stdout", "text": "GOT hello"}
     ],
     "timeout": 30
   }' | python3 -c "import sys,json;print(json.load(sys.stdin)['run_id'])")
   curl -s "localhost:8000/events?run_id=$RUN2&after=0"
   # 预期结果事件：{"phase": "result", "state": "success", "index": null, "total": 3, "exit_code": 0}
   ```

   旧运行的脚本线程已随结果公布停止，只持有旧运行对象，影响不到新运行。

## 高描述符发送等待异常复现与重跑（回归验证）

自动对话发送**超过管道容量的合法长行**、管道已满而子进程暂不读取时，发送线程会在 `select` 上等待管道可写。Linux 的 `select` 只支持文件描述符小于 `FD_SETSIZE`（1024）的句柄：当服务已经打开大量文件（描述符排到 1024 及以上），子进程 stdin 的描述符超出该范围后，管道满时等待会抛 `ValueError: filedescriptor out of range in select()`。

修复前该异常逃出脚本线程：驱动直接退出、**不公布任何结果、不终止进程**，于是单运行槽位被永久占用——超过总时限或手动「终止」都无法重跑。修复后发送等待异常按统一失败收尾：记录当前步骤下标（从 0 开始）与原因、停止后续步骤、终止尚存进程；两路输出排空、进程回收、脚本收尾完成后才公布**唯一**失败结果并释放槽位。取消仍然优先；进程即使恰在此前正常退出（退出码 0）也不得改判成功。不缩短长行、不提前拒绝合法输入、不直接清除 busy。

### 复现前提：让服务持有大量文件描述符

不改动 `docker compose up --build` 的单命令启动方式；下面的本地启动方式仅用于复现（先把进程软限制抬高到硬限制，再占住约 1024 个低位描述符，之后子进程管道描述符必然 ≥ 1024）：

```bash
python3 - <<'PY'
import os, resource, sys
soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
resource.setrlimit(resource.RLIMIT_NOFILE, (min(65536, hard), hard))
# 占住低位描述符，使后续子进程 stdin/stdout/stderr 管道 fd >= 1024
_fds = [os.open(os.devnull, os.O_RDONLY) for _ in range(1024)]
sys.argv = ["app.py"]
exec(open("app.py", encoding="utf-8").read(), {"__name__": "__main__"})
PY
```

> 若环境的硬限制本身低于 1030（如部分容器 `ulimit -Hn` 为 1024），请先在宿主机放开（如 `ulimit -Hn 65536` 后再启动；容器可在 `docker run` 上加 `--ulimit nofile=65536:65536`）。Docker Compose 的默认启动、网页地址与端口均不变（仍为 <http://localhost:8000>）。

### 1. 提交会触发发送等待的自动对话

程序先输出 `READY`，然后**暂不读取 stdin**（长时间睡眠）；步骤等待 `READY`、发送一条 200 KiB 的合法长行写满管道、再等待永远不会出现的 `DONE`。长行较长（超过单条命令行参数约 128 KiB 的上限），先用脚本生成请求体文件，再让 curl 从文件读取（长行不缩短、不提前拒绝）：

```bash
python3 - <<'PY'
import json
payload = {
    "code": "import sys,time\nprint(\"READY\", flush=True)\ntime.sleep(300)\n",
    "steps": [
        {"action": "wait", "stream": "stdout", "text": "READY"},
        {"action": "send", "line": "x" * 200000},
        {"action": "wait", "stream": "stdout", "text": "DONE"},
    ],
    "timeout": 60,
}
with open("payload.json", "w", encoding="utf-8") as f:
    json.dump(payload, f)
PY

RUN=$(curl -s localhost:8000/auto -H 'Content-Type: application/json' \
  -d @payload.json | python3 -c "import sys,json;print(json.load(sys.stdin)['run_id'])")
echo "$RUN"
```

### 2. 轮询事件，看到唯一失败结果（修复前永远等不到 result）

```bash
curl -s "localhost:8000/events?run_id=$RUN&after=0"
```

预期行为：

- 步骤进展停在第 2 步（`index: 1`，下标从 0 开始），后续「等待 `DONE`」不再执行；
- 子进程被 SIGKILL 终止；stdout/stderr 排空、进程回收（`meta` 退出事件在先）之后，才公布**一次**失败结果，长行本身不被截断或拒绝：

  ```json
  {"phase": "result", "state": "failed", "index": 1, "total": 3,
   "reason": "写入失败：发送等待时 select 失败（stdin 描述符 1028 超出 select 支持范围）：filedescriptor out of range in select()",
   "exit_code": -9}
  ```

  （描述符编号随环境变化，含义一致。）
- 即使在修复后的收尾之前进程恰自行正常退出（退出码 0），结果仍为失败，不改判成功。

### 3. 超过时限或手动终止不再占槽，随后可立即重跑

修复前，即便等到 `timeout` 耗尽或对旧 `run_id` 调用终止，`/status` 的 `busy` 始终为 `true`，再次提交一直返回 409。修复后失败公布即释放槽位：

```bash
# 复现期间也可以先手动终止，结果同样会按「已取消」收尾并释放槽位：
# curl -s localhost:8000/terminate -H 'Content-Type: application/json' \
#   -d "{\"run_id\": \"$RUN\"}"

curl -s localhost:8000/status        # busy 为 false

# 换成程序会读输入的正常交互重跑：返回新 run_id，最终成功
RUN2=$(curl -s localhost:8000/auto -H 'Content-Type: application/json' -d '{
  "code": "import sys\nprint(\"READY\", flush=True)\nprint(\"GOT \" + sys.stdin.readline().strip(), flush=True)",
  "steps": [
    {"action": "wait", "stream": "stdout", "text": "READY"},
    {"action": "send", "line": "hello"},
    {"action": "wait", "stream": "stdout", "text": "GOT hello"}
  ],
  "timeout": 30
}' | python3 -c "import sys,json;print(json.load(sys.stdin)['run_id'])")
curl -s "localhost:8000/events?run_id=$RUN2&after=0"
# 预期结果事件：{"phase": "result", "state": "success", "index": null, "total": 3, "exit_code": 0}
```

旧运行的脚本线程已随结果公布停止，只持有旧运行对象，操作不到新运行；旧 `run_id` 对新进程仍返回 409。

## 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/run` | 启动手动运行：`{code}`，可带 `outputs` 声明输出文件；立即返回 `run_id`；已有未结束运行时返回 409，参数非法返回 400 |
| POST | `/auto` | 启动自动对话运行：`{code, steps, timeout}`，可带 `outputs`；立即返回 `run_id`；槽位占用时返回 409，参数非法返回 400 |
| GET | `/events?run_id=…&after=…` | 长轮询增量事件（stdout / stderr / meta 退出码 / auto 步骤进展与结果） |
| POST | `/input` | 发送一行文本；stdin 已关闭或自动对话进行中时返回 409 |
| POST | `/stdin/close` | 关闭标准输入（EOF）；自动对话进行中时返回 409 |
| POST | `/terminate` | SIGKILL 立即终止；自动对话运行同时取消脚本 |
| GET | `/status` | 查询当前运行状态（含自动对话状态与结果） |
| GET | `/files?run_id=…` | 输出文件清单（按声明顺序列出名称、状态、大小）；运行未结束或旧 `run_id` 返回 409 |
| GET | `/files/<名称>?run_id=…` | 下载某个可下载项的原始字节；未声明或不可下载返回 404，未结束或旧标识返回 409 |

## 安全说明

**仅供本机可信代码使用**：子进程与 Flask 同权限运行，仅适用于不派生进程的标准库代码，**没有任何安全沙箱**。请勿暴露到公网或运行不可信代码。
