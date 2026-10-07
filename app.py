"""与 Python 程序实时对话的网页后端。

- 粘贴的代码写入临时文件，在独立子进程中执行，启动后立即返回运行标识。
- stdout / stderr 各由一个线程读取，按 UTF-8 增量解码（跨读边界的
  多字节字符会被补齐，不丢字、不乱码），未换行的片段也立即入队。
- stdin 的写入与关闭由独立的锁串行化，阻塞写不占用事件锁：超过管道
  容量的长行会等待子进程开始读取，期间两路输出照常排空、/events 照常
  返回；整行连同末尾换行完整送达，不截断、不限长。终止会立即杀死子
  进程，等待中的写入按约定以 409 结束。
- 自动对话模式：提交代码、步骤 JSON 数组与总时限（秒），后端按步骤
  自动等待指定 stdout/stderr 文本、发送单行、关闭 stdin，立即返回
  run_id。等待跨事件片段匹配，每路从上次匹配末尾继续，已到达与后续
  内容都参与匹配；脚本只读取、不消费页面事件流。自动运行占用同一单
  运行槽位：期间手动输入与 EOF 返回 409，手动终止取消脚本、停止后续
  动作；发送阻塞同样受总时限与终止控制。全部步骤完成且退出码为 0 才
  成功；超时、写入失败（含孤立代理等无法 UTF-8 编码的行）、发送等待
  异常（如服务打开大量文件导致 stdin 描述符超出 select 支持范围）、
  非零退出或排空后仍有未完成步骤时失败，注明失败位置（从 0 开始的
  步骤下标）与原因并终止尚存进程。任何发送等待异常都按统一失败收尾：
  记录当前步骤下标与原因、停止后续步骤、终止尚存进程，结果公布与重跑都
  等到输出排空、进程回收、脚本停止之后；旧脚本只持有旧运行对象，
  操作不到新运行。
- 输出文件取回：/run 与 /auto 可带 outputs 文件名数组（省略为 []；最多
  10 个互异非空名称，不得为 . 或 ..，不得含斜杠、反斜杠及 NUL；非法
  请求 400，不启动也不替换当前运行）。运行结束时——两路输出排空、进程
  回收之后，删除临时目录之前——按声明顺序收集工作目录里的同名文件：
  仅收集非符号链接的普通文件，每个最多 1 MiB，逐项记录可下载 / 缺失 /
  非普通文件 / 超限 / 读取失败，文件问题不影响运行结果。自动对话运行
  须再等脚本收尾公布结果；此后 GET /files 查清单、GET /files/<名称>
  下载原始字节，未完成 409，未声明或不可下载 404，旧 run_id 一律 409。
- 全站同时仅一个未结束运行；旧 run_id 对新进程一律无效。
- 正常退出与主动终止（SIGKILL，不等程序自行退出）都会先排空两路输出、
  回收进程，之后才公布退出码并允许重跑。
- 状态全部在内存中，进程重启即清空。仅供本机可信代码使用，无安全沙箱。
"""

import codecs
import io
import os
import select
import shutil
import stat
import subprocess
import sys
import tempfile
import threading
import time
import uuid

from flask import Flask, jsonify, request, send_file, send_from_directory

app = Flask(__name__, static_folder="static", static_url_path="")

STATE_RUNNING = "running"
STATE_FINISHED = "finished"

CHUNK = 4096               # 每次从管道读取的原始字节数
LONG_POLL_SECONDS = 20     # /events 长轮询最长等待

MAX_STEPS = 200            # 自动对话最多步骤数
MAX_WAIT_TEXT = 10000      # 单条等待文本的最大长度（字符）
MAX_LINE = 1000000         # 单行发送的最大长度（字符）
MAX_TIMEOUT = 3600         # 自动对话总时限上限（秒）

MAX_OUTPUTS = 10               # 最多声明的输出文件数
MAX_OUTPUT_SIZE = 1024 * 1024  # 单个输出文件最多收集的字节数（1 MiB）

_state_lock = threading.Lock()
_current_run = None        # 当前（可能是唯一未结束的）运行


class Run:
    """一次子进程运行：两路输出增量解码成事件流，退出码最后公布。

    outputs 为声明的输出文件名（程序写入工作目录、需要取回的文件）；
    运行结束时按声明顺序收集原始字节，逐项状态存入 files。
    """

    def __init__(self, code, steps=None, timeout=None, outputs=None):
        self.id = uuid.uuid4().hex
        self.events = []            # [{seq, stream, data}]，seq 从 1 递增
        self.seq = 0
        self.cond = threading.Condition()
        # 串行化 stdin 的写入与关闭：保证一行的字节连续完整、关闭不插队。
        # 阻塞写只持有它、不持有 cond，因此不会挡住输出泵和 /events。
        self.stdin_lock = threading.Lock()
        self.state = STATE_RUNNING
        self.exit_code = None
        self.stdin_closed = False
        self.auto = None            # 自动对话脚本（手动模式为 None）
        self.outputs = outputs or []    # 声明的输出文件名（按声明顺序收集）
        self.files = []             # [{name, status, size, data}]，结束后填充
        self.files_ready = False    # 输出文件是否已收集完毕

        self.tmpdir = tempfile.mkdtemp(prefix="webrun_")
        self.script = os.path.join(self.tmpdir, "main.py")
        with open(self.script, "w", encoding="utf-8") as f:
            f.write(code)

        env = dict(os.environ, PYTHONUNBUFFERED="1", PYTHONIOENCODING="utf-8")
        self.proc = subprocess.Popen(
            [sys.executable, "-u", self.script],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize=0,
            cwd=self.tmpdir,
            env=env,
        )
        self.pumps = [
            threading.Thread(target=self._pump,
                             args=("stdout", self.proc.stdout), daemon=True),
            threading.Thread(target=self._pump,
                             args=("stderr", self.proc.stderr), daemon=True),
        ]
        for t in self.pumps:
            t.start()
        threading.Thread(target=self._reaper, daemon=True).start()
        if steps is not None:
            self.auto = AutoScript(self, steps, timeout)
            self.auto.start()

    def _emit(self, stream, data):
        with self.cond:
            self.seq += 1
            self.events.append({"seq": self.seq, "stream": stream, "data": data})
            self.cond.notify_all()

    def _pump(self, stream, pipe):
        # 增量解码器：多字节 UTF-8 字符被两次 read 切开时会先缓存、
        # 等后半截到达后再输出，因此不丢字也不乱码；非法字节替换为 U+FFFD。
        decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
        try:
            while True:
                chunk = os.read(pipe.fileno(), CHUNK)
                if not chunk:          # EOF：子进程关闭了这一路
                    break
                text = decoder.decode(chunk)
                if text:
                    self._emit(stream, text)
            tail = decoder.decode(b"", final=True)
            if tail:
                self._emit(stream, tail)
        except OSError:
            pass
        finally:
            try:
                pipe.close()
            except OSError:
                pass

    def _reaper(self):
        # 先等两路输出都读到 EOF（排空），再 wait() 回收进程，
        # 全部完成后才公布退出码——保证退出码之后不会再有迟到的输出。
        for t in self.pumps:
            t.join()
        code = self.proc.wait()
        # 删除临时目录之前按声明顺序收集输出文件；文件问题不影响运行结果
        files = self._collect_files()
        with self.cond:
            self.files = files
            self.files_ready = True
            self.exit_code = code
            self.state = STATE_FINISHED
            self.seq += 1
            self.events.append({"seq": self.seq, "stream": "meta",
                                "data": {"exit_code": code}})
            self.cond.notify_all()
        shutil.rmtree(self.tmpdir, ignore_errors=True)

    def _collect_files(self):
        """按声明顺序收集输出文件的原始字节，返回逐项结果列表。

        只收集非符号链接的普通文件，每个最多 1 MiB；缺失、非普通文件、
        超限或读取失败都逐项记录（data 为 None），绝不影响运行结果。
        须在删除临时目录之前调用。
        """
        files = []
        for name in self.outputs:
            entry = {"name": name, "status": "ok", "size": 0, "data": None}
            path = os.path.join(self.tmpdir, name)
            try:
                # lstat 不跟随符号链接：符号链接一律按非普通文件处理
                st = os.lstat(path)
                if not stat.S_ISREG(st.st_mode):
                    entry["status"] = "not_regular"
                elif st.st_size > MAX_OUTPUT_SIZE:
                    entry["status"] = "too_large"
                else:
                    with open(path, "rb") as f:
                        data = f.read(MAX_OUTPUT_SIZE + 1)
                    if len(data) > MAX_OUTPUT_SIZE:   # 读取期间文件被写大
                        entry["status"] = "too_large"
                    else:
                        entry["size"] = len(data)
                        entry["data"] = data
            except FileNotFoundError:
                entry["status"] = "missing"
            except OSError:
                entry["status"] = "unreadable"
            files.append(entry)
        return files

    def slot_busy(self):
        """单运行槽位是否仍被占用：进程未结束，或自动脚本尚未公布结果。"""
        if self.state == STATE_RUNNING:
            return True
        return self.auto is not None and self.auto.state == "running"

    def write_line(self, line):
        """把整行（含末尾换行）写入子进程 stdin。

        成功返回 None，失败返回错误信息。超过管道容量的长行会阻塞到
        子进程开始读取；阻塞期间只持有 stdin_lock，输出泵与 /events
        照常工作。子进程退出或被杀死时，阻塞的写会以 BrokenPipeError
        解除，按约定转为拒绝。含孤立代理等无法 UTF-8 编码的字符时
        不抛异常，按发送失败返回错误信息。
        """
        try:
            payload = (line + "\n").encode("utf-8")
        except UnicodeEncodeError:
            return "行内容无法编码为 UTF-8（含孤立代理字符）"
        with self.stdin_lock:
            with self.cond:
                if self.state != STATE_RUNNING:
                    return "运行已结束"
                if self.stdin_closed:
                    return "标准输入已关闭，写入被拒绝"
            try:
                # 单次 write 可能只写进一部分（如被信号打断），循环写满
                # 为止：程序开始读输入后必收到整行及末尾换行，绝不截断。
                view = memoryview(payload)
                while view:
                    n = self.proc.stdin.write(view)
                    if n is None:      # 阻塞式文件不会发生，防御性处理
                        n = len(view)
                    view = view[n:]
                self.proc.stdin.flush()
            except (BrokenPipeError, OSError):
                return "子进程已关闭标准输入"
        return None

    def write_line_auto(self, line, deadline, cancel_event):
        """自动对话专用写：与 write_line 同约定，但阻塞受总时限与取消控制。

        把管道切为非阻塞后循环写：管道满时在 select 上短暂等待，每轮
        都检查取消事件与总时限，因此手动终止或超时都能及时打断发送；
        整行连同末尾换行仍然完整送达，不截断。含孤立代理等无法
        UTF-8 编码的字符时不抛异常，按发送失败返回错误信息——
        由脚本线程把当前步骤标记为失败并走统一收尾。

        select 只支持小于 FD_SETSIZE（Linux 为 1024）的描述符；当服务
        已打开大量文件、子进程 stdin 描述符超出该范围时，管道满后的
        select 会抛 ValueError。该异常同样按发送失败返回错误信息，
        绝不让异常逃出脚本线程（否则结果无人公布、槽位永久占用）。
        """
        try:
            payload = (line + "\n").encode("utf-8")
        except UnicodeEncodeError:
            return "行内容无法编码为 UTF-8（含孤立代理字符）"
        with self.stdin_lock:
            with self.cond:
                if self.state != STATE_RUNNING:
                    return "运行已结束"
                if self.stdin_closed:
                    return "标准输入已关闭，写入被拒绝"
            fd = self.proc.stdin.fileno()
            # 仅自动运行使用本方法（期间手动输入被 409 拒绝），
            # 改变阻塞模式不影响手动运行的写路径。
            os.set_blocking(fd, False)
            view = memoryview(payload)
            while view:
                if cancel_event.is_set():
                    return "已取消"
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return "超过总时限"
                try:
                    n = os.write(fd, view)
                    view = view[n:]
                except BlockingIOError:
                    # 管道已满：等子进程读出一些再写，同时轮询取消与时限
                    try:
                        select.select([], [fd], [], min(remaining, 0.2))
                    except ValueError as exc:
                        # 服务打开文件过多使 stdin 描述符超出 select 的
                        # FD_SETSIZE 范围（如 fd >= 1024）：发送等待无法
                        # 继续，按发送失败交脚本线程统一收尾，绝不吞线程。
                        return (
                            "发送等待时 select 失败（stdin 描述符 %d 超出 "
                            "select 支持范围）：%s" % (fd, exc))
                    except OSError:
                        return "子进程已关闭标准输入"
                except OSError:
                    return "子进程已关闭标准输入"
        return None

    def close_stdin(self):
        """关闭子进程 stdin（发送 EOF）。

        与写入同用 stdin_lock：进行中的整行写完后关闭才生效，
        生效之后的写入一律被拒绝。
        """
        with self.stdin_lock:
            with self.cond:
                if not self.stdin_closed:
                    self.stdin_closed = True
                    try:
                        self.proc.stdin.close()   # 向子进程发送 EOF
                    except OSError:
                        pass


class AutoScript:
    """自动对话脚本：按步骤等待输出文本 / 发送单行 / 关闭 stdin。

    - 等待在每路各自累积的文本上做子串匹配：输出事件可能把一句话切成
      多个片段，先拼回再匹配，因此跨片段也能命中；每路从上次匹配末尾
      继续，已到达与后续到达的内容都参与匹配。
    - 只读取 run.events（页面事件流），用独立游标遍历，从不删除或
      消费事件，页面轮询不受影响。
    - 全程受总时限与取消事件控制；结果（成功/失败/取消）在输出排空、
    进程回收之后才公布，公布前槽位不释放、不能重跑。
    """

    def __init__(self, run, steps, timeout):
        self.run = run
        self.steps = steps
        self.timeout = timeout
        self.deadline = time.monotonic() + timeout
        self.cancel_event = threading.Event()
        self.state = "running"      # running → success / failed / cancelled
        self.result = None          # 最终结果事件内容（供 /status 回放）
        self.buffers = {"stdout": "", "stderr": ""}   # 各路已到达文本
        self.matched = {"stdout": 0, "stderr": 0}     # 各路已匹配到的位置
        self.cursor = 0             # 只读游标：run.events 的遍历位置
        self.thread = threading.Thread(target=self._main, daemon=True)

    def start(self):
        self.thread.start()

    def cancel(self):
        self.cancel_event.set()

    def _drain_locked(self):
        """把新到的 stdout/stderr 事件并入各路缓冲；须持有 run.cond。"""
        events = self.run.events
        while self.cursor < len(events):
            ev = events[self.cursor]
            self.cursor += 1
            if ev["stream"] in self.buffers:
                self.buffers[ev["stream"]] += ev["data"]
        # 已匹配前缀不会再被搜索，裁掉以防缓冲无限增长
        for stream in self.buffers:
            cut = self.matched[stream]
            if cut > 65536:
                self.buffers[stream] = self.buffers[stream][cut:]
                self.matched[stream] = 0

    def _progress(self, index, desc):
        self.run._emit("auto", {"phase": "step", "index": index,
                                "total": len(self.steps), "desc": desc})

    @staticmethod
    def _shown(text):
        return text if len(text) <= 60 else text[:60] + "…"

    def _wait_text(self, index, stream, text):
        """等待指定一路输出出现 text；命中返回 None，否则返回失败原因。"""
        shown = self._shown(text)
        self._progress(index, "等待 %s 出现「%s」" % (stream, shown))
        run = self.run
        with run.cond:
            while True:
                self._drain_locked()
                # 从该路上一次匹配末尾继续找；已到达内容都在缓冲里
                pos = self.buffers[stream].find(text, self.matched[stream])
                if pos >= 0:
                    self.matched[stream] = pos + len(text)
                    return None
                if self.cancel_event.is_set():
                    return "已取消"
                remaining = self.deadline - time.monotonic()
                if remaining <= 0:
                    return "超过总时限 %g 秒，%s 未出现「%s》" % (
                        self.timeout, stream, shown)
                if run.state == STATE_FINISHED:
                    # 两路输出已排空、进程已回收，文本不会再来了
                    return "输出已排空，%s 始终未出现「%s」" % (stream, shown)
                run.cond.wait(timeout=min(0.2, remaining))

    def _await_exit(self):
        """全部步骤完成后，在总时限内等进程退出（FINISHED 即已排空+回收）。"""
        run = self.run
        with run.cond:
            while run.state != STATE_FINISHED:
                if self.cancel_event.is_set():
                    return "已取消"
                remaining = self.deadline - time.monotonic()
                if remaining <= 0:
                    return "超过总时限 %g 秒，进程仍未退出" % self.timeout
                run.cond.wait(timeout=min(0.2, remaining))
        return None

    def _wait_finished(self):
        """失败/取消后：进程已被 SIGKILL，等 reaper 排空输出并回收进程。"""
        run = self.run
        with run.cond:
            while run.state != STATE_FINISHED:
                run.cond.wait(timeout=0.2)

    def _main(self):
        run = self.run
        failure = None              # (步骤下标或 None, 原因)
        cancelled = False
        at = None                   # 取消时所在步骤（None 表示退出等待阶段）
        try:
            for i, step in enumerate(self.steps):
                at = i
                action = step["action"]
                err = None
                if action == "wait":
                    err = self._wait_text(i, step["stream"], step["text"])
                elif action == "send":
                    line = step["line"]
                    self._progress(i, "发送单行「%s」" % self._shown(line))
                    if self.cancel_event.is_set():
                        err = "已取消"
                    else:
                        err = run.write_line_auto(line, self.deadline,
                                                  self.cancel_event)
                        if err is not None and err != "已取消":
                            err = "写入失败：" + err
                else:               # action == "close"
                    self._progress(i, "关闭标准输入（EOF）")
                    if self.cancel_event.is_set():
                        err = "已取消"
                    else:
                        run.close_stdin()
                if err is not None:
                    # 取消优先于失败：终止与超时/写入失败同时发生时记为取消
                    if self.cancel_event.is_set():
                        cancelled = True
                    else:
                        failure = (i, err)
                    break
            if not cancelled and failure is None:
                at = None
                self._progress(None, "全部步骤完成，等待进程退出")
                err = self._await_exit()
                if err is not None:
                    if self.cancel_event.is_set():
                        cancelled = True
                    else:
                        failure = (None, err)
        except Exception as exc:
            # 发送等待等任何意外异常都不允许杀死脚本线程：否则结果永不
            # 公布、进程不被回收、单运行槽位永久占用、超时与手动终止都
            # 救不回来。按当前步骤统一失败收尾。
            if self.cancel_event.is_set():
                cancelled = True
            else:
                failure = (at, "脚本异常：%s: %s" % (
                    type(exc).__name__, exc))
        if cancelled or failure is not None:
            # 终止尚存进程（已退出的忽略），并等输出排空、进程回收
            try:
                run.proc.kill()
            except OSError:
                pass
            self._wait_finished()
        # 到此输出已排空、进程已回收；公布结果后脚本结束，槽位随之释放。
        # 取消优先；失败（含等待异常）一旦记录，进程即使恰在此前正常
        # 退出（退出码 0）也不得改判成功。
        with run.cond:
            if cancelled:
                self.state = "cancelled"
                self.result = {"phase": "result", "state": "cancelled",
                               "index": at, "total": len(self.steps)}
            elif failure is not None:
                self.state = "failed"
                self.result = {"phase": "result", "state": "failed",
                               "index": failure[0], "total": len(self.steps),
                               "reason": failure[1], "exit_code": run.exit_code}
            elif run.exit_code != 0:
                self.state = "failed"
                self.result = {"phase": "result", "state": "failed",
                               "index": None, "total": len(self.steps),
                               "reason": "进程退出码为 %s" % run.exit_code,
                               "exit_code": run.exit_code}
            else:
                self.state = "success"
                self.result = {"phase": "result", "state": "success",
                               "index": None, "total": len(self.steps),
                               "exit_code": run.exit_code}
            run.seq += 1
            run.events.append({"seq": run.seq, "stream": "auto",
                               "data": self.result})
            run.cond.notify_all()


def _validate_steps(steps):
    """校验步骤数组；合法返回 None，否则返回错误信息。"""
    if not isinstance(steps, list) or not steps:
        return "steps 必须是非空数组"
    if len(steps) > MAX_STEPS:
        return "步骤过多（最多 %d 个）" % MAX_STEPS
    for i, step in enumerate(steps):
        where = "步骤 %d：" % (i + 1)
        if not isinstance(step, dict):
            return where + "必须是对象"
        action = step.get("action")
        if action == "wait":
            if step.get("stream") not in ("stdout", "stderr"):
                return where + "stream 必须是 stdout 或 stderr"
            text = step.get("text")
            if not isinstance(text, str) or not text:
                return where + "text 必须是非空字符串"
            if len(text) > MAX_WAIT_TEXT:
                return where + "text 过长（最多 %d 字符）" % MAX_WAIT_TEXT
        elif action == "send":
            line = step.get("line")
            if not isinstance(line, str):
                return where + "line 必须是字符串"
            if "\n" in line or "\r" in line:
                return where + "line 不能包含换行（每次只发送单行）"
            if len(line) > MAX_LINE:
                return where + "line 过长（最多 %d 字符）" % MAX_LINE
        elif action == "close":
            pass
        else:
            return where + "未知 action（支持 wait / send / close）"
    return None


def _validate_outputs(outputs):
    """校验 outputs 输出文件声明；合法（含省略为 None）返回 None。"""
    if outputs is None:
        return None
    if not isinstance(outputs, list):
        return "outputs 必须是文件名数组"
    if len(outputs) > MAX_OUTPUTS:
        return "输出文件过多（最多 %d 个）" % MAX_OUTPUTS
    seen = set()
    for i, name in enumerate(outputs):
        where = "输出文件 %d：" % (i + 1)
        if not isinstance(name, str) or not name:
            return where + "名称必须是非空字符串"
        if name in (".", ".."):
            return where + "名称不能是 . 或 .."
        if "/" in name or "\\" in name or "\x00" in name:
            return where + "名称不能包含斜杠、反斜杠或 NUL"
        if name in seen:
            return where + "名称重复"
        seen.add(name)
    return None


def _get_run(run_id):
    """只有与当前运行匹配的 run_id 才有效；旧标识控制不了新进程。"""
    with _state_lock:
        run = _current_run
    if run is None or run.id != run_id:
        return None
    return run


def _slot_busy():
    """单运行槽位是否被占用：进程未结束，或自动脚本尚未公布结果。"""
    run = _current_run
    return run is not None and run.slot_busy()


def _auto_info(run):
    auto = run.auto
    if auto is None:
        return None
    return {"state": auto.state, "total": len(auto.steps),
            "result": auto.result}


@app.route("/")
def index():
    return send_from_directory("static", "index.html")


@app.post("/run")
def start_run():
    global _current_run
    data = request.get_json(force=True, silent=True) or {}
    code = data.get("code", "")
    if not code.strip():
        return jsonify({"error": "代码为空"}), 400
    error = _validate_outputs(data.get("outputs"))
    if error is not None:
        return jsonify({"error": error}), 400
    with _state_lock:
        if _slot_busy():
            return jsonify({"error": "已有运行未结束，请先终止"}), 409
        run = Run(code, outputs=data.get("outputs") or [])
        _current_run = run
    # 子进程已启动，立即返回运行标识
    return jsonify({"run_id": run.id})


@app.post("/auto")
def start_auto():
    """自动对话模式：代码 + 步骤数组 + 总时限（秒），立即返回 run_id。"""
    global _current_run
    data = request.get_json(force=True, silent=True) or {}
    code = data.get("code", "")
    if not isinstance(code, str) or not code.strip():
        return jsonify({"error": "代码为空"}), 400
    error = _validate_steps(data.get("steps"))
    if error is not None:
        return jsonify({"error": error}), 400
    timeout = data.get("timeout")
    if (not isinstance(timeout, (int, float)) or isinstance(timeout, bool)
            or not 0 < timeout <= MAX_TIMEOUT):
        return jsonify(
            {"error": "timeout 必须是 0~%d 秒之间的数字" % MAX_TIMEOUT}), 400
    error = _validate_outputs(data.get("outputs"))
    if error is not None:
        return jsonify({"error": error}), 400
    with _state_lock:
        if _slot_busy():
            return jsonify({"error": "已有运行未结束，请先终止"}), 409
        run = Run(code, steps=data["steps"], timeout=float(timeout),
                  outputs=data.get("outputs") or [])
        _current_run = run
    # 子进程与脚本线程已启动，立即返回运行标识
    return jsonify({"run_id": run.id})


@app.get("/events")
def events():
    """长轮询：返回 seq > after 的新事件；run_id 不匹配即返回 409。"""
    run_id = request.args.get("run_id", "")
    try:
        after = int(request.args.get("after", "0"))
    except ValueError:
        after = 0
    run = _get_run(run_id)
    if run is None:
        return jsonify({"error": "运行标识已失效", "status": "stale"}), 409
    deadline = time.monotonic() + LONG_POLL_SECONDS
    with run.cond:
        while (run.seq <= after and run.state == STATE_RUNNING
               and time.monotonic() < deadline):
            run.cond.wait(timeout=0.5)
        fresh = [e for e in run.events if e["seq"] > after]
        return jsonify({
            "run_id": run.id,
            "status": run.state,
            "exit_code": run.exit_code,
            "stdin_closed": run.stdin_closed,
            "busy": run.slot_busy(),
            "auto": _auto_info(run),
            "events": fresh,
        })


@app.post("/input")
def send_input():
    data = request.get_json(force=True, silent=True) or {}
    run = _get_run(data.get("run_id", ""))
    if run is None:
        return jsonify({"error": "运行标识已失效"}), 409
    if run.auto is not None and run.auto.state == "running":
        return jsonify({"error": "自动对话进行中，手动输入被禁用"}), 409
    # 长行可能在此阻塞等待子进程读取，但不影响输出排空与 /events
    error = run.write_line(data.get("line", ""))
    if error is not None:
        return jsonify({"error": error}), 409
    return jsonify({"ok": True})


@app.post("/stdin/close")
def close_stdin():
    data = request.get_json(force=True, silent=True) or {}
    run = _get_run(data.get("run_id", ""))
    if run is None:
        return jsonify({"error": "运行标识已失效"}), 409
    if run.auto is not None and run.auto.state == "running":
        return jsonify({"error": "自动对话进行中，手动关闭标准输入被禁用"}), 409
    run.close_stdin()
    return jsonify({"ok": True})


@app.post("/terminate")
def terminate():
    data = request.get_json(force=True, silent=True) or {}
    run = _get_run(data.get("run_id", ""))
    if run is None:
        return jsonify({"error": "运行标识已失效"}), 409
    if run.auto is not None:
        run.auto.cancel()     # 取消脚本：停止后续动作，结果记为「已取消」
    try:
        run.proc.kill()   # SIGKILL：立即终止，不等程序自行退出
    except OSError:
        pass
    return jsonify({"ok": True})


@app.get("/status")
def status():
    with _state_lock:
        run = _current_run
    if run is None:
        return jsonify({"state": "idle"})
    data = {"state": run.state, "run_id": run.id,
            "exit_code": run.exit_code, "busy": run.slot_busy()}
    auto = _auto_info(run)
    if auto is not None:
        data["auto"] = auto
    return jsonify(data)


_FILE_STATUS_TEXT = {
    "missing": "缺失",
    "not_regular": "非普通文件",
    "too_large": "超过 1 MiB",
    "unreadable": "读取失败",
}


def _files_not_ready(run):
    """输出文件是否还不可取：运行未结束，或自动脚本尚未公布结果。"""
    with run.cond:
        return run.slot_busy() or not run.files_ready


@app.get("/files")
def list_files():
    """输出文件清单：按声明顺序列出每项的名称、状态与大小。"""
    run = _get_run(request.args.get("run_id", ""))
    if run is None:
        return jsonify({"error": "运行标识已失效"}), 409
    if _files_not_ready(run):
        return jsonify({"error": "运行未结束，输出文件尚未收集"}), 409
    with run.cond:
        files = [{"name": e["name"], "status": e["status"], "size": e["size"]}
                 for e in run.files]
    return jsonify({"run_id": run.id, "files": files})


@app.get("/files/<name>")
def download_file(name):
    """下载某个已声明且可下载的输出文件（原始字节，不在页面预览）。"""
    run = _get_run(request.args.get("run_id", ""))
    if run is None:
        return jsonify({"error": "运行标识已失效"}), 409
    if _files_not_ready(run):
        return jsonify({"error": "运行未结束，输出文件尚未收集"}), 409
    with run.cond:
        entry = next((e for e in run.files if e["name"] == name), None)
    if entry is None:
        return jsonify({"error": "本次运行未声明该输出文件"}), 404
    if entry["status"] != "ok":
        return jsonify({"error": "该文件不可下载（%s）"
                        % _FILE_STATUS_TEXT.get(entry["status"],
                                                entry["status"]),
                        "status": entry["status"]}), 404
    return send_file(io.BytesIO(entry["data"]),
                     mimetype="application/octet-stream",
                     as_attachment=True, download_name=name)


if __name__ == "__main__":
    # threaded=True：等待输入或一路无输出时，其余请求仍可并发处理
    app.run(host="0.0.0.0", port=8000, threaded=True)
