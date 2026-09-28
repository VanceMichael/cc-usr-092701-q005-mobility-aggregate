"""仅追加事件日志（审计真相源）。

存储布局（目录 ``store_dir``）：

* ``00000001.jsonl``、``00000002.jsonl`` …… 每个文件是一个**事务块**，
  对应一次单条上传或一次批量导入；
* 块文件先写 ``*.tmp`` 再 :func:`os.replace` 原子落盘，因此批量导入中途
  失败、进程被杀都只会留下被忽略的临时文件，不会产生半块；
* 块内结构：一行块头、若干事件、一行块尾；事件通过 ``prev`` 哈希串链，
  块尾给出块末链头，下一块的块头必须引用它。

重放时逐块校验链头、事件序号与块尾，任何篡改都会抛 :class:`CorruptLogError`。
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Iterator

from .encoding import canonical, digest

BLOCK_SUFFIX = ".jsonl"
_BLOCK_NAME = re.compile(r"^(\d+)\.jsonl$")


class CorruptLogError(RuntimeError):
    """事件日志被截断或篡改。"""


@dataclass(frozen=True)
class Event:
    seq: int
    type: str
    payload: dict

    def envelope(self, prev: str) -> dict:
        return {"seq": self.seq, "type": self.type, "payload": self.payload, "prev": prev}


class EventStore:
    def __init__(self, store_dir: str | os.PathLike):
        self.dir = os.fspath(store_dir)
        os.makedirs(self.dir, exist_ok=True)
        self._blocks = self._scan()
        self._head: str = "GENESIS"
        self._seq = 0
        for _ in self.iter_events():  # 完整重放一遍以校验并推进链头
            pass

    # ---- 公共接口 --------------------------------------------------------

    def head(self) -> str:
        """当前日志链头哈希（整个日志的指纹基线）。"""
        return self._head

    def seq(self) -> int:
        return self._seq

    def iter_events(self) -> Iterator[Event]:
        """按序号重放全部业务事件（块头/块尾已校验并剔除）。"""
        head = "GENESIS"
        seq = 0
        for block_no in self._blocks:
            path = self._block_path(block_no)
            lines = self._read_lines(path)
            header, footer, event_lines = self._split_frame(path, lines, block_no)
            if header["prev"] != head:
                raise CorruptLogError(f"{path}：块头链头不匹配")
            for line in event_lines:
                seq += 1
                env = self._loads(line)
                expect = digest({k: env[k] for k in ("seq", "type", "payload", "prev")})
                if env.get("hash") != expect:
                    raise CorruptLogError(f"{path}：事件 {seq} 哈希不匹配")
                if env["prev"] != head:
                    raise CorruptLogError(f"{path}：事件 {seq} 断链")
                if env["seq"] != seq:
                    raise CorruptLogError(f"{path}：事件序号应为 {seq}")
                head = env["hash"]
                yield Event(seq=env["seq"], type=env["type"], payload=env["payload"])
            if footer["head"] != head:
                raise CorruptLogError(f"{path}：块尾链头不匹配")
            if footer["count"] != len(event_lines):
                raise CorruptLogError(f"{path}：事件计数不匹配")
        self._head = head
        self._seq = seq

    def append_block(self, events: list[tuple[str, dict]]) -> list[Event]:
        """原子追加一个事务块（空块拒绝）。返回已落盘事件。"""
        if not events:
            raise ValueError("不允许追加空事务块")
        block_no = (self._blocks[-1] + 1) if self._blocks else 1
        assigned: list[Event] = []
        lines: list[str] = []
        head = self._head
        lines.append(canonical({"block": block_no, "prev": head}))
        for event_type, payload in events:
            self._seq += 1
            event = Event(seq=self._seq, type=event_type, payload=payload)
            env = event.envelope(head)
            env["hash"] = digest(env)
            head = env["hash"]
            lines.append(canonical(env))
            assigned.append(event)
        lines.append(canonical({"block_end": block_no, "head": head, "count": len(events)}))

        tmp = os.path.join(self.dir, f".{block_no:08d}.tmp")
        with open(tmp, "w", encoding="utf-8", newline="\n") as f:
            f.write("\n".join(lines) + "\n")
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self._block_path(block_no))
        self._fsync_dir()
        self._blocks.append(block_no)
        self._head = head
        return assigned

    # ---- 内部 ------------------------------------------------------------

    @staticmethod
    def _loads(line: str) -> dict:
        import json

        try:
            value = json.loads(line)
        except ValueError as exc:
            raise CorruptLogError("日志行不是合法 JSON") from exc
        if not isinstance(value, dict):
            raise CorruptLogError("日志行结构非法")
        return value

    def _read_lines(self, path: str) -> list[str]:
        with open(path, encoding="utf-8") as f:
            content = f.read()
        lines = content.split("\n")
        if lines and lines[-1] == "":
            lines.pop()
        if len(lines) < 3:
            raise CorruptLogError(f"{path}：块帧数不足")
        return lines

    def _split_frame(self, path: str, lines: list[str], block_no: int):
        header = self._loads(lines[0])
        footer = self._loads(lines[-1])
        if header.get("block") != block_no or "prev" not in header:
            raise CorruptLogError(f"{path}：块头非法")
        if footer.get("block_end") != block_no or "head" not in footer or "count" not in footer:
            raise CorruptLogError(f"{path}：块尾非法")
        return header, footer, lines[1:-1]

    def _block_path(self, block_no: int) -> str:
        return os.path.join(self.dir, f"{block_no:08d}{BLOCK_SUFFIX}")

    def _scan(self) -> list[int]:
        found: list[int] = []
        for name in os.listdir(self.dir):
            m = _BLOCK_NAME.match(name)
            if m:
                found.append(int(m.group(1)))
        found.sort()
        if found and found[0] != 1:
            raise CorruptLogError("日志块必须从 00000001 开始")
        if found != list(range(1, len(found) + 1)):
            raise CorruptLogError("日志块编号不连续")
        return found

    def _fsync_dir(self) -> None:
        try:
            fd = os.open(self.dir, os.O_RDONLY)
        except OSError:  # pragma: no cover - 某些平台不支持目录 fsync
            return
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
