"""JSON 文件原子存储。

所有状态放在一份 JSON 中，每次操作结束统一落盘：
先写临时文件再 ``os.replace`` 并 fsync，因此进程崩溃或重启后
状态要么是操作前、要么是操作后，不会出现写入一半的归集结果。
"""

from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path

STATE_VERSION = 1


class Store:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        if self.path.exists():
            self.state = json.loads(self.path.read_text(encoding="utf-8"))
            if self.state.get("state_version") != STATE_VERSION:
                raise ValueError(
                    f"状态文件版本不兼容：期望 {STATE_VERSION}，实际 {self.state.get('state_version')}"
                )
        else:
            self.state = {
                "state_version": STATE_VERSION,
                "seq": 0,
                # key: source\x00source_seq\x00stat_date（空字符不会出现在正常来源文本中）
                "items": {},
                "conflicts": [],
                # stat_date -> 按版本号排列的已发布快报（均不可变）
                "reports": {},
            }

    @property
    def seq(self) -> int:
        return self.state["seq"]

    def next_seq(self) -> int:
        self.state["seq"] += 1
        return self.state["seq"]

    @staticmethod
    def item_key(source: str, source_seq: str, stat_date: str) -> str:
        return chr(0).join((source, source_seq, stat_date))

    def get_item(self, key: str) -> dict | None:
        return self.state["items"].get(key)

    def put_item(self, key: str, record: dict) -> None:
        self.state["items"][key] = record

    def list_items(self) -> list[dict]:
        return list(self.state["items"].values())

    def add_conflict(self, record: dict) -> None:
        self.state["conflicts"].append(record)

    def find_conflict(self, conflict_id: str) -> dict | None:
        for record in self.state["conflicts"]:
            if record["conflict_id"] == conflict_id:
                return record
        return None

    def list_reports(self, stat_date: str) -> list[dict]:
        return self.state["reports"].get(stat_date, [])

    def add_report(self, stat_date: str, report: dict) -> None:
        self.state["reports"].setdefault(stat_date, []).append(report)

    def commit(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp_name = tempfile.mkstemp(
            prefix=self.path.name + ".", suffix=".tmp", dir=str(self.path.parent)
        )
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(self.state, handle, ensure_ascii=False, sort_keys=True, indent=2)
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp_name, self.path)
            dir_fd = os.open(str(self.path.parent), os.O_RDONLY)
            try:
                os.fsync(dir_fd)
            finally:
                os.close(dir_fd)
        except BaseException:
            try:
                os.unlink(tmp_name)
            except FileNotFoundError:
                pass
            raise
