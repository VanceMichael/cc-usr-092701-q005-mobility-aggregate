"""离线归集服务命令行。

通用参数 ``--store`` 指定事件日志目录（默认 ``./data/store``），
``--publishers/--reviewers`` 仅在目录首次初始化时生效，之后以目录内
``authz.json`` 为准（授权名单本身也是固定输入的一部分）。

示例：

    python3 -m src.mobility_aggregate.cli --store data/store import fixtures/batch_2026-09-25.json
    python3 -m src.mobility_aggregate.cli --store data/store close 2026-09-25
    python3 -m src.mobility_aggregate.cli --store data/store publish 2026-09-25 --by 统计处
    python3 -m src.mobility_aggregate.cli --store data/store query 2026-09-25
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from decimal import Decimal

from .encoding import canonical
from .service import AggregationService, verify_audit_bundle
from .store import CorruptLogError

AUTHZ_FILE = "authz.json"
DEFAULT_STORE = os.path.join("data", "store")


class _DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return format(o, "f")
        return super().default(o)


def _emit(obj) -> None:
    print(json.dumps(obj, ensure_ascii=False, indent=2, cls=_DecimalEncoder, sort_keys=False))


def _load_authz(store_dir: str, publishers: list[str] | None, reviewers: list[str] | None) -> dict:
    os.makedirs(store_dir, exist_ok=True)
    path = os.path.join(store_dir, AUTHZ_FILE)
    if os.path.exists(path):
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    authz = {
        "publishers": publishers or ["统计处"],
        "reviewers": reviewers or ["统计处"],
    }
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        f.write(canonical(authz))
    return authz


def _service(args) -> AggregationService:
    authz = _load_authz(args.store, args.publishers, args.reviewers)
    return AggregationService(args.store, publishers=authz["publishers"], reviewers=authz["reviewers"])


def _load_items(path: str) -> list[dict]:
    with open(path, encoding="utf-8") as f:
        value = json.load(f)
    if isinstance(value, dict) and isinstance(value.get("items"), list):
        return value["items"]
    if isinstance(value, list):
        return value
    raise ValueError(f"{path}：批量文件应为数组或含 items 数组的对象")


def cmd_import(args) -> None:
    items: list[dict] = []
    for path in args.files:
        items.extend(_load_items(path))
    svc = _service(args)
    _emit(svc.submit_batch(items))


def cmd_resolve(args) -> None:
    svc = _service(args)
    _emit(svc.resolve_conflict(args.source, args.seq, args.decision, args.by, args.reason or ""))


def cmd_admit(args) -> None:
    svc = _service(args)
    _emit(svc.admit_quarantine(args.source, args.seq, args.by, args.reason))


def cmd_status(args) -> None:
    svc = _service(args)
    _emit({
        "log_head": svc.store.head(),
        "event_count": svc.store.seq(),
        "closed_days": sorted(d.isoformat() for d in svc.closed_days),
        "open_conflicts": [c.to_payload() for c in svc.open_conflicts.values()],
        "quarantine": [
            {"source": e["item"].source, "seq": e["item"].seq,
             "stat_date": e["item"].stat_date.isoformat(),
             "passengers": format(e["item"].passengers, "f"),
             "reason": e["reason"], "at": e["at"]}
            for e in svc.quarantine
        ],
        "published": {d.isoformat(): [v.version_no for v in vs]
                      for d, vs in sorted(svc.reports.items())},
    })


def cmd_close(args) -> None:
    _emit(_service(args).close_day(args.date))


def cmd_publish(args) -> None:
    svc = _service(args)
    report = svc.publish(args.date, args.by)
    _emit({"published": True, "report": report.to_payload()})


def cmd_correct(args) -> None:
    svc = _service(args)
    report = svc.correct(args.date, args.by, args.reason)
    _emit({"corrected": True, "report": report.to_payload()})


def cmd_query(args) -> None:
    svc = _service(args)
    _emit(svc.drilldown(args.date, args.version))


def cmd_versions(args) -> None:
    svc = _service(args)
    _emit([v.to_payload() for v in svc.list_versions(args.date)])


def cmd_trace(args) -> None:
    _emit(_service(args).revision_trace(args.source, args.seq))


def cmd_verify(args) -> None:
    _emit(_service(args).reverify())


def cmd_bundle(args) -> None:
    svc = _service(args)
    _emit(svc.export_audit_bundle(args.out_dir, args.date))


def cmd_verify_bundle(args) -> None:
    result = verify_audit_bundle(args.dir)
    _emit(result)
    if not result["all_ok"]:
        sys.exit(1)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="跨区域客流离线归集服务")
    parser.add_argument("--store", default=DEFAULT_STORE, help="事件日志目录")
    parser.add_argument("--publishers", nargs="*", help="初始化时设置：可发布更正版的授权人员")
    parser.add_argument("--reviewers", nargs="*", help="初始化时设置：可处理复核/隔离的授权人员")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("import", help="批量导入分项数据（一个事务，失败整批回滚）")
    p.add_argument("files", nargs="+")
    p.set_defaults(func=cmd_import)

    p = sub.add_parser("resolve", help="裁决复核队列")
    p.add_argument("source")
    p.add_argument("seq")
    p.add_argument("--decision", choices=("keep_original", "use_new"), required=True)
    p.add_argument("--by", required=True)
    p.add_argument("--reason", default="")
    p.set_defaults(func=cmd_resolve)

    p = sub.add_parser("admit", help="授权接纳封账后的晚到分项为更正素材")
    p.add_argument("source")
    p.add_argument("seq")
    p.add_argument("--by", required=True)
    p.add_argument("--reason", required=True)
    p.set_defaults(func=cmd_admit)

    p = sub.add_parser("status", help="查看封账日、待复核冲突与隔离区")
    p.set_defaults(func=cmd_status)

    p = sub.add_parser("close", help="对统计日封账")
    p.add_argument("date")
    p.set_defaults(func=cmd_close)

    p = sub.add_parser("publish", help="封账后发布锁定的原版日报")
    p.add_argument("date")
    p.add_argument("--by", required=True)
    p.set_defaults(func=cmd_publish)

    p = sub.add_parser("correct", help="授权人员生成更正版本")
    p.add_argument("date")
    p.add_argument("--by", required=True)
    p.add_argument("--reason", required=True)
    p.set_defaults(func=cmd_correct)

    p = sub.add_parser("query", help="总量下钻到运输方式、分类口径与分项")
    p.add_argument("date")
    p.add_argument("--version", type=int, default=None)
    p.set_defaults(func=cmd_query)

    p = sub.add_parser("versions", help="列出某日全部日报版本")
    p.add_argument("date")
    p.set_defaults(func=cmd_versions)

    p = sub.add_parser("trace", help="查看某来源序号的修订记录")
    p.add_argument("source")
    p.add_argument("seq")
    p.set_defaults(func=cmd_trace)

    p = sub.add_parser("verify", help="重放日志并重算全部报告指纹")
    p.set_defaults(func=cmd_verify)

    p = sub.add_parser("audit-bundle", help="导出审计包（日志副本+清单）")
    p.add_argument("out_dir")
    p.add_argument("--date", default=None)
    p.set_defaults(func=cmd_bundle)

    p = sub.add_parser("verify-bundle", help="用审计包复算当时结果")
    p.add_argument("dir")
    p.set_defaults(func=cmd_verify_bundle)

    return parser


def main(argv: list[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    try:
        args.func(args)
    except CorruptLogError as exc:
        raise SystemExit(f"日志完整性校验失败：{exc}") from exc
    except (ValueError, KeyError, PermissionError, FileExistsError) as exc:
        raise SystemExit(f"操作失败：{exc}") from exc


if __name__ == "__main__":
    main()
