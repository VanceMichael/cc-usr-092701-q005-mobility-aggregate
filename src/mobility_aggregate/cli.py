"""命令行入口：导入、复核、发布、更正、查询、下钻、审计重放。

示例：
    python -m src.mobility_aggregate.cli --state data/state.json import batch.json --at 2026-09-27T08:00
    python -m src.mobility_aggregate.cli --state data/state.json publish 2026-09-26 --issuer zhang
    python -m src.mobility_aggregate.cli --state data/state.json report 2026-09-26 --drilldown
    python -m src.mobility_aggregate.cli replay fixtures/replay_bundle.json
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

from .replay import load_bundle, replay_bundle
from .service import AggregationError, AggregationService
from .store import Store


def _print(value: object) -> None:
    print(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="mobility-aggregate", description="跨区域客流统计归集服务")
    parser.add_argument("--state", default="data/state.json", help="归集状态文件路径")
    parser.add_argument("--issuers", default="", help="授权更正人员，逗号分隔")
    parser.add_argument("--cutoff", default=None, help="每日封账时刻，如 09:00（统计日次日）")
    sub = parser.add_subparsers(dest="command", required=True)

    p_import = sub.add_parser("import", help="整批导入分项数据")
    p_import.add_argument("file", help="JSON 文件：{\"batch_id\": ..., \"items\": [...]} 或分项数组")
    p_import.add_argument("--at", required=True, help="接收时刻 ISO 时间")
    p_import.add_argument("--batch-id", default=None)

    p_conflicts = sub.add_parser("conflicts", help="查看复核单")
    p_conflicts.add_argument("--status", default=None, choices=("pending", "resolved"))

    p_resolve = sub.add_parser("resolve", help="复核冲突单")
    p_resolve.add_argument("conflict_id")
    p_resolve.add_argument("--action", required=True, choices=("accept", "reject"))
    p_resolve.add_argument("--issuer", required=True)
    p_resolve.add_argument("--at", required=True)
    p_resolve.add_argument("--note", default="")

    p_pub = sub.add_parser("publish", help="发布（或更正）锁定日报")
    p_pub.add_argument("stat_date")
    p_pub.add_argument("--issuer", required=True)
    p_pub.add_argument("--at", required=True)
    p_pub.add_argument("--reason", default=None, help="更正版本必填的更正原因")

    p_report = sub.add_parser("report", help="查看日报")
    p_report.add_argument("stat_date")
    p_report.add_argument("--version", default="latest")
    p_report.add_argument("--drilldown", action="store_true", help="下钻到运输方式/口径/分项/修订")

    sub.add_parser("versions", help="查看日报版本链").add_argument("stat_date")
    sub.add_parser("events", help="查看审计事件流")

    p_replay = sub.add_parser("replay", help="用固定输入 bundle 重现计算")
    p_replay.add_argument("file")
    p_replay.add_argument("--state", dest="replay_state", default=None,
                          help="可选：把重放状态落盘到该路径以便检查")
    return parser


def _service(args: argparse.Namespace) -> AggregationService:
    store = Store(args.state)
    issuers = {x.strip() for x in args.issuers.split(",") if x.strip()}

    def policy(stat_date: str, received_at: datetime) -> bool:
        if not args.cutoff:
            return False
        from datetime import date, timedelta
        hour, minute = (int(x) for x in args.cutoff.split(":", 1))
        d = date.fromisoformat(stat_date)
        deadline = datetime.combine(
            d + timedelta(days=1), datetime.min.time().replace(hour=hour, minute=minute)
        )
        return received_at > deadline

    return AggregationService(store, authorized_issuers=issuers, closing_policy=policy)


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "replay":
            result = replay_bundle(load_bundle(args.file), store_path=args.replay_state)
            _print(result)
            return 0

        service = _service(args)
        if args.command == "import":
            payload = json.loads(Path(args.file).read_text(encoding="utf-8"))
            if isinstance(payload, dict) and "items" in payload:
                items = payload["items"]
                args.batch_id = args.batch_id or payload.get("batch_id")
            else:
                items = payload
            result = service.ingest_batch(
                items, received_at=datetime.fromisoformat(args.at), batch_id=args.batch_id
            )
            _print(result.to_dict())
        elif args.command == "conflicts":
            _print(service.list_conflicts(args.status))
        elif args.command == "resolve":
            _print(service.resolve_conflict(
                args.conflict_id, action=args.action, issuer=args.issuer,
                decided_at=datetime.fromisoformat(args.at), note=args.note,
            ))
        elif args.command == "publish":
            report = service.publish_daily(
                args.stat_date, issuer=args.issuer,
                published_at=datetime.fromisoformat(args.at),
                correction_reason=args.reason,
            )
            _print({"report_id": report["report_id"], "version": report["version"],
                    "kind": report["kind"], "total_passengers": report["total_passengers"],
                    "comparison": report["comparison"], "manifest": report["manifest"]})
        elif args.command == "report":
            version = args.version if args.version == "latest" else int(args.version)
            report = service.get_report(args.stat_date, version)
            _print(service.drilldown(report) if args.drilldown else report)
        elif args.command == "versions":
            _print(service.list_report_versions(args.stat_date))
        elif args.command == "events":
            _print(service.list_events())
        return 0
    except (AggregationError, ValueError, FileNotFoundError) as exc:
        print(f"错误：{exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
