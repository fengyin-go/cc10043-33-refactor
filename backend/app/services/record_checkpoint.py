"""记录检查点领域服务：管理接入任务的断点位置，支持恢复和回溯，提供检查点的确认和过期管理。

数据链路位置：断点管理层，为接入任务提供可靠的断点记录和恢复依据，是续采策略的数据基础。
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class RecordCheckpointService(BaseModuleService):
    MODULE = "record_checkpoint"
    ENTITY = "检查点"
    OPERATION_TERM = "接入"
    LIST_FIELDS = ["关联接入任务", "检查点类型", "检查点值", "创建时间", "确认状态"]
    REQUIRED_FIELDS = ["关联接入任务", "检查点类型", "检查点值"]
    STATUS_ORDER = ["待确认", "已确认", "已过期"]
    ACTION_RULES = {"确认检查点": "已确认", "废弃检查点": "已过期"}
    NEGATIVE_ACTIONS = []

    # ---- 共享读取接口：检查点取数边界与匹配规则的唯一责任点 ----
    #
    # 续采兼容检查（resume_policy）与偏移对应检查（offset_manager）都依赖检查点数据，
    # 但旧检查点记录可能只有文本、任务名也可能把消费组和 Topic 拼在一起，读取哪一版、
    # 按什么规则匹配都收敛到本模块（检查点归属服务），调用方不再各自直连数据仓库。

    def snapshot_checkpoints(self) -> list[dict[str, Any]]:
        """读取一份检查点快照，作为各项检查的同一份取数依据。

        每次调用都从数据仓库取最新数据并返回行副本，副本上的改动不会回写仓库；
        联合核对先取一次快照再复用，两个结论即可由同一次取数复跑。
        单项调用不传快照时仍走这里，因此旧客户端单查一侧永远读到最新数据。
        """
        return [dict(row) for row in store.rows(self.MODULE)]

    def match_by_point_type(
        self, start_type: str, checkpoints: list[dict[str, Any]] | None = None
    ) -> list[dict[str, Any]]:
        """按检查点类型匹配：续采起点类型是检查点类型文本的子串即命中。

        兼容旧检查点只有文本的情况（子串包含即算匹配，不做枚举化）。
        """
        rows = self.snapshot_checkpoints() if checkpoints is None else checkpoints
        return [c for c in rows if start_type in str(c.get("检查点类型", ""))]

    def match_by_group_topic(
        self, group: str, topic: str, checkpoints: list[dict[str, Any]] | None = None
    ) -> list[dict[str, Any]]:
        """按消费组与 Topic 匹配：两者都作为子串出现在关联接入任务名中即命中。

        兼容任务名拼接了消费组和主题的旧数据，保持原本的子串包含规则。
        """
        rows = self.snapshot_checkpoints() if checkpoints is None else checkpoints
        return [
            cp for cp in rows
            if group in cp.get("关联接入任务", "") and topic in cp.get("关联接入任务", "")
        ]

    def joint_checkpoint_cross_check(
        self,
        offset_entry_id: int,
        policy_entry_id: int,
        checkpoints: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """联合核对：偏移对应检查与续采兼容检查从同一份检查点取数计算。

        一次取数（checkpoints 缺省时现场读取最新快照）同时喂给两项检查，
        返回中带回该快照，值班员可用它复跑整轮核对，解释两个数字为何不同。
        单项对外接口的行为不变，旧客户端单查一侧仍读到最新数据。
        """
        from app.services import service_for

        snapshot = self.snapshot_checkpoints() if checkpoints is None else checkpoints
        offset_service = service_for("offset_manager")
        resume_service = service_for("resume_policy")
        offset_result = offset_service.validate_checkpoint(
            offset_entry_id, checkpoints=snapshot)
        compat_result = resume_service.validate_checkpoint_compat(
            policy_entry_id, checkpoints=snapshot)
        return {
            "ok": bool(offset_result.get("ok")) and compat_result.get("compatible", False),
            "offset_check": offset_result,
            "compat_check": compat_result,
            "offset_matched_ids": [
                int(cp.get("id", 0))
                for cp in self.match_by_group_topic(
                    str(offset_result.get("group", "")),
                    str(offset_result.get("topic", "")),
                    checkpoints=snapshot,
                )
            ] if offset_result.get("ok") else [],
            "compat_matched_ids": [
                int(cp.get("id", 0))
                for cp in self.match_by_point_type(
                    str(compat_result.get("start_type", "")),
                    checkpoints=snapshot,
                )
            ] if "message" not in compat_result else [],
            "checkpoint_snapshot": snapshot,
        }

    # ---- 业务扩展：断点记录与恢复 ----

    def create_checkpoint(self, task_name: str, point_type: str, point_value: str) -> dict[str, Any]:
        """创建新的检查点记录。"""
        rows = store.rows(self.MODULE)
        cp_id = max((int(r.get("id", 0)) for r in rows), default=0) + 1
        now = datetime.now(timezone.utc).isoformat()
        checkpoint = {
            "id": cp_id, "关联接入任务": task_name, "检查点类型": point_type,
            "检查点值": point_value, "创建时间": now, "确认状态": "待确认",
            "status": "待确认", "pending": True, "abnormal": False,
        }
        rows.append(checkpoint)
        return {"ok": True, "message": "检查点已创建", "checkpoint": checkpoint}

    def get_latest_checkpoint(self, task_name: str) -> dict[str, Any] | None:
        """获取指定接入任务的最新已确认检查点。"""
        rows = store.rows(self.MODULE)
        matched = [r for r in rows if task_name in str(r.get("关联接入任务", ""))
                   and r.get("status") == "已确认"]
        if not matched:
            return None
        return max(matched, key=lambda r: str(r.get("创建时间", "")))

    def validate_checkpoint_chain(self, entry_id: int) -> dict[str, Any]:
        """校验检查点链的完整性：是否存在断链或顺序异常。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"ok": False, "message": "检查点不存在"}
        task = entry.get("关联接入任务", "")
        rows = store.rows(self.MODULE)
        chain = sorted(
            [r for r in rows if task in str(r.get("关联接入任务", ""))],
            key=lambda r: str(r.get("创建时间", "")))
        gaps = []
        for i in range(1, len(chain)):
            prev_val = chain[i - 1].get("检查点值", "")
            curr_val = chain[i].get("检查点值", "")
            if prev_val >= curr_val:
                gaps.append(f"检查点值非递增: {prev_val} → {curr_val}")
        return {
            "ok": len(gaps) == 0,
            "task": task,
            "chain_length": len(chain),
            "gaps": gaps,
        }

    def route_to_resume(self, entry_id: int) -> dict[str, Any] | None:
        """检查点路由到续采策略，返回匹配的续采规则。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None
        point_type = entry.get("检查点类型", "")
        policies = store.rows("resume_policy")
        for policy in policies:
            if policy.get("status") == "已启用" and point_type in policy.get("续采起点类型", ""):
                return {"policy": policy, "checkpoint": entry}
        return None

    def audit_expired_checkpoints(self) -> list[int]:
        """审计过期检查点并批量标记。"""
        rows = store.rows(self.MODULE)
        expired_ids = []
        now = datetime.now(timezone.utc)
        for row in rows:
            if row.get("status") != "待确认":
                continue
            created_str = str(row.get("创建时间", ""))
            try:
                created = datetime.fromisoformat(created_str)
                if (now - created).days > 30:
                    row["status"] = "已过期"
                    expired_ids.append(int(row.get("id", 0)))
            except ValueError:
                continue
        if expired_ids:
            audit_rows = store.rows("stream_audit")
            aid = max((int(r.get("id", 0)) for r in audit_rows), default=0) + 1
            audit_rows.append({
                "id": aid, "处理时间": now.isoformat(), "数据源": "record_checkpoint",
                "消息总数": str(len(expired_ids)), "成功数": str(len(expired_ids)),
                "失败原因分布": "", "status": "正常", "pending": True, "abnormal": False,
            })
        return expired_ids

    def commit_offset(self, entry_id: int) -> str | None:
        """将已确认检查点提交到偏移量管理。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None or entry.get("status") != "已确认":
            return None
        offset_rows = store.rows("offset_manager")
        oid = max((int(r.get("id", 0)) for r in offset_rows), default=0) + 1
        now = datetime.now(timezone.utc).isoformat()
        offset_rows.append({
            "id": oid, "消费组": entry.get("关联接入任务", ""),
            "Topic/Queue": entry.get("关联接入任务", ""),
            "分区/分片": "0", "当前Offset": entry.get("检查点值", "0"),
            "消费延迟": "0", "status": "正常", "pending": True, "abnormal": False,
        })
        return now