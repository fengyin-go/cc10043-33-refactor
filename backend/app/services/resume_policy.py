"""续采策略领域服务：定义中断恢复后的续采起点策略和失败降级行为。

数据链路位置：恢复策略层，为中断的采集任务提供续采规则，决定恢复起点和失败处理方式。
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class ResumePolicyService(BaseModuleService):
    MODULE = "resume_policy"
    ENTITY = "续采规则"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["规则名称", "续采起点类型", "续采步长", "失败上限", "降级行为"]
    REQUIRED_FIELDS = ["规则名称", "续采起点类型", "续采步长"]
    STATUS_ORDER = ["草稿", "已启用", "已停用"]
    ACTION_RULES = {"启用规则": "已启用", "停用规则": "已停用"}
    NEGATIVE_ACTIONS = ["停用规则"]

    # ---- 业务扩展：续采策略与降级行为 ----

    def compute_resume_point(self, entry_id: int, last_offset: int) -> dict[str, Any]:
        """根据续采策略计算恢复起点。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None or entry.get("status") != "已启用":
            return {"ok": False, "message": "续采规则不可用"}
        start_type = entry.get("起始点类型", entry.get("续采起点类型", "断点"))
        try:
            step = int(str(entry.get("续采步长", "0")))
        except ValueError:
            step = 0
        if start_type == "断点":
            resume_from = last_offset + 1
        elif start_type == "最早":
            resume_from = 0
        elif start_type == "偏移":
            resume_from = last_offset - step if last_offset - step >= 0 else 0
        else:
            resume_from = last_offset
        return {"ok": True, "resume_from": resume_from, "start_type": start_type, "step": step}

    def handle_failure_limit(self, entry_id: int, failure_count: int) -> dict[str, Any]:
        """检查失败次数是否达到上限，触发降级行为。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None or entry.get("status") != "已启用":
            return {"threshold_reached": False, "action": None}
        try:
            limit = int(str(entry.get("失败上限", "10")))
        except ValueError:
            limit = 10
        if failure_count >= limit:
            degrade = entry.get("降级行为", "暂停")
            return {"threshold_reached": True, "action": degrade, "failures": failure_count, "limit": limit}
        return {"threshold_reached": False, "action": None, "failures": failure_count, "limit": limit}

    def validate_checkpoint_compat(
        self, entry_id: int, checkpoints: list[dict[str, Any]] | None = None
    ) -> dict[str, Any]:
        """校验续采规则与检查点的兼容性。

        检查点的取数与类型匹配规则统一由记录检查点服务承担：单项调用不传
        checkpoints 时由共享读取接口读取最新数据，联合核对时复用同一份快照。
        返回字段、匹配条件与错误反馈保持不变。
        """
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return {"compatible": False, "message": "规则不存在"}
        start_type = entry.get("续采起点类型", "")
        from app.services import service_for
        checkpoint_service = service_for("record_checkpoint")
        matching = checkpoint_service.match_by_point_type(start_type, checkpoints)
        return {"compatible": len(matching) > 0, "start_type": start_type,
                "matching_checkpoints": len(matching)}

    def link_to_isolation(self, entry_id: int, failed_msgs: int) -> str | None:
        """续采失败时的消息隔离路由。"""
        entry = store.find(self.MODULE, entry_id)
        if entry is None:
            return None
        now = datetime.now(timezone.utc).isoformat()
        for _ in range(min(failed_msgs, 3)):
            isolation_rows = store.rows("failure_isolation")
            fid = max((int(r.get("id", 0)) for r in isolation_rows), default=0) + 1
            isolation_rows.append({
                "id": fid, "失败消息ID": f"resume-err-{fid}",
                "失败原因": f"续采失败: {entry.get('规则名称', '')}",
                "原始Topic": entry.get("续采起点类型", ""),
                "隔离时间": now, "重试次数": "0",
                "status": "待处理", "pending": True, "abnormal": False,
            })
        return now