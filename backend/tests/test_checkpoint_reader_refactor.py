"""检查点取数边界重构的联合核对测试。

覆盖需求：
1. 单项调用（偏移对应检查、续采兼容检查）保留既有匹配条件、返回字段和错误反馈；
2. 旧检查点只有文本类型、任务名拼接消费组和主题时，原有匹配规则与空结果保留；
3. 联合核对只取一次检查点，偏移量模块与续采策略从同一份快照计算两个结论；
4. 值班员在联合核对期间确认一条迟到检查点后：
   - 旧客户端单查一侧仍读到最新数据；
   - 用联合核对返回的快照可复跑同一轮，两项结论不变。
"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.store import store  # noqa: E402
from app.services import service_for  # noqa: E402

CHECKPOINT = "record_checkpoint"
OFFSET = "offset_manager"
POLICY = "resume_policy"


def _next_id(module: str) -> int:
    return max((int(r.get("id", 0)) for r in store.rows(module)), default=0) + 1


def add_checkpoint(task: str, point_type: str, value: str, status: str = "已确认") -> int:
    rows = store.rows(CHECKPOINT)
    cp_id = _next_id(CHECKPOINT)
    rows.append({
        "id": cp_id, "关联接入任务": task, "检查点类型": point_type,
        "检查点值": value, "创建时间": f"2026-11-23T10:{cp_id:02d}:00+08:00",
        "确认状态": status, "status": status, "pending": status != "已过期",
        "abnormal": False,
    })
    return cp_id


def add_offset(group: str, topic: str, offset: str) -> int:
    rows = store.rows(OFFSET)
    oid = _next_id(OFFSET)
    rows.append({
        "id": oid, "消费组": group, "Topic/Queue": topic, "分区/分片": "0",
        "当前Offset": offset, "消费延迟": "0", "status": "正常",
        "pending": True, "abnormal": False,
    })
    return oid


def add_policy(rule_name: str, start_type: str) -> int:
    rows = store.rows(POLICY)
    pid = _next_id(POLICY)
    rows.append({
        "id": pid, "规则名称": rule_name, "续采起点类型": start_type,
        "续采步长": "10", "失败上限": "5", "降级行为": "暂停",
        "status": "已启用", "pending": True, "abnormal": False,
    })
    return pid


class CheckpointReaderRefactorTest(unittest.TestCase):
    def setUp(self) -> None:
        # 每个用例回到仅含种子数据的干净仓库
        store._tables = {
            name: [dict(row) for row in rows]
            for name, rows in store._tables.items()
        }
        store._tables[CHECKPOINT] = [dict(r) for r in store._tables[CHECKPOINT]
                                     if int(r.get("id", 0)) <= 3]
        store._tables[OFFSET] = [dict(r) for r in store._tables[OFFSET]
                                 if int(r.get("id", 0)) <= 3]
        store._tables[POLICY] = [dict(r) for r in store._tables[POLICY]
                                 if int(r.get("id", 0)) <= 3]
        self.cp_service = service_for(CHECKPOINT)
        self.offset_service = service_for(OFFSET)
        self.policy_service = service_for(POLICY)

    # ---- 单项调用：既有行为保留 ----

    def test_single_calls_keep_empty_results_and_fields(self) -> None:
        offset_id = add_offset("组-不存在", "题-不存在", "12")
        policy_id = add_policy("规则-空", "类型-不存在")

        offset_result = self.offset_service.validate_checkpoint(offset_id)
        self.assertEqual(
            set(offset_result),
            {"ok", "group", "topic", "current_offset", "matched_checkpoints"},
        )
        self.assertTrue(offset_result["ok"])
        self.assertEqual(offset_result["matched_checkpoints"], 0)
        self.assertEqual(offset_result["current_offset"], "12")

        compat_result = self.policy_service.validate_checkpoint_compat(policy_id)
        self.assertEqual(
            set(compat_result),
            {"compatible", "start_type", "matching_checkpoints"},
        )
        self.assertFalse(compat_result["compatible"])
        self.assertEqual(compat_result["matching_checkpoints"], 0)
        self.assertEqual(compat_result["start_type"], "类型-不存在")

    def test_single_calls_keep_error_feedback(self) -> None:
        missing_offset = self.offset_service.validate_checkpoint(9999)
        self.assertEqual(missing_offset,
                         {"ok": False, "message": "偏移量记录不存在"})
        missing_policy = self.policy_service.validate_checkpoint_compat(9999)
        self.assertEqual(missing_policy,
                         {"compatible": False, "message": "规则不存在"})

    def test_legacy_text_type_and_joined_task_name_rules(self) -> None:
        # 旧数据：检查点类型只是纯文本，任务名拼接了消费组和主题
        add_checkpoint("订单消费组/订单主题/分片0", "时间戳", "1700000000")
        offset_id = add_offset("订单消费组", "订单主题", "88")
        policy_id = add_policy("时间戳续采", "时间戳")

        offset_result = self.offset_service.validate_checkpoint(offset_id)
        self.assertEqual(offset_result["matched_checkpoints"], 1)

        compat_result = self.policy_service.validate_checkpoint_compat(policy_id)
        self.assertTrue(compat_result["compatible"])
        self.assertEqual(compat_result["matching_checkpoints"], 1)

    # ---- 联合核对：同一次取数 + 迟到检查点 ----

    def test_joint_cross_check_with_late_checkpoint(self) -> None:
        # 一条只符合起始类型，一条只符合组与主题
        only_type_id = add_checkpoint("其他任务/其他主题", "水位点", "100")
        only_group_id = add_checkpoint("联合组/联合主题/分区1", "偏移位点", "200")
        offset_id = add_offset("联合组", "联合主题", "200")
        policy_id = add_policy("水位点规则", "水位点")

        # 值班员做联合核对：一次取数，两项结论来自同一份快照。
        # 两个数字都为 1，但命中的是不同的检查点行——这正是要向值班员解释的差异。
        round_one = self.cp_service.joint_checkpoint_cross_check(offset_id, policy_id)
        self.assertEqual(round_one["offset_check"]["matched_checkpoints"], 1)
        self.assertEqual(round_one["compat_check"]["matching_checkpoints"], 1)
        self.assertTrue(round_one["ok"])
        self.assertEqual(round_one["offset_matched_ids"], [only_group_id])
        self.assertEqual(round_one["compat_matched_ids"], [only_type_id])
        self.assertNotEqual(round_one["offset_matched_ids"],
                            round_one["compat_matched_ids"])
        snapshot_ids = {int(cp["id"]) for cp in round_one["checkpoint_snapshot"]}
        self.assertEqual(snapshot_ids, {1, 2, 3, only_type_id, only_group_id})

        # 读完第一侧后，值班员确认了一条迟到的检查点（同时符合类型与组/主题）
        late_id = add_checkpoint("联合组/联合主题/分区2", "水位点", "300", "已确认")

        # 旧客户端只查一侧：仍可读到最新数据，迟到行被两侧各自的最新读取命中
        fresh_offset = self.offset_service.validate_checkpoint(offset_id)
        fresh_compat = self.policy_service.validate_checkpoint_compat(policy_id)
        self.assertEqual(fresh_offset["matched_checkpoints"], 2)
        self.assertEqual(fresh_compat["matching_checkpoints"], 2)

        # 这一轮联合核对的两个结论必须可以从同一次取数复跑：快照里没有迟到行
        rerun = self.cp_service.joint_checkpoint_cross_check(
            offset_id, policy_id, checkpoints=round_one["checkpoint_snapshot"])
        self.assertEqual(rerun["offset_check"]["matched_checkpoints"],
                         round_one["offset_check"]["matched_checkpoints"])
        self.assertEqual(rerun["compat_check"]["matching_checkpoints"],
                         round_one["compat_check"]["matching_checkpoints"])
        self.assertEqual(rerun["offset_matched_ids"], round_one["offset_matched_ids"])
        self.assertEqual(rerun["compat_matched_ids"], round_one["compat_matched_ids"])
        snapshot_rerun_ids = {int(cp["id"]) for cp in rerun["checkpoint_snapshot"]}
        self.assertNotIn(late_id, snapshot_rerun_ids)

        # 不带快照重新联合核对时，两个结论同时反映迟到行且基于同一份新取数
        round_two = self.cp_service.joint_checkpoint_cross_check(offset_id, policy_id)
        self.assertEqual(round_two["offset_check"]["matched_checkpoints"], 2)
        self.assertEqual(round_two["compat_check"]["matching_checkpoints"], 2)
        self.assertTrue(round_two["ok"])
        self.assertIn(late_id, {int(cp["id"]) for cp in round_two["checkpoint_snapshot"]})

    def test_snapshot_is_isolated_from_live_store(self) -> None:
        snapshot = self.cp_service.snapshot_checkpoints()
        original_len = len(store.rows(CHECKPOINT))
        add_checkpoint("快照隔离任务", "快照隔离类型", "1")
        # 快照不随后续写入变化，且修改快照不回写仓库
        self.assertEqual(len(snapshot), original_len)
        snapshot.append({"id": 9999, "关联接入任务": "伪造"})
        self.assertNotIn(9999, [int(r.get("id", 0)) for r in store.rows(CHECKPOINT)])

    def test_joint_cross_check_propagates_missing_entry_feedback(self) -> None:
        result = self.cp_service.joint_checkpoint_cross_check(9999, 9999)
        self.assertFalse(result["ok"])
        self.assertEqual(result["offset_check"],
                         {"ok": False, "message": "偏移量记录不存在"})
        self.assertEqual(result["compat_check"],
                         {"compatible": False, "message": "规则不存在"})
        self.assertEqual(result["offset_matched_ids"], [])
        self.assertEqual(result["compat_matched_ids"], [])


if __name__ == "__main__":
    unittest.main(verbosity=2)
