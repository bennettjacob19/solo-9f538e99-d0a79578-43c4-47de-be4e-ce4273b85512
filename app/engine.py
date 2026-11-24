"""清退引擎：按工单推进命名空间清理的状态机。

阶段：VALIDATING -> RETAINING -> DELETING_NAMESPACE -> DONE（异常：FAILED）
阻塞：blocked_reason 字段记录当前阻塞原因，推进时自动清除并重试。

关键不变量：
  * 先留存、验证（在原命名空间之外独立读回校验 sha256），再删除对应源 PVC。
  * 任一时刻 原卷字节 + 留存副本字节 <= 开单基线 + 最大选中卷大小。
  * 所有删除按开单时快照的 UID 寻址；同名重建的命名空间 UID 不同，不受影响。
  * 删除操作写入 ops 日志（幂等键）；已确认(confirmed)的删除绝不重发；
    结果未明(unknown)的删除先 GET 核对：目标已消失则标记确认，仍在才重发。
"""
from __future__ import annotations

import json
import os
import shutil
import threading
import uuid
from pathlib import Path

from .db import DB
from .simcluster import (ConflictError, DroppedResponseError, NotFoundError,
                         SimCluster)
from .util import now_iso, sha256_file, write_json

MAX_STEPS_PER_ADVANCE = 1000


class Engine:
    def __init__(self, data_dir: Path | str):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.sim = SimCluster(self.data_dir / "cluster")
        self.retained_root = self.data_dir / "retained"
        self.retained_root.mkdir(parents=True, exist_ok=True)
        self.db = DB(self.data_dir / "app.db")
        self._lock = threading.RLock()

    # ---------------- 开单 ----------------
    def create_ticket(self, namespace: str, retain_pvcs: list[str], note: str | None = None) -> dict:
        with self._lock:
            if not retain_pvcs:
                raise ValueError("至少选择一个需保留的 PVC")
            ns = self.sim.get_namespace_by_name(namespace)
            if not ns:
                raise NotFoundError(f"命名空间 {namespace!r} 不存在")
            pvcs = self.sim.list_pvcs(ns_uid=ns["uid"])
            by_name = {p["name"]: p for p in pvcs}
            missing = [n for n in retain_pvcs if n not in by_name]
            if missing:
                raise NotFoundError(f"命名空间 {namespace!r} 中不存在 PVC: {missing}")
            tid = uuid.uuid4().hex[:12]
            selected = [by_name[n] for n in retain_pvcs]
            baseline = sum(p["size_bytes"] for p in pvcs)
            max_sel = max(p["size_bytes"] for p in selected)
            self.db.execute(
                "INSERT INTO tickets(id,created_at,namespace_name,namespace_uid,state,"
                "blocked_reason,note,baseline_bytes,max_selected_bytes,peak_bytes) "
                "VALUES(?,?,?,?,?,?,?,?,?,?)",
                (tid, now_iso(), ns["name"], ns["uid"], "VALIDATING", None, note,
                 baseline, max_sel, baseline))
            for p in pvcs:
                self.db.execute(
                    "INSERT INTO pvcs(ticket_id,uid,name,pv_name,size_bytes,reclaim_policy,"
                    "selected,sha256,copy_state,delete_state) VALUES(?,?,?,?,?,?,?,?,?,?)",
                    (tid, p["uid"], p["name"], p["pv_name"], p["size_bytes"],
                     p["reclaim_policy"], 1 if p["name"] in retain_pvcs else 0,
                     p["sha256"], "pending", "pending"))
            return self.status(tid)

    # ---------------- 查询 ----------------
    def _ticket(self, tid: str) -> dict:
        t = self.db.one("SELECT * FROM tickets WHERE id=?", (tid,))
        if not t:
            raise NotFoundError(f"工单 {tid} 不存在")
        return t

    def list_tickets(self) -> list[dict]:
        return self.db.query("SELECT * FROM tickets ORDER BY created_at DESC")

    def _current_bytes(self, t: dict) -> int:
        """原卷现存字节 + 本单留存副本字节。"""
        total = sum(p["size_bytes"] for p in self.sim.list_pvcs(ns_uid=t["namespace_uid"]))
        rdir = self.retained_root / t["id"]
        if rdir.exists():
            total += sum(f.stat().st_size for f in rdir.rglob("data.bin"))
        return total

    def retained_items(self, tid: str) -> list[dict]:
        rdir = self.retained_root / tid
        out = []
        if rdir.exists():
            for d in sorted(rdir.iterdir()):
                meta = d / "meta.json"
                data = d / "data.bin"
                if meta.exists() and data.exists():
                    m = json.loads(meta.read_text(encoding="utf-8"))
                    out.append({**m, "data_bytes_on_disk": data.stat().st_size})
        return out

    def retained_file(self, tid: str, pvc_name: str) -> Path:
        self._ticket(tid)
        row = self.db.one(
            "SELECT * FROM pvcs WHERE ticket_id=? AND name=? AND selected=1", (tid, pvc_name))
        if not row:
            raise NotFoundError(f"工单 {tid} 中不存在选中 PVC {pvc_name!r}")
        path = self.retained_root / tid / pvc_name / "data.bin"
        meta = self.retained_root / tid / pvc_name / "meta.json"
        if not path.exists() or not meta.exists():
            raise NotFoundError(f"工单 {tid} 的 PVC {pvc_name!r} 尚未完成留存")
        return path

    def status(self, tid: str) -> dict:
        t = self._ticket(tid)
        pvcs = self.db.query("SELECT * FROM pvcs WHERE ticket_id=? ORDER BY name", (tid,))
        ops = self.db.query("SELECT * FROM ops WHERE ticket_id=? ORDER BY rowid", (tid,))
        current = self._current_bytes(t)
        cap = t["baseline_bytes"] + t["max_selected_bytes"]
        leftover_pvs = []
        for p in pvcs:
            pv = self.sim.get_pv(p["pv_name"])
            if pv:
                leftover_pvs.append({
                    "pv_name": pv["name"], "pvc_name": p["name"], "state": pv["state"],
                    "reclaim_policy": pv["reclaim_policy"], "size_bytes": pv["size_bytes"],
                })
        return {
            "ticket": {**t, "cap_bytes": cap},
            "pvcs": pvcs,
            "ops": ops,
            "storage": {
                "baseline_bytes": t["baseline_bytes"],
                "max_selected_bytes": t["max_selected_bytes"],
                "cap_bytes": cap,
                "current_bytes": current,
                "peak_bytes": t["peak_bytes"],
                "headroom_bytes": cap - current,
            },
            "retained": self.retained_items(tid),
            "leftover_pvs": leftover_pvs,
            "cluster": {
                "namespace_exists": self.sim.get_namespace_by_uid(t["namespace_uid"]) is not None,
                "remaining_pvcs": self.sim.list_pvcs(ns_uid=t["namespace_uid"]),
            },
        }

    # ---------------- 推进 ----------------
    def advance(self, tid: str) -> dict:
        with self._lock:
            t = self._ticket(tid)
            if t["state"] in ("DONE", "FAILED"):
                return self.status(tid)
            self._set_blocked(tid, None)
            # 先核对上一回合可能结果未明的删除
            self.reconcile(tid)
            if self.db.one("SELECT 1 AS x FROM ops WHERE ticket_id=? AND state='unknown'", (tid,)):
                self._set_blocked(tid, "存在结果未明的删除，核对后仍未确认（可能再次丢包）；请再次推进")
                return self.status(tid)
            for _ in range(MAX_STEPS_PER_ADVANCE):
                t = self._ticket(tid)
                if t["state"] in ("DONE", "FAILED") or t["blocked_reason"]:
                    break
                if t["state"] == "VALIDATING":
                    progressed = self._validate(t)
                elif t["state"] == "RETAINING":
                    progressed = self._retain_step(t)
                elif t["state"] == "DELETING_NAMESPACE":
                    progressed = self._delete_ns_step(t)
                else:
                    progressed = False
                if not progressed:
                    break
                self._bump_peak(self._ticket(tid))
            return self.status(tid)

    def _validate(self, t: dict) -> bool:
        tid = t["id"]
        ns = self.sim.get_namespace_by_uid(t["namespace_uid"])
        if not ns:
            self._set_blocked(
                tid,
                f"开单时的命名空间 {t['namespace_name']}(uid={t['namespace_uid']}) 已不存在；"
                "若系同名重建，其 UID 不同，本单不会作用于新命名空间。请人工核查后处理。")
            return False
        missing = [p["name"] for p in self.db.query(
            "SELECT * FROM pvcs WHERE ticket_id=? AND selected=1", (tid,))
            if self.sim.get_pvc(p["uid"]) is None]
        if missing:
            self._set_blocked(tid, f"选中 PVC 已不存在，无法留存: {missing}")
            return False
        self.db.execute("UPDATE tickets SET state='RETAINING' WHERE id=?", (tid,))
        return True

    def _retain_step(self, t: dict) -> bool:
        tid = t["id"]
        rows = self.db.query(
            "SELECT * FROM pvcs WHERE ticket_id=? AND selected=1 ORDER BY name", (tid,))
        todo = [r for r in rows
                if not (r["copy_state"] == "verified" and r["delete_state"] == "confirmed")]
        if not todo:
            self.db.execute("UPDATE tickets SET state='DELETING_NAMESPACE' WHERE id=?", (tid,))
            return True
        r = todo[0]
        if r["copy_state"] == "pending":
            current = self._current_bytes(t)
            cap = t["baseline_bytes"] + t["max_selected_bytes"]
            if current + r["size_bytes"] > cap:
                self._set_blocked(
                    tid,
                    f"存储上限保护：当前 {current} + 待拷 {r['size_bytes']} 将超过上限 {cap}"
                    f"（基线 {t['baseline_bytes']} + 最大选中卷 {t['max_selected_bytes']}），暂停拷贝")
                return False
            self._copy_to_retained(t, r)
            return True
        if r["copy_state"] == "copied":
            return self._verify_retained(t, r)
        # 已验证 -> 删除源 PVC（按 PV 回收策略由模拟端处理）
        return self._send_delete(t, "pvc", r["uid"], r["name"])

    def _delete_ns_step(self, t: dict) -> bool:
        tid = t["id"]
        if not self._send_delete(t, "namespace", t["namespace_uid"], t["namespace_name"]):
            return False
        ns = self.sim.get_namespace_by_uid(t["namespace_uid"])
        remaining = self.sim.list_pvcs(ns_uid=t["namespace_uid"])
        if ns is None and not remaining:
            self.db.execute("UPDATE tickets SET state='DONE', done_at=? WHERE id=?",
                            (now_iso(), tid))
            return True
        self._set_blocked(tid, "命名空间或未选卷尚未完全移除，等待核对")
        return False

    # ---------------- 留存 ----------------
    def _copy_to_retained(self, t: dict, r: dict) -> None:
        tid = t["id"]
        pvc = self.sim.get_pvc(r["uid"])
        if not pvc:
            self._set_blocked(tid, f"选中 PVC {r['name']}(uid={r['uid']}) 已消失，无法留存")
            return
        src = self.sim.pv_data_path(pvc["pv_name"])
        dst_dir = self.retained_root / tid / r["name"]
        dst_dir.mkdir(parents=True, exist_ok=True)
        dst = dst_dir / "data.bin"
        shutil.copyfile(src, dst)
        with open(dst, "rb") as f:
            os.fsync(f.fileno())
        write_json(dst_dir / "meta.json", {
            "ticket_id": tid, "pvc_name": r["name"], "pvc_uid": r["uid"],
            "source_namespace": t["namespace_name"],
            "source_namespace_uid": t["namespace_uid"],
            "pv_name": r["pv_name"], "size_bytes": r["size_bytes"],
            "sha256": r["sha256"], "retained_at": now_iso(),
        })
        self.db.execute("UPDATE pvcs SET copy_state='copied' WHERE ticket_id=? AND uid=?",
                        (tid, r["uid"]))

    def _verify_retained(self, t: dict, r: dict) -> bool:
        """独立读回校验：只读留存目录中的副本，与开单时快照的 sha256 比对。"""
        tid = t["id"]
        dst = self.retained_root / tid / r["name"] / "data.bin"
        if not dst.exists():
            self.db.execute("UPDATE tickets SET state='FAILED', blocked_reason=? WHERE id=?",
                            (f"留存副本缺失: {dst}", tid))
            return False
        actual = sha256_file(dst)
        if actual != r["sha256"] or dst.stat().st_size != r["size_bytes"]:
            self.db.execute("UPDATE tickets SET state='FAILED', blocked_reason=? WHERE id=?",
                            (f"留存副本读回校验失败：{r['name']} sha256/大小与开单快照不符，"
                             "已中止（对应源 PVC 未删除）", tid))
            return False
        self.db.execute("UPDATE pvcs SET copy_state='verified' WHERE ticket_id=? AND uid=?",
                        (tid, r["uid"]))
        return True

    # ---------------- 删除（带幂等日志） ----------------
    def _send_delete(self, t: dict, kind: str, target_uid: str, target_name: str) -> bool:
        tid = t["id"]
        key = f"{tid}:{kind}:{target_uid}"
        op = self.db.one("SELECT * FROM ops WHERE idempotency_key=?", (key,))
        if op and op["state"] == "confirmed":
            return True  # 已确认的删除不再发送
        if op is None:
            self.db.execute(
                "INSERT INTO ops(idempotency_key,ticket_id,kind,target_uid,target_name,"
                "state,attempts,last_error,updated_at) VALUES(?,?,?,?,?,?,0,NULL,?)",
                (key, tid, kind, target_uid, target_name, "pending", now_iso()))
        try:
            if kind == "pvc":
                self.sim.delete_pvc(target_uid, key)
            else:
                self.sim.delete_namespace(target_uid, key)
        except DroppedResponseError as e:
            self.db.execute(
                "UPDATE ops SET state='unknown', attempts=attempts+1, last_error=?, "
                "updated_at=? WHERE idempotency_key=?", (str(e), now_iso(), key))
            if kind == "pvc":
                self.db.execute(
                    "UPDATE pvcs SET delete_state='unknown' WHERE ticket_id=? AND uid=?",
                    (tid, target_uid))
            self._set_blocked(
                tid,
                f"删除回包丢失：{kind} {target_name}(uid={target_uid}) 服务端可能已生效，"
                "结果未明；将在核对后续办（已确认的删除不会重发）")
            return False
        except NotFoundError as e:
            # 目标已不存在 -> 删除实际已生效，幂等确认为止
            self.db.execute(
                "UPDATE ops SET state='confirmed', attempts=attempts+1, last_error=?, "
                "updated_at=? WHERE idempotency_key=?",
                (f"target already gone: {e}", now_iso(), key))
            if kind == "pvc":
                self.db.execute(
                    "UPDATE pvcs SET delete_state='confirmed' WHERE ticket_id=? AND uid=?",
                    (tid, target_uid))
            return True
        self.db.execute(
            "UPDATE ops SET state='confirmed', attempts=attempts+1, updated_at=? "
            "WHERE idempotency_key=?", (now_iso(), key))
        if kind == "pvc":
            self.db.execute(
                "UPDATE pvcs SET delete_state='confirmed' WHERE ticket_id=? AND uid=?",
                (tid, target_uid))
        return True

    # ---------------- 核对（启动时与推进前） ----------------
    def reconcile(self, tid: str | None = None) -> int:
        """核对结果未明(unknown)的删除：目标已消失则标记确认（不重发），仍在则重发。"""
        resolved = 0
        with self._lock:
            if tid:
                rows = self.db.query(
                    "SELECT * FROM ops WHERE state='unknown' AND ticket_id=?", (tid,))
            else:
                rows = self.db.query("SELECT * FROM ops WHERE state='unknown'")
            for op in rows:
                if op["kind"] == "pvc":
                    exists = self.sim.get_pvc(op["target_uid"]) is not None
                else:
                    exists = self.sim.get_namespace_by_uid(op["target_uid"]) is not None
                if not exists:
                    self.db.execute(
                        "UPDATE ops SET state='confirmed', last_error=?, updated_at=? "
                        "WHERE idempotency_key=?",
                        ("reconciled: target already gone, delete took effect; NOT resent",
                         now_iso(), op["idempotency_key"]))
                    if op["kind"] == "pvc":
                        self.db.execute(
                            "UPDATE pvcs SET delete_state='confirmed' "
                            "WHERE ticket_id=? AND uid=?", (op["ticket_id"], op["target_uid"]))
                    resolved += 1
                else:
                    t = self._ticket(op["ticket_id"])
                    if self._send_delete(t, op["kind"], op["target_uid"], op["target_name"]):
                        resolved += 1
        return resolved

    # ---------------- 辅助 ----------------
    def _set_blocked(self, tid: str, reason: str | None) -> None:
        self.db.execute("UPDATE tickets SET blocked_reason=? WHERE id=?", (reason, tid))

    def _bump_peak(self, t: dict) -> None:
        current = self._current_bytes(t)
        self.db.execute("UPDATE tickets SET peak_bytes=MAX(peak_bytes, ?) WHERE id=?",
                        (current, t["id"]))

    def reset(self) -> None:
        with self._lock:
            self.sim.reset()
            shutil.rmtree(self.retained_root, ignore_errors=True)
            self.retained_root.mkdir(parents=True, exist_ok=True)
            for table in ("ops", "pvcs", "tickets"):
                self.db.execute(f"DELETE FROM {table}")
