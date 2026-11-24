"""清退引擎：按工单推进命名空间清理的状态机。

阶段：VALIDATING -> RETAINING -> DELETING_NAMESPACE -> DONE（异常：FAILED）
阻塞：blocked_reason 字段记录当前阻塞原因，推进时自动清除并重试。

关键不变量：
  * 先留存、独立读回校验 sha256，再删除对应源 PVC。
  * 容量按“开单快照中各 PV 的实际正文长度 + 本单留存副本正文长度”统计；
    PVC 对象删除后，Retain/Recycle 对应的 PV 仍可能有正文或已被 scrub。
  * 容量上限为 开单总量 + 最大选中卷。Delete/Recycle 选中卷验后删除会腾出原件，
    Retain 选中卷则会长期同时保留原件和独立副本，因此必要时只提前回收可腾位的
    未选卷；能留到命名空间收尾处理的未选卷不提前删除。
  * 所有删除按开单时快照的 UID 寻址；同名重建的命名空间 UID 不同，不受影响。
  * 删除操作写入 ops 日志（幂等键）；已确认(confirmed)的删除绝不重发；
    结果未明(unknown)的删除先 GET/stat 核对：目标已消失且核实腾位结果后确认，
    仍在才重发。
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

    # ---------------- 查询与实际容量计量 ----------------
    def _ticket(self, tid: str) -> dict:
        t = self.db.one("SELECT * FROM tickets WHERE id=?", (tid,))
        if not t:
            raise NotFoundError(f"工单 {tid} 不存在")
        return t

    def list_tickets(self) -> list[dict]:
        tickets = self.db.query("SELECT * FROM tickets ORDER BY created_at DESC")
        out: list[dict] = []
        for t in tickets:
            storage = self._storage_breakdown(t)
            cap = t["baseline_bytes"] + t["max_selected_bytes"]
            out.append({
                **t,
                "cap_bytes": cap,
                "current_bytes": storage["current_bytes"],
                "actual_bytes": storage["actual_bytes"],
                "actual_peak_bytes": storage["actual_peak_bytes"],
                "original_pv_bytes": storage["original_pv_bytes"],
                "retained_copy_bytes": storage["retained_copy_bytes"],
            })
        return out

    def _pv_live_bytes(self, pv_name: str) -> tuple[dict | None, int]:
        """Return live PV meta and actual data.bin body length.

        A missing PV object contributes zero.  The file size, rather than the PVC
        request size, is the measured occupancy; Recycle's scrub therefore shows
        up immediately as zero bytes.
        """
        pv = self.sim.get_pv(pv_name)
        if not pv:
            return None, 0
        path = self.sim.pv_data_path(pv_name)
        try:
            body = path.stat().st_size if path.is_file() else 0
        except OSError:
            body = 0
        return pv, body

    def _retained_copy_bytes(self, tid: str) -> int:
        rdir = self.retained_root / tid
        if not rdir.exists():
            return 0
        total = 0
        for path in rdir.rglob("data.bin"):
            try:
                if path.is_file():
                    total += path.stat().st_size
            except OSError:
                continue
        return total

    @staticmethod
    def _effective_policy(row: dict, pv: dict | None = None) -> str:
        if pv and pv.get("reclaim_policy"):
            return pv["reclaim_policy"]
        return row["reclaim_policy"]

    def _reclaimable_bytes(self, row: dict, pv: dict | None = None,
                           body: int | None = None, pvc_exists: bool | None = None) -> int:
        """Bytes deleting this PVC now would remove from the measured footprint."""
        if pvc_exists is False:
            return 0
        if pv is None:
            pv, live_body = self._pv_live_bytes(row["pv_name"])
            if body is None:
                body = live_body
        elif body is None:
            _, body = self._pv_live_bytes(row["pv_name"])
        if not pv:
            # SimCluster only applies policy when PV meta is present.  A missing
            # meta/data directory is already absent from PV occupancy.
            return 0
        policy = self._effective_policy(row, pv)
        if policy == "Retain":
            return 0
        # Delete removes the file/directory; Recycle truncates data.bin to zero.
        return max(0, int(body or 0))

    def _storage_breakdown(self, t: dict) -> dict:
        """Measure this ticket's original PVs and its independent copies."""
        rows = self.db.query("SELECT * FROM pvcs WHERE ticket_id=? ORDER BY rowid", (t["id"],))
        volumes: list[dict] = []
        original_pv_bytes = 0
        for r in rows:
            pv, body = self._pv_live_bytes(r["pv_name"])
            original_pv_bytes += body
            pvc = self.sim.get_pvc(r["uid"])
            pvc_exists = pvc is not None
            policy = self._effective_policy(r, pv)
            reclaimable = self._reclaimable_bytes(
                r, pv=pv, body=body, pvc_exists=pvc_exists)
            volumes.append({
                "pvc_uid": r["uid"],
                "pvc_name": r["name"],
                "pv_name": r["pv_name"],
                "selected": bool(r["selected"]),
                "snapshot_bytes": r["size_bytes"],
                "actual_bytes": body,
                "pvc_exists": pvc_exists,
                "pv_exists": pv is not None,
                "pv_state": (pv or {}).get("state", "Absent"),
                "reclaim_policy": policy,
                "reclaimable_bytes_now": reclaimable,
                "copy_state": r["copy_state"],
                "delete_state": r["delete_state"],
            })
        retained_copy_bytes = self._retained_copy_bytes(t["id"])
        current = original_pv_bytes + retained_copy_bytes
        cap = t["baseline_bytes"] + t["max_selected_bytes"]
        freed_row = self.db.one(
            "SELECT COALESCE(SUM(COALESCE(verified_freed_bytes, 0)), 0) AS n "
            "FROM ops WHERE ticket_id=? AND kind='pvc' AND state='confirmed'",
            (t["id"],))
        verified_freed_bytes = int(freed_row["n"] if freed_row else 0)
        return {
            "ticket_uid": t["id"],
            "namespace_uid": t["namespace_uid"],
            "baseline_bytes": t["baseline_bytes"],
            "max_selected_bytes": t["max_selected_bytes"],
            "cap_bytes": cap,
            "current_bytes": current,
            "actual_bytes": current,
            "occupied_bytes": current,
            "original_pv_bytes": original_pv_bytes,
            "original_pv_body_bytes": original_pv_bytes,
            "pv_body_bytes": original_pv_bytes,
            "existing_pv_bytes": original_pv_bytes,
            "retained_copy_bytes": retained_copy_bytes,
            "retained_file_bytes": retained_copy_bytes,
            "retained_bytes": retained_copy_bytes,
            "verified_freed_bytes": verified_freed_bytes,
            "freed_bytes": verified_freed_bytes,
            "headroom_bytes": cap - current,
            "peak_bytes": max(int(t["peak_bytes"]), current),
            "stored_peak_bytes": int(t["peak_bytes"]),
            "actual_peak_bytes": max(int(t["peak_bytes"]), current),
            "volumes": volumes,
        }

    def _current_bytes(self, t: dict) -> int:
        return self._storage_breakdown(t)["current_bytes"]

    def _capacity_plan(self, t: dict, pvcs: list[dict] | None = None,
                       storage: dict | None = None) -> dict:
        """Compute whether a compliant order can stay under the capacity cap.

        Delete/Recycle selected volumes replace their source with an equal-size
        verified copy and then free/truncate the source: net permanent extra is
        zero.  Retain selected volumes keep the source PV *and* add the copy, so
        all selected Retain sizes accumulate.  The cap already permits one
        max-selected-size overlap; only the excess must be obtained by early
        reclamation of unselected Delete/Recycle volumes.
        """
        if storage is None:
            storage = self._storage_breakdown(t)
        volumes = storage["volumes"]
        selected_retain_bytes = sum(
            v["snapshot_bytes"] for v in volumes
            if v["selected"] and v["reclaim_policy"] == "Retain")
        required_early = max(0, selected_retain_bytes - t["max_selected_bytes"])

        already_reclaimed = 0
        remaining_yield = 0
        for v in volumes:
            if v["selected"]:
                continue
            already_reclaimed += max(0, v["snapshot_bytes"] - v["actual_bytes"])
            if v["pvc_exists"]:
                remaining_yield += v["reclaimable_bytes_now"]
        available = already_reclaimed + remaining_yield
        shortfall = max(0, required_early - available)
        return {
            "feasible": shortfall == 0,
            "solvable": shortfall == 0,
            "selected_retain_original_plus_copy_bytes": selected_retain_bytes,
            "allowed_overlap_bytes": t["max_selected_bytes"],
            "minimum_early_reclaim_bytes": required_early,
            "already_reclaimed_unselected_bytes": already_reclaimed,
            "remaining_reclaimable_unselected_bytes": remaining_yield,
            "available_reclaim_bytes": available,
            "shortfall_bytes": shortfall,
            "minimum_shortfall_bytes": shortfall,
            "minimum_missing_bytes": shortfall,
            "min_missing_bytes": shortfall,
            "missing_bytes": shortfall,
        }

    def _blocking_volumes(self, t: dict, ops: list[dict] | None = None,
                          storage: dict | None = None,
                          plan: dict | None = None) -> list[dict]:
        if storage is None:
            storage = self._storage_breakdown(t)
        if plan is None:
            plan = self._capacity_plan(t, storage=storage)
        if ops is None:
            ops = self.db.query("SELECT * FROM ops WHERE ticket_id=? ORDER BY rowid", (t["id"],))
        by_uid = {v["pvc_uid"]: v for v in storage["volumes"]}
        blockers: list[dict] = []
        seen: set[tuple[str, str]] = set()

        for op in ops:
            if op["state"] != "unknown":
                continue
            v = by_uid.get(op["target_uid"], {})
            code = "delete_result_unknown"
            key = (code, op["target_uid"])
            if key in seen:
                continue
            seen.add(key)
            blockers.append({
                "code": code,
                "kind": op["kind"],
                "target_uid": op["target_uid"],
                "target_name": op["target_name"],
                "pvc_name": v.get("pvc_name", op["target_name"]),
                "pv_name": v.get("pv_name"),
                "reclaim_policy": v.get("reclaim_policy"),
                "actual_bytes": v.get("actual_bytes"),
                "reclaimable_bytes_now": v.get("reclaimable_bytes_now"),
                "op_state": "unknown",
                "message": "删除回包丢失，实际是否腾位需重启/再次推进核对；已生效删除不会重发",
            })

        if plan["shortfall_bytes"] > 0:
            # Retain unselected volumes are the usual hard blocker: deleting the
            # PVC releases the object but K8s Retain semantics keep PV bytes.
            for v in storage["volumes"]:
                if v["selected"] or not v["pvc_exists"]:
                    continue
                if v["reclaimable_bytes_now"] <= 0 and v["actual_bytes"] > 0:
                    code = "retain_policy_holds_bytes"
                    key = (code, v["pvc_uid"])
                    if key in seen:
                        continue
                    seen.add(key)
                    blockers.append({
                        "code": code,
                        "kind": "pvc",
                        "target_uid": v["pvc_uid"],
                        "target_name": v["pvc_name"],
                        "pvc_name": v["pvc_name"],
                        "pv_name": v["pv_name"],
                        "reclaim_policy": v["reclaim_policy"],
                        "pv_state": v["pv_state"],
                        "actual_bytes": v["actual_bytes"],
                        "reclaimable_bytes_now": 0,
                        "message": (
                            f"卷 {v['pvc_name']}/{v['pv_name']} 策略为 "
                            f"{v['reclaim_policy']}，删 PVC 后仍保留 "
                            f"{v['actual_bytes']} 字节，不能用于腾位"),
                    })
            # If every remaining unselected Delete/Recycle volume is still too
            # small in aggregate, identify those candidates as well.
            if not any(b["code"] == "retain_policy_holds_bytes" for b in blockers):
                for v in storage["volumes"]:
                    if v["selected"] or not v["pvc_exists"]:
                        continue
                    code = "insufficient_reclaim_candidate"
                    key = (code, v["pvc_uid"])
                    if key in seen:
                        continue
                    seen.add(key)
                    blockers.append({
                        "code": code,
                        "kind": "pvc",
                        "target_uid": v["pvc_uid"],
                        "target_name": v["pvc_name"],
                        "pvc_name": v["pvc_name"],
                        "pv_name": v["pv_name"],
                        "reclaim_policy": v["reclaim_policy"],
                        "pv_state": v["pv_state"],
                        "actual_bytes": v["actual_bytes"],
                        "reclaimable_bytes_now": v["reclaimable_bytes_now"],
                        "message": "全部可回收未选卷回收后仍未达到容量下限",
                    })

            # Selected Retain volumes structurally require both the retained
            # copy and the original PV.  List them when there is no unselected
            # blocker so the response still identifies the capacity contributors.
            for v in storage["volumes"]:
                if not v["selected"] or v["reclaim_policy"] != "Retain":
                    continue
                code = "selected_retain_duplicate_overhead"
                key = (code, v["pvc_uid"])
                if key in seen:
                    continue
                seen.add(key)
                blockers.append({
                    "code": code,
                    "kind": "pvc",
                    "target_uid": v["pvc_uid"],
                    "target_name": v["pvc_name"],
                    "pvc_name": v["pvc_name"],
                    "pv_name": v["pv_name"],
                    "reclaim_policy": v["reclaim_policy"],
                    "pv_state": v["pv_state"],
                    "actual_bytes": v["actual_bytes"],
                    "reclaimable_bytes_now": 0,
                    "message": (
                        f"选中 Retain 卷 {v['pvc_name']}/{v['pv_name']} 校验后也只能删 PVC；"
                        "Retain PV 原件与按单留存副本会同时占用空间"),
                })
        return blockers

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
        ops_raw = self.db.query("SELECT * FROM ops WHERE ticket_id=? ORDER BY rowid", (tid,))
        ops: list[dict] = []
        for op in ops_raw:
            item = dict(op)
            if item["kind"] == "pvc" and item.get("verified_freed_bytes") is None:
                item["verified_freed_bytes"] = self._pvc_verified_freed_bytes(
                    tid, item["target_uid"])
            item["freed_bytes"] = item.get("verified_freed_bytes")
            item["actual_freed_bytes"] = item.get("verified_freed_bytes")
            ops.append(item)
        storage = self._storage_breakdown(t)
        plan = self._capacity_plan(t, pvcs=pvcs, storage=storage)
        blockers = self._blocking_volumes(t, ops=ops, storage=storage, plan=plan)
        blocker_names = [b.get("pvc_name") or b.get("target_name") for b in blockers]
        storage["blocking_volumes"] = blockers
        storage["blocking_volume_names"] = blocker_names
        cap = t["baseline_bytes"] + t["max_selected_bytes"]
        leftover_pvs = []
        for p in pvcs:
            pv = self.sim.get_pv(p["pv_name"])
            if pv:
                leftover_pvs.append({
                    "pv_name": pv["name"], "pvc_name": p["name"], "state": pv["state"],
                    "reclaim_policy": pv["reclaim_policy"], "size_bytes": pv["size_bytes"],
                    "actual_bytes": self._pv_live_bytes(p["pv_name"])[1],
                })
        return {
            "ticket": {**t, "cap_bytes": cap},
            "pvcs": pvcs,
            "ops": ops,
            "operations": ops,
            "storage": storage,
            "capacity_plan": plan,
            "minimum_missing_bytes": plan["minimum_missing_bytes"],
            "blockers": blockers,
            "blocking_volumes": blockers,
            "blocked_volumes": blockers,
            "blocking_volume_names": blocker_names,
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
            self._bump_peak(self._ticket(tid))
            unknown = self.db.query(
                "SELECT * FROM ops WHERE ticket_id=? AND state='unknown' ORDER BY rowid", (tid,))
            if unknown:
                names = ", ".join(f"{o['kind']}:{o['target_name']}" for o in unknown)
                self._set_blocked(
                    tid,
                    f"存在结果未明的删除（{names}），已核对但目标仍未确认（可能再次丢包）；"
                    "请再次推进或重启续办。已确认的删除不会重发。")
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

        storage = self._storage_breakdown(t)
        plan = self._capacity_plan(t, storage=storage)
        if not plan["feasible"]:
            blockers = self._blocking_volumes(t, storage=storage, plan=plan)
            names = [b.get("pv_name") and f"{b['pvc_name']}/{b['pv_name']}" or b["target_name"]
                     for b in blockers]
            extra = f"；阻塞卷: {names}" if names else "；全部可提前回收的未选卷均已计入"
            short = plan["shortfall_bytes"]
            kib = f"（{short // 1024} KiB）" if short % 1024 == 0 else ""
            self._set_blocked(
                tid,
                "容量规划无解：即使按合规顺序处理并提前回收全部可腾位的未选卷，"
                f"峰值仍会超过上限 {plan['allowed_overlap_bytes'] + t['baseline_bytes']} 字节；"
                f"最少还缺 {short} 字节{kib}。本次推进不删除任何卷。{extra}")
            return False

        self.db.execute("UPDATE tickets SET state='RETAINING' WHERE id=?", (tid,))
        return True

    def _selected_order_key(self, r: dict) -> tuple[int, int, str]:
        # Finish an already copied/verified volume first (especially useful
        # after restart), then process Delete/Recycle before Retain so source
        # bytes are turned into verified copies without accumulating duplicates.
        in_progress = 0 if r["copy_state"] != "pending" else 1
        persistent = 1 if r["reclaim_policy"] == "Retain" else 0
        return (in_progress, persistent, r["name"])

    def _retain_step(self, t: dict) -> bool:
        tid = t["id"]
        rows = self.db.query(
            "SELECT * FROM pvcs WHERE ticket_id=? AND selected=1", (tid,))
        todo = [r for r in rows
                if not (r["copy_state"] == "verified" and r["delete_state"] == "confirmed")]
        if not todo:
            self.db.execute("UPDATE tickets SET state='DELETING_NAMESPACE' WHERE id=?", (tid,))
            return True
        r = sorted(todo, key=self._selected_order_key)[0]
        if r["copy_state"] == "pending":
            storage = self._storage_breakdown(t)
            cap = t["baseline_bytes"] + t["max_selected_bytes"]
            dst = self.retained_root / tid / r["name"] / "data.bin"
            already_copied = dst.stat().st_size if dst.exists() and dst.is_file() else 0
            additional_copy_bytes = max(0, r["size_bytes"] - already_copied)
            overage = storage["current_bytes"] + additional_copy_bytes - cap
            if overage > 0:
                # Do not copy into a cap violation.  Early reclamation is only
                # used when the ticket genuinely needs the space.
                return self._free_capacity(t, overage)
            return self._copy_to_retained(t, r)
        if r["copy_state"] == "copied":
            return self._verify_retained(t, r)
        # 已验证 -> 删除源 PVC（按 PV 回收策略由模拟端处理）
        return self._send_delete(t, "pvc", r["uid"], r["name"])

    def _free_capacity(self, t: dict, required_bytes: int) -> bool:
        """Early-reclaim an unselected volume only when needed for capacity."""
        tid = t["id"]
        rows = self.db.query(
            "SELECT * FROM pvcs WHERE ticket_id=? AND selected=0 ORDER BY name", (tid,))
        candidates: list[tuple[int, dict]] = []
        total_yield = 0
        for r in rows:
            if self.sim.get_pvc(r["uid"]) is None:
                continue
            pv, body = self._pv_live_bytes(r["pv_name"])
            y = self._reclaimable_bytes(r, pv=pv, body=body, pvc_exists=True)
            if y > 0:
                candidates.append((y, r))
                total_yield += y

        if total_yield < required_bytes:
            short = required_bytes - total_yield
            storage = self._storage_breakdown(t)
            plan = self._capacity_plan(t, storage=storage)
            blockers = self._blocking_volumes(t, storage=storage, plan=plan)
            names = [f"{b['pvc_name']}/{b.get('pv_name')}" for b in blockers]
            suffix = f"；阻塞卷: {names}" if names else ""
            self._set_blocked(
                tid,
                f"腾位受阻：本次拷贝前至少还需 {required_bytes} 字节，全部可提前回收的 "
                f"Delete/Recycle 未选卷只能腾出 {total_yield} 字节，即时缺口 {short} 字节"
                f"（全局最少还缺 {plan['shortfall_bytes']} 字节）；未发送新的删卷请求。{suffix}")
            return False

        # Prefer the smallest volume that alone covers the need; otherwise make
        # the largest possible step and recompute on the next state-machine pass.
        enough = [(y, r) for y, r in candidates if y >= required_bytes]
        if enough:
            y, r = min(enough, key=lambda x: x[0])
        else:
            y, r = max(candidates, key=lambda x: x[0])
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
    def _copy_to_retained(self, t: dict, r: dict) -> bool:
        tid = t["id"]
        pvc = self.sim.get_pvc(r["uid"])
        if not pvc:
            self._set_blocked(tid, f"选中 PVC {r['name']}(uid={r['uid']}) 已消失，无法留存")
            return False
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
        return True

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

    # ---------------- 删除（带幂等日志与腾位核对） ----------------
    def _pvc_verified_freed_bytes(self, ticket_id: str, target_uid: str) -> int | None:
        row = self.db.one(
            "SELECT * FROM pvcs WHERE ticket_id=? AND uid=?", (ticket_id, target_uid))
        if not row:
            return None
        _, body = self._pv_live_bytes(row["pv_name"])
        return max(0, int(row["size_bytes"]) - body)

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
            freed = self._pvc_verified_freed_bytes(tid, target_uid) if kind == "pvc" else None
            self.db.execute(
                "UPDATE ops SET state='confirmed', attempts=attempts+1, last_error=?, "
                "verified_freed_bytes=COALESCE(?, verified_freed_bytes), updated_at=? "
                "WHERE idempotency_key=?",
                (f"target already gone: {e}", freed, now_iso(), key))
            if kind == "pvc":
                self.db.execute(
                    "UPDATE pvcs SET delete_state='confirmed' WHERE ticket_id=? AND uid=?",
                    (tid, target_uid))
            return True

        freed = self._pvc_verified_freed_bytes(tid, target_uid) if kind == "pvc" else None
        self.db.execute(
            "UPDATE ops SET state='confirmed', attempts=attempts+1, "
            "verified_freed_bytes=COALESCE(?, verified_freed_bytes), updated_at=? "
            "WHERE idempotency_key=?", (freed, now_iso(), key))
        if kind == "pvc":
            self.db.execute(
                "UPDATE pvcs SET delete_state='confirmed' WHERE ticket_id=? AND uid=?",
                (tid, target_uid))
        return True

    # ---------------- 核对（启动时与推进前） ----------------
    def reconcile(self, tid: str | None = None) -> int:
        """核对丢回包(unknown)或进程中断留下的 pending 删除。

        目标消失则确认且不重发，仍在则重发。对 PVC 删除还会 stat 原 PV 正文，
        核实 Delete/Recycle 是否实际腾出空间，或 Retain 是否按策略保留正文
        （腾出 0 字节）。
        """
        resolved = 0
        with self._lock:
            if tid:
                rows = self.db.query(
                    "SELECT * FROM ops WHERE state IN ('unknown', 'pending') "
                    "AND ticket_id=? ORDER BY rowid", (tid,))
            else:
                rows = self.db.query(
                    "SELECT * FROM ops WHERE state IN ('unknown', 'pending') ORDER BY rowid")
            for op in rows:
                if op["kind"] == "pvc":
                    exists = self.sim.get_pvc(op["target_uid"]) is not None
                else:
                    exists = self.sim.get_namespace_by_uid(op["target_uid"]) is not None
                if not exists:
                    freed = None
                    detail = ""
                    if op["kind"] == "pvc":
                        freed = self._pvc_verified_freed_bytes(
                            op["ticket_id"], op["target_uid"])
                        detail = f"; verified_actual_freed_bytes={freed}"
                    self.db.execute(
                        "UPDATE ops SET state='confirmed', last_error=?, "
                        "verified_freed_bytes=COALESCE(?, verified_freed_bytes), updated_at=? "
                        "WHERE idempotency_key=?",
                        ("reconciled: target already gone, delete took effect; NOT resent"
                         f"; 已核实实际腾出 {freed} 字节"
                         + detail,
                         freed, now_iso(), op["idempotency_key"]))
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
