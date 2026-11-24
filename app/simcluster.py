"""文件模拟的 K8s 集群。

目录即集群：
  <root>/namespaces/<ns_uid>/meta.json     命名空间（按 UID 寻址，名称可重复）
  <root>/pvcs/<pvc_uid>/meta.json          PVC（绑定对象，已停写）
  <root>/pvs/<pv_name>/meta.json + data.bin  PV（真实持有卷数据文件）
  <root>/audit.jsonl                       模拟端审计日志（每次删除一行）

删除语义：
  * 删除 PVC 时按其所绑 PV 的回收策略处理：
      Delete  -> 删除 PV 及数据文件
      Recycle -> 清空数据（模拟 scrub），PV 转为 Available
      Retain  -> 保留 PV 与数据，PV 转为 Released
  * 删除命名空间会级联删除其下全部 PVC。
  * 故障注入：可配置接下来 N 次删除“服务端生效但回包丢失”，
    此时删除已落盘，但调用方收到 DroppedResponseError（结果未明）。
"""
from __future__ import annotations

import json
import os
import shutil
import threading
import uuid
from pathlib import Path

from .util import now_iso, read_json, sha256_file, write_json


class NotFoundError(Exception):
    pass


class ConflictError(Exception):
    pass


class DroppedResponseError(Exception):
    """删除已在服务端生效，但回包丢失：调用方无法确认结果。"""


RECLAIM_POLICIES = ("Delete", "Retain", "Recycle")


class SimCluster:
    def __init__(self, root: Path):
        self.root = Path(root)
        self.ns_root = self.root / "namespaces"
        self.pvc_root = self.root / "pvcs"
        self.pv_root = self.root / "pvs"
        for d in (self.ns_root, self.pvc_root, self.pv_root):
            d.mkdir(parents=True, exist_ok=True)
        self.audit_path = self.root / "audit.jsonl"
        self._lock = threading.RLock()
        self._drop_delete_responses = 0  # 故障注入计数（内存态，重启清零）

    # ---------------- 故障注入 ----------------
    def set_drop_delete_responses(self, n: int) -> int:
        with self._lock:
            self._drop_delete_responses = n
            return self._drop_delete_responses

    def get_fault(self) -> dict:
        return {"drop_delete_responses": self._drop_delete_responses}

    def _maybe_drop(self) -> bool:
        if self._drop_delete_responses > 0:
            self._drop_delete_responses -= 1
            return True
        return False

    # ---------------- 审计 ----------------
    def _audit(self, action: str, **fields) -> None:
        entry = {"ts": now_iso(), "action": action, **fields}
        with self._lock:
            with open(self.audit_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(entry, ensure_ascii=False) + "\n")

    def audit_log(self) -> list[dict]:
        if not self.audit_path.exists():
            return []
        out = []
        for line in self.audit_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line:
                out.append(json.loads(line))
        return out

    # ---------------- 查询 ----------------
    def get_namespace_by_uid(self, uid: str) -> dict | None:
        meta = self.ns_root / uid / "meta.json"
        return read_json(meta) if meta.exists() else None

    def get_namespace_by_name(self, name: str) -> dict | None:
        for d in sorted(self.ns_root.iterdir()):
            meta = d / "meta.json"
            if meta.exists():
                m = read_json(meta)
                if m["name"] == name:
                    return m
        return None

    def list_namespaces(self) -> list[dict]:
        return [read_json(d / "meta.json") for d in sorted(self.ns_root.iterdir()) if (d / "meta.json").exists()]

    def get_pvc(self, uid: str) -> dict | None:
        meta = self.pvc_root / uid / "meta.json"
        return read_json(meta) if meta.exists() else None

    def list_pvcs(self, ns_uid: str | None = None) -> list[dict]:
        out = []
        for d in sorted(self.pvc_root.iterdir()):
            meta = d / "meta.json"
            if meta.exists():
                m = read_json(meta)
                if ns_uid is None or m["ns_uid"] == ns_uid:
                    out.append(m)
        return out

    def get_pv(self, name: str) -> dict | None:
        meta = self.pv_root / name / "meta.json"
        return read_json(meta) if meta.exists() else None

    def list_pvs(self) -> list[dict]:
        return [read_json(d / "meta.json") for d in sorted(self.pv_root.iterdir()) if (d / "meta.json").exists()]

    def pv_data_path(self, pv_name: str) -> Path:
        return self.pv_root / pv_name / "data.bin"

    def cluster_dump(self) -> dict:
        pvs = []
        for pv in self.list_pvs():
            data = self.pv_root / pv["name"] / "data.bin"
            pvs.append({**pv, "data_bytes_on_disk": data.stat().st_size if data.exists() else 0})
        return {"namespaces": self.list_namespaces(), "pvcs": self.list_pvcs(), "pvs": pvs}

    # ---------------- 写入 ----------------
    def seed_namespace(self, name: str, pvcs: list[dict]) -> dict:
        """创建命名空间及若干已停写的绑定卷（数据落盘为文件）。"""
        with self._lock:
            if self.get_namespace_by_name(name):
                raise ConflictError(f"命名空间 {name!r} 已存在")
            ns_uid = uuid.uuid4().hex
            write_json(self.ns_root / ns_uid / "meta.json",
                       {"name": name, "uid": ns_uid, "created_at": now_iso()})
            created = []
            for spec in pvcs:
                pvc_uid = uuid.uuid4().hex
                pv_name = f"pv-{pvc_uid[:12]}"
                size = int(spec["size_bytes"])
                policy = spec.get("reclaim_policy", "Delete")
                if policy not in RECLAIM_POLICIES:
                    raise ValueError(f"未知回收策略: {policy}")
                data_path = self.pv_root / pv_name / "data.bin"
                data_path.parent.mkdir(parents=True, exist_ok=True)
                with open(data_path, "wb") as f:
                    f.write(os.urandom(size))
                digest = sha256_file(data_path)
                write_json(self.pv_root / pv_name / "meta.json", {
                    "name": pv_name, "reclaim_policy": policy, "state": "Bound",
                    "pvc_uid": pvc_uid, "ns_uid": ns_uid, "size_bytes": size,
                    "sha256": digest, "created_at": now_iso(),
                })
                write_json(self.pvc_root / pvc_uid / "meta.json", {
                    "name": spec["name"], "uid": pvc_uid, "ns_uid": ns_uid,
                    "ns_name": name, "pv_name": pv_name, "size_bytes": size,
                    "sha256": digest, "reclaim_policy": policy,
                    "stopped_writing": True, "created_at": now_iso(),
                })
                created.append({"name": spec["name"], "uid": pvc_uid, "pv_name": pv_name,
                                "size_bytes": size, "sha256": digest, "reclaim_policy": policy})
            self._audit("seed_namespace", namespace=name, ns_uid=ns_uid,
                        pvcs=[c["name"] for c in created])
            return {"namespace": name, "uid": ns_uid, "pvcs": created}

    # ---------------- 删除（含丢包注入） ----------------
    def delete_pvc(self, uid: str, idempotency_key: str) -> None:
        with self._lock:
            pvc = self.get_pvc(uid)
            if not pvc:
                self._audit("delete_pvc", target_uid=uid, idempotency_key=idempotency_key,
                            result="not_found")
                raise NotFoundError(f"PVC uid={uid} 不存在")
            self._remove_pvc_locked(pvc)
            dropped = self._maybe_drop()
            self._audit("delete_pvc", target_uid=uid, target_name=pvc["name"],
                        idempotency_key=idempotency_key,
                        result="applied_response_dropped" if dropped else "ok")
            if dropped:
                raise DroppedResponseError(
                    f"删除 PVC {pvc['name']}(uid={uid}) 已生效，但回包丢失")

    def delete_namespace(self, uid: str, idempotency_key: str) -> None:
        with self._lock:
            ns = self.get_namespace_by_uid(uid)
            if not ns:
                self._audit("delete_namespace", target_uid=uid, idempotency_key=idempotency_key,
                            result="not_found")
                raise NotFoundError(f"命名空间 uid={uid} 不存在")
            for pvc in self.list_pvcs(ns_uid=uid):
                self._remove_pvc_locked(pvc)
            shutil.rmtree(self.ns_root / uid)
            dropped = self._maybe_drop()
            self._audit("delete_namespace", target_uid=uid, target_name=ns["name"],
                        idempotency_key=idempotency_key,
                        result="applied_response_dropped" if dropped else "ok")
            if dropped:
                raise DroppedResponseError(
                    f"删除命名空间 {ns['name']}(uid={uid}) 已生效，但回包丢失")

    def _remove_pvc_locked(self, pvc: dict) -> None:
        """移除 PVC 对象，并按其 PV 的回收策略处理底层卷。"""
        shutil.rmtree(self.pvc_root / pvc["uid"], ignore_errors=True)
        pv_dir = self.pv_root / pvc["pv_name"]
        pv_meta_path = pv_dir / "meta.json"
        if not pv_meta_path.exists():
            return
        pv = read_json(pv_meta_path)
        policy = pv["reclaim_policy"]
        if policy == "Delete":
            shutil.rmtree(pv_dir, ignore_errors=True)
        elif policy == "Recycle":
            data = pv_dir / "data.bin"
            if data.exists():
                data.write_bytes(b"")  # 模拟 scrub
            pv.update({"state": "Available", "pvc_uid": None, "recycled_at": now_iso()})
            write_json(pv_meta_path, pv)
        elif policy == "Retain":
            pv.update({"state": "Released", "pvc_uid": None, "released_at": now_iso()})
            write_json(pv_meta_path, pv)

    # ---------------- 复位 ----------------
    def reset(self) -> None:
        with self._lock:
            for d in (self.ns_root, self.pvc_root, self.pv_root):
                shutil.rmtree(d, ignore_errors=True)
                d.mkdir(parents=True, exist_ok=True)
            if self.audit_path.exists():
                self.audit_path.unlink()
            self._drop_delete_responses = 0
