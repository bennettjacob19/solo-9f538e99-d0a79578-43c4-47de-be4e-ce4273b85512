"""端到端自测：命名空间清退留存演练。

覆盖：
  1. 开单 -> 推进，注入 1 次删除丢包 -> 删除生效但回包丢失，工单阻塞待核对
  2. 重启服务 -> 启动时核对结果未明的删除（GET 确认，不重发）-> 推进至 DONE
  3. 存储水位：峰值 <= 开单基线 + 最大选中卷
  4. 凭单号读回所选数据，sha256 与开单快照一致
  5. 未选卷按 PV 回收策略处理（Delete 删除 / Retain 保留为 Released）
  6. 同名重建命名空间不受旧单影响；新命名空间可独立开单清退
  7. 已确认的删除全程不重发（审计日志每个目标仅一条删除记录）

运行：.venv/bin/python tests/selftest.py
"""
from __future__ import annotations

import hashlib
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[1]
PORT = 8765
BASE = f"http://127.0.0.1:{PORT}"

FAILURES: list[str] = []
CHECKS = 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global CHECKS
    CHECKS += 1
    if cond:
        print(f"  PASS  {name}")
    else:
        FAILURES.append(f"{name}: {detail}")
        print(f"  FAIL  {name}  {detail}")


def sha(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()


def start_server(data_dir: Path) -> subprocess.Popen:
    env = dict(os.environ, DATA_DIR=str(data_dir))
    log = open(data_dir / "server.log", "ab")
    p = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "app.main:app", "--port", str(PORT)],
        cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT)
    deadline = time.time() + 30
    while time.time() < deadline:
        if p.poll() is not None:
            raise RuntimeError(f"server exited early, see {data_dir}/server.log")
        try:
            if httpx.get(f"{BASE}/sim/cluster", timeout=2).status_code == 200:
                return p
        except httpx.TransportError:
            time.sleep(0.3)
    p.kill()
    raise RuntimeError("server did not become ready")


def stop_server(p: subprocess.Popen) -> None:
    p.terminate()
    try:
        p.wait(timeout=10)
    except subprocess.TimeoutExpired:
        p.kill()
        p.wait(timeout=5)


def main() -> int:
    data_dir = Path(tempfile.mkdtemp(prefix="ns-retain-drill-"))
    print(f"DATA_DIR={data_dir}")
    server = start_server(data_dir)
    c = httpx.Client(base_url=BASE, timeout=30)
    try:
        print("\n== 0. /docs 可用 ==")
        check("GET /docs -> 200", c.get("/docs").status_code == 200)

        print("\n== 1. 造数：命名空间 demo + 4 个 PVC ==")
        seed = c.post("/sim/seed", json={
            "namespace": "demo",
            "pvcs": [
                {"name": "data-a", "size_bytes": 1_000_000, "reclaim_policy": "Delete"},
                {"name": "data-b", "size_bytes": 2_000_000, "reclaim_policy": "Delete"},
                {"name": "scratch", "size_bytes": 3_000_000, "reclaim_policy": "Delete"},
                {"name": "archive", "size_bytes": 500_000, "reclaim_policy": "Retain"},
            ],
        })
        check("seed -> 201", seed.status_code == 201, seed.text)
        seeded = {p["name"]: p for p in seed.json()["pvcs"]}
        ns_uid_v1 = seed.json()["uid"]
        baseline = 1_000_000 + 2_000_000 + 3_000_000 + 500_000
        max_sel = 2_000_000
        cap = baseline + max_sel

        print("\n== 2. 开单：清退 demo，保留 data-a / data-b ==")
        t = c.post("/tickets", json={"namespace": "demo",
                                     "retain_pvcs": ["data-a", "data-b"]})
        check("开单 -> 201", t.status_code == 201, t.text)
        tid = t.json()["ticket"]["id"]
        tk = t.json()["ticket"]
        check("开单快照基线正确", tk["baseline_bytes"] == baseline,
              f"got {tk['baseline_bytes']}")
        check("最大选中卷正确", tk["max_selected_bytes"] == max_sel)
        check("开单记录命名空间 UID", tk["namespace_uid"] == ns_uid_v1)
        bad = c.post("/tickets", json={"namespace": "demo", "retain_pvcs": []})
        check("空保留列表 -> 422", bad.status_code == 422)

        print("\n== 3. 注入 1 次删除丢包，推进 ==")
        c.post("/sim/fault", json={"drop_delete_responses": 1})
        r1 = c.post(f"/tickets/{tid}/advance").json()
        check("丢包后工单阻塞并说明原因",
              r1["ticket"]["blocked_reason"] is not None
              and "回包丢失" in r1["ticket"]["blocked_reason"],
              r1["ticket"]["blocked_reason"] or "no blocked_reason")
        unknown_ops = [o for o in r1["ops"] if o["state"] == "unknown"]
        check("存在 1 条结果未明的删除", len(unknown_ops) == 1, str(r1["ops"]))
        check("未明删除的对象是 data-a",
              unknown_ops and unknown_ops[0]["target_name"] == "data-a")
        pa = {p["name"]: p for p in r1["pvcs"]}
        check("data-a 已拷贝并验证（先留存后删除）",
              pa["data-a"]["copy_state"] == "verified", str(pa["data-a"]))
        check("data-a 源卷在服务端已被删除（删除生效、回包丢失）",
              all(p["name"] != "data-a" for p in c.get("/sim/cluster").json()["pvcs"]))
        check("水位未越上限", r1["storage"]["peak_bytes"] <= cap,
              f"peak={r1['storage']['peak_bytes']} cap={cap}")

        print("\n== 4. 重启服务：启动时核对未明删除 ==")
        stop_server(server)
        server = start_server(data_dir)  # 同一 DATA_DIR，触发启动核对
        r2 = c.get(f"/tickets/{tid}").json()
        op_a = [o for o in r2["ops"] if o["target_name"] == "data-a"][0]
        check("重启后未明删除已核对为 confirmed", op_a["state"] == "confirmed",
              str(op_a))
        check("核对记录注明未重发", "NOT resent" in (op_a["last_error"] or ""),
              op_a["last_error"])
        audit = c.get("/sim/audit").json()["entries"]
        del_a = [e for e in audit if e["action"] == "delete_pvc"
                 and e.get("target_name") == "data-a"]
        check("data-a 的删除全程只发送一次", len(del_a) == 1, str(del_a))

        print("\n== 5. 续办推进至 DONE ==")
        r3 = c.post(f"/tickets/{tid}/advance").json()
        check("工单 DONE", r3["ticket"]["state"] == "DONE", r3["ticket"]["state"])
        check("无阻塞原因", r3["ticket"]["blocked_reason"] is None)
        cl = c.get("/sim/cluster").json()
        check("原命名空间已移除", all(n["uid"] != ns_uid_v1 for n in cl["namespaces"]))
        check("未选卷(scratch/Delete)已移除",
              all(p["name"] != "scratch" for p in cl["pvcs"]))
        check("未选卷(archive/Retain)PVC 已移除",
              all(p["name"] != "archive" for p in cl["pvcs"]))
        leftover = {p["pvc_name"]: p for p in r3["leftover_pvs"]}
        check("Retain 策略 PV 保留为 Released 且数据在",
              leftover.get("archive", {}).get("state") == "Released", str(leftover))
        check("Delete 策略 PV 已随卷删除", "scratch" not in leftover)
        check("存储峰值 <= 基线+最大选中卷",
              r3["storage"]["peak_bytes"] <= cap,
              f"peak={r3['storage']['peak_bytes']} cap={cap}")
        check("存储峰值恰好触及上限边界（逐卷拷贝、验后即删）",
              r3["storage"]["peak_bytes"] == cap,
              f"peak={r3['storage']['peak_bytes']} cap={cap}")
        check("完成后仅剩留存副本字节",
              r3["storage"]["current_bytes"] == 3_000_000,
              str(r3["storage"]))

        print("\n== 6. 凭单号读回所选数据 ==")
        items = c.get(f"/tickets/{tid}/retained").json()["items"]
        check("留存清单含 data-a/data-b",
              sorted(i["pvc_name"] for i in items) == ["data-a", "data-b"])
        for name in ("data-a", "data-b"):
            body = c.get(f"/tickets/{tid}/retained/{name}").content
            check(f"{name} 读回 sha256 与开单快照一致",
                  sha(body) == seeded[name]["sha256"])
        check("未选 PVC 不可读回 -> 404",
              c.get(f"/tickets/{tid}/retained/scratch").status_code == 404)

        print("\n== 7. 同名重建不受旧单影响 ==")
        seed2 = c.post("/sim/seed", json={
            "namespace": "demo",
            "pvcs": [{"name": "data-x", "size_bytes": 100_000,
                      "reclaim_policy": "Delete"}],
        })
        check("同名重建 -> 201", seed2.status_code == 201, seed2.text)
        ns_uid_v2 = seed2.json()["uid"]
        check("重建后 UID 不同", ns_uid_v2 != ns_uid_v1)
        again = c.post(f"/tickets/{tid}/advance").json()
        check("旧单重复推进仍为 DONE", again["ticket"]["state"] == "DONE")
        cl = c.get("/sim/cluster").json()
        check("新命名空间未被旧单触碰",
              any(n["uid"] == ns_uid_v2 for n in cl["namespaces"])
              and any(p["name"] == "data-x" for p in cl["pvcs"]))

        print("\n== 8. 新命名空间独立开单 + 命名空间删除丢包续办 ==")
        t2 = c.post("/tickets", json={"namespace": "demo", "retain_pvcs": ["data-x"]})
        check("第二张开单 -> 201", t2.status_code == 201, t2.text)
        tid2 = t2.json()["ticket"]["id"]
        check("第二单绑定新 UID", t2.json()["ticket"]["namespace_uid"] == ns_uid_v2)
        c.post("/sim/fault", json={"drop_delete_responses": 2})  # PVC 删 + NS 删各丢一次
        s1 = c.post(f"/tickets/{tid2}/advance").json()
        check("第一次丢包：PVC 删除未明，工单阻塞",
              s1["ticket"]["blocked_reason"] is not None
              and any(o["state"] == "unknown" and o["kind"] == "pvc" for o in s1["ops"]))
        s2 = c.post(f"/tickets/{tid2}/advance").json()
        check("第二次丢包：命名空间删除未明，工单阻塞",
              s2["ticket"]["blocked_reason"] is not None
              and any(o["state"] == "unknown" and o["kind"] == "namespace"
                      for o in s2["ops"]), str(s2["ops"]))
        s3 = c.post(f"/tickets/{tid2}/advance").json()
        check("核对续办后 DONE", s3["ticket"]["state"] == "DONE", s3["ticket"]["state"])
        body = c.get(f"/tickets/{tid2}/retained/data-x").content
        check("data-x 凭单号读回一致", sha(body) == seed2.json()["pvcs"][0]["sha256"])
        check("第二单水位未越上限",
              s3["storage"]["peak_bytes"] <= s3["storage"]["cap_bytes"])

        print("\n== 9. 全局：已确认的删除从不重发 ==")
        audit = c.get("/sim/audit").json()["entries"]
        seen: dict[tuple, int] = {}
        for e in audit:
            if e["action"] in ("delete_pvc", "delete_namespace"):
                k = (e["action"], e["target_uid"])
                seen[k] = seen.get(k, 0) + 1
        check("每个删除目标全程仅收到一次删除",
              all(v == 1 for v in seen.values()), str(seen))
        check("旧单数据在新周期后仍可凭单号读回",
              sha(c.get(f"/tickets/{tid}/retained/data-a").content)
              == seeded["data-a"]["sha256"])
        check("工单列表含两单", len(c.get("/tickets").json()["tickets"]) == 2)
    finally:
        stop_server(server)

    print(f"\n{'=' * 50}")
    if FAILURES:
        print(f"FAILED {len(FAILURES)}/{CHECKS}:")
        for f in FAILURES:
            print(f"  - {f}")
        return 1
    print(f"ALL CHECKS PASSED ({CHECKS})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
