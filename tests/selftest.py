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
        check("无需提前腾位时，未选 scratch 不单独发 PVC 删除，留给命名空间收尾",
              not any(o["kind"] == "pvc" and o["target_name"] == "scratch" for o in r3["ops"]),
              str([(o["kind"], o["target_name"]) for o in r3["ops"]]))
        check("存储峰值 <= 基线+最大选中卷",
              r3["storage"]["peak_bytes"] <= cap,
              f"peak={r3['storage']['peak_bytes']} cap={cap}")
        check("存储峰值恰好触及上限边界（逐卷拷贝、验后即删）",
              r3["storage"]["peak_bytes"] == cap,
              f"peak={r3['storage']['peak_bytes']} cap={cap}")
        check("完成后实际占用=留存副本 + 尚在的 Retain PV 原件",
              r3["storage"]["current_bytes"] == 3_500_000
              and r3["storage"]["retained_copy_bytes"] == 3_000_000
              and r3["storage"]["original_pv_bytes"] == 500_000,
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

        print("\n== 10. 三卷 8 KiB：两个 Retain 选中卷 + 一个 Delete 未选卷（有解） ==")
        tiny = c.post("/sim/seed", json={
            "namespace": "tiny-ok",
            "pvcs": [
                {"name": "ra", "size_bytes": 8192, "reclaim_policy": "Retain"},
                {"name": "rb", "size_bytes": 8192, "reclaim_policy": "Retain"},
                {"name": "dc", "size_bytes": 8192, "reclaim_policy": "Delete"},
            ],
        })
        check("tiny-ok seed -> 201", tiny.status_code == 201, tiny.text)
        tiny_seed = {p["name"]: p for p in tiny.json()["pvcs"]}
        tt = c.post("/tickets", json={"namespace": "tiny-ok", "retain_pvcs": ["ra", "rb"]})
        check("tiny-ok 开单 -> 201", tt.status_code == 201, tt.text)
        ttid = tt.json()["ticket"]["id"]
        ok = c.post(f"/tickets/{ttid}/advance").json()
        check("有解场景 DONE", ok["ticket"]["state"] == "DONE", ok["ticket"].get("blocked_reason"))
        check("实际上报峰值恰好 32768，不再漏算 Retain PV",
              ok["storage"]["peak_bytes"] == 32768
              and ok["storage"]["actual_peak_bytes"] == 32768,
              str(ok["storage"]))
        check("完成时原件 16384 + 副本 16384 = 32768",
              ok["storage"]["original_pv_bytes"] == 16384
              and ok["storage"]["retained_copy_bytes"] == 16384
              and ok["storage"]["current_bytes"] == 32768,
              str(ok["storage"]))
        op_order = [(o["target_name"], o["kind"]) for o in ok["ops"]]
        check("清退顺序：ra 留存后删 PVC，确需腾位时提前删 dc，再处理 rb",
              op_order == [("ra", "pvc"), ("dc", "pvc"), ("rb", "pvc"),
                           ("tiny-ok", "namespace")],
              str(op_order))
        freed = {o["target_name"]: o.get("verified_freed_bytes") for o in ok["ops"]}
        check("腾位字节核对：Retain 为 0，Delete 未选卷为 8192",
              freed.get("ra") == 0 and freed.get("rb") == 0 and freed.get("dc") == 8192,
              str(freed))
        for name in ("ra", "rb"):
            body = c.get(f"/tickets/{ttid}/retained/{name}").content
            check(f"tiny-ok {name} 副本可下载且哈希一致",
                  sha(body) == tiny_seed[name]["sha256"])

        print("\n== 11. 三卷 8 KiB 全 Retain：无解，先报最少缺口且不删卷 ==")
        no = c.post("/sim/seed", json={
            "namespace": "tiny-no",
            "pvcs": [
                {"name": "ra", "size_bytes": 8192, "reclaim_policy": "Retain"},
                {"name": "rb", "size_bytes": 8192, "reclaim_policy": "Retain"},
                {"name": "rc", "size_bytes": 8192, "reclaim_policy": "Retain"},
            ],
        })
        check("tiny-no seed -> 201", no.status_code == 201, no.text)
        tn = c.post("/tickets", json={"namespace": "tiny-no", "retain_pvcs": ["ra", "rb"]})
        tnid = tn.json()["ticket"]["id"]
        audit_before_no = len(c.get("/sim/audit").json()["entries"])
        bad = c.post(f"/tickets/{tnid}/advance").json()
        audit_after_no = len(c.get("/sim/audit").json()["entries"])
        check("无解时仍停留 VALIDATING 并报告最少还缺 8192 字节",
              bad["ticket"]["state"] == "VALIDATING"
              and "还缺 8192 字节" in (bad["ticket"]["blocked_reason"] or "")
              and "本次推进不删除任何卷" in (bad["ticket"]["blocked_reason"] or ""),
              bad["ticket"].get("blocked_reason"))
        check("capacity_plan 给出 shortfall/minimum_missing=8192",
              bad["capacity_plan"]["feasible"] is False
              and bad["capacity_plan"]["shortfall_bytes"] == 8192
              and bad["capacity_plan"]["minimum_missing_bytes"] == 8192,
              str(bad.get("capacity_plan")))
        blocker_names = [b.get("target_name") for b in bad["blocking_volumes"]]
        check("阻塞卷指出不能腾位的 rc",
              "rc" in blocker_names
              and any(b.get("target_name") == "rc"
                      and b.get("reclaim_policy") == "Retain"
                      for b in bad["blocking_volumes"]),
              str(bad.get("blocking_volumes")))
        check("无解推进没有发送任何删除/审计记录",
              bad["ops"] == [] and audit_after_no == audit_before_no,
              f"ops={bad['ops']} audit_delta={audit_after_no - audit_before_no}")
        cluster_no = c.get("/sim/cluster").json()
        check("无解推进后三个 PVC 均仍在",
              {p["name"] for p in cluster_no["pvcs"] if p["ns_name"] == "tiny-no"}
              == {"ra", "rb", "rc"})

        print("\n== 12. 三卷 8 KiB：首卷与腾位删除丢回包，重启核实且不重发 ==")
        drop = c.post("/sim/seed", json={
            "namespace": "tiny-drop",
            "pvcs": [
                {"name": "ra", "size_bytes": 8192, "reclaim_policy": "Retain"},
                {"name": "rb", "size_bytes": 8192, "reclaim_policy": "Retain"},
                {"name": "dc", "size_bytes": 8192, "reclaim_policy": "Delete"},
            ],
        })
        check("tiny-drop seed -> 201", drop.status_code == 201, drop.text)
        drop_seed = {p["name"]: p for p in drop.json()["pvcs"]}
        td = c.post("/tickets", json={"namespace": "tiny-drop", "retain_pvcs": ["ra", "rb"]})
        tdid = td.json()["ticket"]["id"]
        c.post("/sim/fault", json={"drop_delete_responses": 1})
        d1 = c.post(f"/tickets/{tdid}/advance").json()
        check("首个 Retain 选中卷删除回包丢失并阻塞",
              any(o["target_name"] == "ra" and o["state"] == "unknown" for o in d1["ops"])
              and d1["ticket"]["blocked_reason"],
              str(d1["ops"]))
        body_while_blocked = c.get(f"/tickets/{tdid}/retained/ra").content
        check("腾位/核对受阻时已核验 ra 副本仍可按单下载",
              sha(body_while_blocked) == drop_seed["ra"]["sha256"])

        stop_server(server)
        server = start_server(data_dir)
        d2 = c.get(f"/tickets/{tdid}").json()
        op_ra = [o for o in d2["ops"] if o["target_name"] == "ra"][0]
        check("重启核实 ra 删除已生效，Retain 原件未腾出字节，且不重发",
              op_ra["state"] == "confirmed"
              and op_ra["verified_freed_bytes"] == 0
              and "NOT resent" in (op_ra["last_error"] or "")
              and "实际腾出 0 字节" in (op_ra["last_error"] or ""),
              str(op_ra))
        audit = c.get("/sim/audit").json()["entries"]
        check("ra 删除审计仅一条",
              len([e for e in audit if e.get("action") == "delete_pvc"
                   and e.get("target_uid") == op_ra["target_uid"]]) == 1)

        c.post("/sim/fault", json={"drop_delete_responses": 1})
        d3 = c.post(f"/tickets/{tdid}/advance").json()
        op_dc = [o for o in d3["ops"] if o["target_name"] == "dc"]
        check("提前回收 dc 腾位时回包丢失并指出阻塞卷",
              bool(op_dc) and op_dc[0]["state"] == "unknown"
              and any(b.get("target_name") == "dc"
                      and b.get("code") == "delete_result_unknown"
                      for b in d3["blocking_volumes"]),
              f"ops={d3['ops']} blockers={d3.get('blocking_volumes')}")
        check("dc 丢包后 ra 已核验副本仍可下载",
              sha(c.get(f"/tickets/{tdid}/retained/ra").content)
              == drop_seed["ra"]["sha256"])

        stop_server(server)
        server = start_server(data_dir)
        d4 = c.get(f"/tickets/{tdid}").json()
        op_dc_after = [o for o in d4["ops"] if o["target_name"] == "dc"][0]
        check("再次重启核实 dc 已实际腾出 8192 字节且不重发",
              op_dc_after["state"] == "confirmed"
              and op_dc_after["verified_freed_bytes"] == 8192
              and "NOT resent" in (op_dc_after["last_error"] or "")
              and "实际腾出 8192 字节" in (op_dc_after["last_error"] or ""),
              str(op_dc_after))
        d5 = c.post(f"/tickets/{tdid}/advance").json()
        check("丢包续办后 DONE 且峰值为 32768",
              d5["ticket"]["state"] == "DONE"
              and d5["storage"]["peak_bytes"] == 32768,
              f"{d5['ticket'].get('blocked_reason')} storage={d5['storage']}")
        for name in ("ra", "rb"):
            body = c.get(f"/tickets/{tdid}/retained/{name}").content
            check(f"tiny-drop {name} 最终下载哈希一致",
                  sha(body) == drop_seed[name]["sha256"])
        audit = c.get("/sim/audit").json()["entries"]
        ops_by_name = {o["target_name"]: o for o in d5["ops"] if o["kind"] == "pvc"}
        for name in ("ra", "dc", "rb"):
            target_uid = ops_by_name[name]["target_uid"]
            count = len([e for e in audit if e.get("action") == "delete_pvc"
                         and e.get("target_uid") == target_uid])
            check(f"tiny-drop {name} 已生效删除全程仅发送一次", count == 1, f"count={count}")
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
