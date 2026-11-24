"""K8s 测试命名空间清退留存演练服务。

演练面（/docs 可操作）：
  POST /tickets                       开单：指定待删除命名空间 + 至少一个需保留的 PVC
  GET  /tickets                       工单列表
  GET  /tickets/{tid}                 进度、阻塞原因、存储水位
  POST /tickets/{tid}/advance         推进清理（自动核对未明删除后继续）
  GET  /tickets/{tid}/retained        留存副本清单
  GET  /tickets/{tid}/retained/{pvc}  凭单号读回所选数据

模拟端管理面：
  POST /sim/seed    造一个命名空间和若干已停写绑定卷
  GET  /sim/cluster 查看模拟集群现状
  POST /sim/fault   设置接下来 N 次删除“生效但丢回包”
  GET  /sim/fault   查看故障注入配置
  GET  /sim/audit   模拟端审计日志（每次删除一行）
  POST /sim/reset   清空全部演练数据
"""
from __future__ import annotations

import os
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, RedirectResponse

from .engine import Engine
from .models import FaultRequest, SeedRequest, TicketRequest
from .simcluster import ConflictError, NotFoundError

engine: Engine


@asynccontextmanager
async def lifespan(app: FastAPI):
    global engine
    engine = Engine(os.environ.get("DATA_DIR", "./data"))
    resolved = engine.reconcile()  # 重启后核对结果未明/中断的删除
    if resolved:
        print(f"[startup] reconciled {resolved} unknown/interrupted delete op(s)")
    yield


app = FastAPI(
    title="K8s 命名空间清退留存演练",
    version="1.0.0",
    description="清退测试命名空间时保留指定 PVC 数据：先独立读回校验留存副本，再按 PV 回收策略删除源卷；"
                "支持删除丢包注入、SQLite 进度持久化、重启核对续办、按开单 UID 清理。",
    lifespan=lifespan,
)


def _guard(fn, *args, **kwargs):
    try:
        return fn(*args, **kwargs)
    except NotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ConflictError as e:
        raise HTTPException(status_code=409, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse("/docs")


# ---------------- 演练面 ----------------
@app.post("/tickets", status_code=201, tags=["tickets"],
          summary="开单：指定待删除命名空间和至少一个需保留的 PVC")
def create_ticket(req: TicketRequest):
    return _guard(engine.create_ticket, req.namespace, req.retain_pvcs, req.note)


@app.get("/tickets", tags=["tickets"], summary="工单列表")
def list_tickets():
    return {"tickets": engine.list_tickets()}


@app.get("/tickets/{tid}", tags=["tickets"], summary="查看进度、阻塞原因与存储水位")
def get_ticket(tid: str):
    return _guard(engine.status, tid)


@app.post("/tickets/{tid}/advance", tags=["tickets"],
          summary="推进清理（先核对未明删除，再续办后续步骤）")
def advance_ticket(tid: str):
    return _guard(engine.advance, tid)


@app.get("/tickets/{tid}/retained", tags=["tickets"], summary="留存副本清单")
def list_retained(tid: str):
    return _guard(lambda: {"ticket_id": tid, "items": engine.retained_items(tid)})


@app.get("/tickets/{tid}/retained/{pvc_name}", tags=["tickets"],
         summary="凭单号读回所选 PVC 的数据")
def download_retained(tid: str, pvc_name: str):
    path = _guard(engine.retained_file, tid, pvc_name)
    return FileResponse(path, filename=f"{tid}-{pvc_name}.bin", media_type="application/octet-stream")


# ---------------- 模拟端管理面 ----------------
@app.post("/sim/seed", status_code=201, tags=["sim"], summary="造一个命名空间和若干已停写绑定卷")
def sim_seed(req: SeedRequest):
    return _guard(engine.sim.seed_namespace, req.namespace,
                  [p.model_dump() for p in req.pvcs])


@app.get("/sim/cluster", tags=["sim"], summary="查看模拟集群现状")
def sim_cluster():
    return engine.sim.cluster_dump()


@app.post("/sim/fault", tags=["sim"], summary="设置接下来 N 次删除生效但丢回包")
def sim_set_fault(req: FaultRequest):
    n = engine.sim.set_drop_delete_responses(req.drop_delete_responses)
    return {"drop_delete_responses": n}


@app.get("/sim/fault", tags=["sim"], summary="查看故障注入配置")
def sim_get_fault():
    return engine.sim.get_fault()


@app.get("/sim/audit", tags=["sim"], summary="模拟端审计日志")
def sim_audit():
    return {"entries": engine.sim.audit_log()}


@app.post("/sim/reset", tags=["sim"], summary="清空全部演练数据（集群、留存、工单）")
def sim_reset():
    engine.reset()
    return {"reset": True}
