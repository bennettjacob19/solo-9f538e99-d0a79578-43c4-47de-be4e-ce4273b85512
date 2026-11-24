# K8s 测试命名空间清退留存演练（ns-retain-drill）

值班员清退测试命名空间时，**指定 PVC 的数据要留下**，其余卷随环境回收。本服务用
FastAPI + 文件模拟集群 + SQLite 进度，完整演练这一流程：开单 → 逐卷留存并独立读回
校验 → 按 PV 回收策略删除 → 删除命名空间 → 凭单号读回数据。支持删除丢包注入、
重启核对续办、按开单 UID 清理（同名重建不受影响）。

## 快速开始

### Docker Compose（推荐）

```bash
docker compose up --build
# 打开 http://localhost:8000/docs
```

数据（模拟集群、留存副本、SQLite）持久化在命名卷 `drill-data` 中，容器重启不丢进度。

### 本地运行

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
DATA_DIR=./data .venv/bin/uvicorn app.main:app --port 8000
```

### 自测（44 项断言，含丢包后续办与重启核对）

```bash
.venv/bin/pip install httpx
.venv/bin/python tests/selftest.py
```

## 演练流程（全部可在 /docs 操作）

1. `POST /sim/seed` 造一个命名空间和若干已停写绑定卷（文件保存，可指定 PV 回收策略）。
2. `POST /tickets` 开单：`{"namespace": "demo", "retain_pvcs": ["data-a"]}`（至少一个）。
   开单时快照命名空间 UID、全部 PVC 的 UID/大小/sha256，记录基线字节数。
3. 可选：`POST /sim/fault` `{"drop_delete_responses": 1}` 注入删除丢包
   （服务端生效但回包丢失）。
4. `POST /tickets/{id}/advance` 推进清理；`GET /tickets/{id}` 查看进度、阻塞原因、
   存储水位（baseline / cap / current / peak）。
5. 丢包后工单阻塞并注明“结果未明”；再次 advance 或**重启服务**即自动核对：
   目标已消失则标记确认（**不重发**），仍在才重发。
6. `DONE` 后 `GET /tickets/{id}/retained/{pvc}` 凭单号读回所选数据。

## 需求映射

| 要求 | 实现 |
| --- | --- |
| /docs 指定命名空间 + ≥1 个保留 PVC | `POST /tickets`，`retain_pvcs` `min_length=1` |
| 推进清理、查看进度与阻塞原因 | `POST /tickets/{id}/advance`；`GET /tickets/{id}` 返回阶段、`blocked_reason`、逐步 ops |
| 文件保存已停写绑定卷 | `data/cluster/pvs/<pv>/data.bin` + `meta.json`（`stopped_writing: true`） |
| 按 PV 回收策略处理 PVC 删除 | Delete 删数据 / Recycle 清空转 Available / Retain 保留转 Released（`simcluster._remove_pvc_locked`） |
| 模拟删除生效后丢回包 | 故障注入计数器：删除落盘后抛 `DroppedResponseError`，操作记为 `unknown` |
| 独立读回确认后才删源 PVC | 留存副本落在集群目录之外，sha256 与**开单快照**比对通过（`verified`）后才发删除 |
| 合计字节 ≤ 开单基线 + 最大选中卷 | 逐卷“拷贝→验证→删源”，拷贝前检查 `current+size ≤ cap`，`peak_bytes` 全程记录 |
| 原命名空间与未选卷移除后才完成 | `DONE` 门槛：命名空间不存在且该 UID 下无任何 PVC；Retain 残留的 Released PV 在 `leftover_pvs` 中明示 |
| 凭单号读回所选数据 | `GET /tickets/{id}/retained[/{pvc}]`，DONE 后仍可下载 |
| 进度入 SQLite，重启核对未明删除 | `tickets/pvcs/ops` 三表；启动与每次推进前 `reconcile()`：GET 核对，消失即确认、仍在才重发 |
| 已确认的删除不再发送 | ops 幂等键 `ticket:kind:uid`，`confirmed` 直接短路；审计日志可证每目标仅一条删除 |
| 按开单 UID 清理，同名重建免疫 | 所有删除/查询按开单快照 UID 寻址；重建命名空间 UID 不同，旧单触碰不到 |

## 目录结构

```
app/
  main.py        FastAPI 路由（演练面 + 模拟端管理面），启动时核对未明删除
  engine.py      清理状态机：VALIDATING → RETAINING → DELETING_NAMESPACE → DONE/FAILED
  simcluster.py  文件模拟集群：命名空间/PVC/PV、回收策略、丢包注入、审计日志
  db.py          SQLite（WAL）
  models.py      请求模型
tests/selftest.py  端到端自测（起停真实服务进程，含重启核对）
Dockerfile / docker-compose.yml
```

## 验证

`tests/selftest.py` 共 44 项断言，全部通过，要点：

- 注入 1 次丢包 → 删除生效、回包丢失、工单阻塞且原因可读；
- **重启服务** → 启动核对把未明删除标记确认且**不重发**（审计日志每目标仅 1 条删除）；
- 续办推进至 DONE：原命名空间与未选卷移除，Retain 的 PV 保留为 Released；
- 存储峰值**恰好等于** 基线+最大选中卷（逐卷拷贝、验后即删的边界）；
- 凭单号读回 data-a/data-b，sha256 与开单快照一致；
- 同名重建命名空间 UID 不同，旧单重复推进不影响新空间；新空间可独立开单，
  并再次演练 PVC 删除与命名空间删除两次丢包的核对续办。

## 未完成项 / 限制

- 单进程同步推进；并发推进同一工单靠进程内锁，多副本部署需外部锁/队列。
- 丢包注入计数器在内存中，重启清零（业务进度在 SQLite，不受影响）。
- Recycle 策略以“截零”模拟 scrub；未模拟 PV 动态供给、finalizer 卡顿等更多 K8s 细节。
- 拷贝为整文件复制（单卷 ≤64MB 限制），未做分块流式与断点续拷。
- 无认证鉴权；`/sim/*` 是演练管理面，真实环境需隔离。
- 本沙箱无 Docker，镜像未实际构建；Dockerfile/compose 已经静态核对，本地自测以
  uvicorn 直跑验证。
