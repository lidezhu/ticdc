# Event Broker 扫描流程整理说明

## 背景与问题根因

`eventBroker` 原先将扫描范围计算、握手/ready 状态、scan window 和
sync-point 栅栏、内存配额、扫描执行以及扫描结果发送全部放在
`event_broker.go` 的连续长路径中。它让下面几个独立的状态约束互相隐含：

- Dispatcher 是否可扫描由 epoch、移除状态、握手状态和单任务约束共同决定；
- 扫描区间必须同时受 event-store resolved-ts、schema resolved-ts、DDL、scan
  window 与 sync point 约束；
- Changefeed 配额是一次扫描的预留，未实际生成事件的部分必须归还；
- Dispatcher 配额是下游当前可承受容量，拒绝扫描时不应改变共享的
  changefeed 配额。

最直接的正确性问题是：旧路径先从 changefeed 配额中预留，再检查
dispatcher 配额；后者不足时直接返回，预留直到下一次拥塞控制消息才会恢复。
持续触发时会虚耗共享配额，并让无关 dispatcher 的扫描被饿死。

另有两个使行为难以理解的细节：扫描限额更新后仍返回旧限额，以及有界入队成功
时创建的 timer 没有停止。

## 新的职责边界

扫描逻辑迁移到 `pkg/eventservice/event_broker_scan.go`，按以下顺序组织：

```text
event-store notification / reset
          |
          v
scanReady
  ├─ ready / handshake
  └─ getScanTaskDataRange
          |
          v
scan worker: doScan
  ├─ recheck range and rate limit
  ├─ admitScan (downstream quota -> changefeed reservation)
  ├─ eventScanner.scan
  ├─ releaseUnused
  └─ sendScannedEvents (DML / DDL / resolved-ts)
```

`event_broker.go` 现在保留 broker 的构造、worker 生命周期、消息批发送、
dispatcher 生命周期和心跳处理；扫描状态机不再散落在该文件中。

## 关键不变量

1. 同一个 dispatcher 同时最多一个扫描任务：`isTaskScanning` 仍由
   `pushTask` 和 `doScan` 的 defer 成对维护。
2. 入队前检查只用于避免无效任务；worker 执行前必须再次计算扫描范围，避免队列
   等待期间状态推进导致的过期决定。
3. 扫描范围不能跨越 schema、scan window、DDL 或 sync point 栅栏。全局 scan
   window 被落后 dispatcher 钉住时，带栅栏的 dispatcher 仍可使用受限本地步长推进。
4. `scanAdmission` 唯一拥有一次 changefeed 配额预留。扫描完成后，负值/零字节
   结果归还全部预留；小于预留的结果只归还差额；大事务可保留超过预留的实际占用。
   扫描报错时不会入队任何返回事件，因此归还全部预留。
5. dispatcher 容量先于共享 changefeed 配额检查，因此一个被拒绝的 dispatcher
   不会影响同 changefeed 的其他 dispatcher。

## 行为变化

- 修复 dispatcher 配额不足时的 changefeed 配额泄漏。
- 动态扫描限额到达更新周期后立即对本次扫描生效，而非延后一轮。
- 成功的有界入队会停止其超时 timer。
- 未改变 wire protocol、事件排序、DDL/sync point 的发送顺序或对外配置。

## 覆盖测试

新增单元测试覆盖：

- dispatcher 配额拒绝不会预留 changefeed 配额；
- 扫描结束只归还未使用的预留配额；
- 扫描失败归还全部预留配额；
- 扫描限额更新后返回新值。

现有 event broker、scanner、scan-window 与 dispatcher 生命周期测试继续覆盖范围
裁剪、reset、握手和事件顺序。
