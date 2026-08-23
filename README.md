# eba-go

Go 实现：单一库。三原语是 Envelope（承载）、Bus（投递）、Actor（收信）。Job / JobHost / Clock 只组合这三件，不是第四原语。

**逻辑并发单位是 Job**：一份 root 信封的完整编排生命周期。同一 `JobHost` 上可同时有多份 Active Job。`Handle` / `Poll` 是宿主泵（Inbox 一读者），不为每个 Job 起 goroutine。看门狗只扫 Active 的 inflight。`QueueLimit` 只限尚未开工队列。OS 调度不是本库原语。

```
eba-go/
  go.mod
  src/           package eba 门面 + 各模块包
    envelope/    信封、topic、id
    idgen/       发号器
    pattern/     topic 匹配
    inbox/       有界收件箱
    bus/         总线与订阅表
    subscriber/  收信面
    publisher/   只发面
    registry/    会合台账
    result/      会合体编码
    reply/       应答（Matchmaker）
    clock/       看门狗供时
    job/         Job 两句
    jobhost/     宿主泵
  tests/unit/
  tests/integration/
  example/task_system/
```

- **Bus**：`Subscribe` / `Unsubscribe` / `Publish`。未命中静默；满箱整次 `MailboxFull` 回滚。只发不收用 `Publisher`（无 Inbox，不得订）。
- **收信**：`JobHost.Handle`。每步 `dispatch` → 看门狗 → flush。会合结果立即认答。开泵时空箱则抽空本 Inbox；积压不插队。禁止重入。
- **会合**：只走 `Registry.StartRequest` → 四元组 `ResolveOnly` → `FinishSafe`。发号只经借入的 `IdGen`。
- **Job 两句**：同步 `Begin → Reply → Finish`；多步 `Request`。外部等待：请求方 `Request`，应答方 `Matchmaker` 扣住信封再 `Reply`。Clock 只扫 inflight。
- **槽位**：根按 cause（= id）；叶子按子请求 id。同一 Host 可多份 Active。
- **错误**：违约用 panic（装配、状态机）；运营失败返回 error（满箱、背压、非法 topic）。

原则见本库 [`PRINCIPLES.md`](PRINCIPLES.md)。

## 设计原则

| 原则 | 落地 |
|---|---|
| 设计简约普适 | 三原语 Envelope / Bus / Actor；Job 只组合 |
| 实现丰富完善 | 会合、看门狗、背压、延迟应答（Matchmaker）、单泵多 Job |
| 最小实现下界 | `StartRequest` → `ResolveOnly` → `FinishSafe` |
| 软约束 | Inbox 一读者与 `**` 不硬拒；续抽以箱空为界 |
| 接口克制 | `package eba` 门面；内部包不另开第二套会合 |
| 契约丰富 | [`PRINCIPLES.md`](PRINCIPLES.md) 与 README 同一套泵故事 |
| 克制导入导出 | 门面类型别名只做跨包可达，不叠业务名 |
| 克制暴露 | `jobhost` 泵细节与开工队列不进稳定叙事 |
| 克制大文件 | 按模块拆分 |
| 规避死代码 | 结果不进开工队列 |
| 规避内部冲突 | 会合不与开工队列混排 |
| 全局路线唯一 | 认答只经 `ResolveOnly` |

语言差不是第二套会合。`package eba` 类型别名只为跨包可达，不是第二条认答路线。封面可宽订；嵌入按 topic 列表窄订，泵在宿主只调 Handle。本轮不做：取消传播、一流多结果、Header 加 reply-to、库级泵、每请求超时参数。

```bash
go test ./...
go run ./example/task_system
```

Go >= 1.22，无运行时依赖。
