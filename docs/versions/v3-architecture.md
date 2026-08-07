# botmux-go V3 技术方案：Session ↔ Worker 彻底解耦与自动自愈

> **版本**: V3 (规划中)  
> **日期**: 2026-08-07  
> **前置**: V2 会话持久化与恢复（已完成）  
> **目标**: 解决 V2 的 3 个 P0 痛点 — 僵尸 Session、Worker 不可自愈、无 CLI 管理命令

---

## 一、V2 痛点回顾（精确到代码行）

### 1.1 痛点全景

| # | 痛点 | 代码位置 | 根因 |
|---|------|---------|------|
| P0-1 | **僵尸 Session**：Daemon 重启后 Session 在 map 里存在但永远不能收发消息 | [daemon.go:346-364 `SendInput()`](file:///Users/bytedance/botmux-go/internal/daemon/daemon.go#L346-L364) `<-ws.Connected` 等 15s 超时 | `restoreSessions()` 只填元数据，`Connected: make(chan struct{})` 新建空 channel 永远不 close |
| P0-2 | **Worker 不可自愈**：Worker 异常退出后 Session 直接被删 | [daemon.go:283-292 `waitWorkerExit()`](file:///Users/bytedance/botmux-go/internal/daemon/daemon.go#L283-L292) 无条件调 `removeSession()` | `removeSession()` 同时清 `sessions map` + `store.MarkClosed()`，把元数据也毁了 |
| P0-3 | **无 CLI 管理命令**：查/关 Session 只能手动改 JSON | [main.go `execCommand()`](file:///Users/bytedance/botmux-go/cmd/daemon/main.go) 只支持 `new` / `send` | 协议层只有 6 种消息类型（MsgNewSession/Input/Output/Ready/Error/Heartbeat），缺 list/history/close |

### 1.2 根因的根因

```
V2 核心结构：WorkerSession = 元数据 + 运行时 混在一个 struct
                        ↑
                   这是"宽表"设计的问题 ——
                   "应该跨重启存活"的数据 +
                   "重启必然失效"的数据 耦合在一起
```

具体看 [daemon.go:19-34 `WorkerSession`](file:///Users/bytedance/botmux-go/internal/daemon/daemon.go#L19-L34)：

```go
type WorkerSession struct {
    // —— 应该跨重启存活的（持久化层）
    SessionID  string
    BotID      string
    CliType    string
    CliPath    string
    WorkingDir string
    LastOutput []string
    lastActive time.Time

    // —— 重启必然失效的（运行时层）
    Cmd        *exec.Cmd      // ← 子进程句柄，父进程挂了必丢
    Conn       net.Conn      // ← TCP 连接，进程关就断
    Connected  chan struct{} // ← 通知型 channel，重启只能新建空的
    hbSeen     time.Time
}
```

---

## 二、V3 改造总览

### 2.1 核心思路：把"宽表"拆成两张 1:1 关联的表

```
V2（宽表，大量 NULL）:
┌──────────────────────────────────────────────────┐
│ WorkerSession（一个 struct 装所有字段）           │
│ SessionID / BotID / CliType / WorkingDir          │  ← 元数据层（应跨重启）
│ LastOutput / lastActive / hbSeen                 │
│ Cmd / Conn / Connected                           │  ← 运行时层（重启必丢）
└──────────────────────────────────────────────────┘

V3（1:1 关联两张表）:
         session_id（主键）
              │
    ┌─────────┴──────────┐
    ▼                    ▼
 sessions map       workers map
 [*SessionMeta]     [*WorkerHandle]
 ┌─────────────┐   ┌─────────────┐
 │ 持久化元数据 │   │ 运行时句柄 │
 │ 跨重启存活  │   │ 重启重建   │
 │ JSON 双写   │   │ Monitor 巡检│
 └─────────────┘   └─────────────┘
```

### 2.2 新结构体设计

#### ① SessionMeta（持久化层，对应 sessions map）

```go
// internal/daemon/session_meta.go

type SessionMeta struct {
    SessionID  string           `json:"session_id"`
    BotID      string           `json:"bot_id"`
    CliType    string           `json:"cli_type"`
    CliPath    string           `json:"cli_path"`
    WorkingDir string           `json:"working_dir"`
    LastOutput []string         `json:"last_output"`
    LastActive time.Time        `json:"last_active"`
    CreatedAt  time.Time        `json:"created_at"`
    Closed     bool             `json:"closed"`
    Status     SessionStatus    `json:"status"`  // 新增：状态机
}

type SessionStatus string
const (
    StatusCreated   SessionStatus = "CREATED"    // 元数据已创建，尚无 Worker
    StatusSpawning   SessionStatus = "SPAWNING"   // Worker 正在 fork
    StatusReady     SessionStatus = "READY"      // Worker 已连接，可用
    StatusRecovering SessionStatus = "RECOVERING" // Worker 死了，Monitor 正在补
    StatusClosed    SessionStatus = "CLOSED"     // 已关闭，不再自愈
)

// 持久化转换
func (m *SessionMeta) ToPersisted() *PersistedSession { ... }
func SessionMetaFromPersisted(ps *PersistedSession) *SessionMeta { ... }
```

#### ② WorkerHandle（运行时层，对应 workers map）

```go
// internal/daemon/worker_handle.go

type WorkerHandle struct {
    SessionID string
    Cmd       *exec.Cmd
    Conn      net.Conn
    Ready     chan struct{}     // 替代原 Connected，语义更准确
    Pid       int
    hbSeen    time.Time
    isReady   bool
    mu        sync.Mutex
}

func (h *WorkerHandle) IsAlive() bool {
    return h.Conn != nil && time.Since(h.hbSeen) < 30*time.Second
}

func (h *WorkerHandle) Send(msg *protocol.Message) error {
    if h.Conn == nil { return errors.New("no connection") }
    _, err := msg.WriteTo(h.Conn)
    return err
}

func (h *WorkerHandle) markReady() {
    h.mu.Lock()
    defer h.mu.Unlock()
    if !h.isReady {
        h.isReady = true
        close(h.Ready)  // 只 close 一次
    }
}
```

#### ③ Daemon 结构体重构

```go
// daemon.go — 改造后
type Daemon struct {
    // ... 原有字段 ...

    // V2 单 map → V3 双 map（1:1 关联）
    sessionsMu sync.RWMutex
    sessions   map[string]*SessionMeta     // ← 持久化元数据（跨重启存活）

    workersMu  sync.RWMutex
    workers    map[string]*WorkerHandle    // ← 运行时句柄（重启重建，Monitor 自愈）

    // V3 新增：Monitor 控制
    monitorMu  sync.Mutex
    monitorStop chan struct{}
}
```

### 2.3 SessionMonitor（V3 核心守护者）

```go
// internal/daemon/session_monitor.go

const (
    monitorInterval    = 1 * time.Second   // 巡检间隔
    workerAliveTimeout = 30 * time.Second  // 心跳超时
    maxSpawnRetries    = 5                 // 单次补 Worker 最大重试
)

func (d *Daemon) startSessionMonitor() {
    ticker := time.NewTicker(monitorInterval)
    d.monitorStop = make(chan struct{})

    go func() {
        for {
            select {
            case <-ticker.C:
                d.reconcileSessions()
            case <-d.monitorStop:
                ticker.Stop()
                return
            case <-d.ctx.Done():
                ticker.Stop()
                return
            }
        }
    }()
}

func (d *Daemon) reconcileSessions() {
    d.sessionsMu.RLock()
    metas := make([]*SessionMeta, 0, len(d.sessions))
    for _, m := range d.sessions {
        metas = append(metas, m)
    }
    d.sessionsMu.RUnlock()

    for _, meta := range metas {
        if meta.Closed {
            continue  // CLOSED 不再自愈
        }

        d.workersMu.RLock()
        handle, exists := d.workers[meta.SessionID]
        d.workersMu.RUnlock()

        if !exists || !handle.IsAlive() {
            d.spawnWorkerForSession(meta)
        }
    }
}
```

### 2.4 spawnWorkerForSession（Worker 自动拉起）

```go
// 新增方法，从 NewSession 中抽离 Worker fork 逻辑
func (d *Daemon) spawnWorkerForSession(meta *SessionMeta) error {
    handle := &WorkerHandle{
        SessionID: meta.SessionID,
        Ready:     make(chan struct{}),
        hbSeen:    time.Now(),
    }

    cmd := exec.CommandContext(d.ctx, d.selfExe)
    cmd.Env = append(os.Environ(),
        "BOTMUX_ROLE=worker",
        "BOTMUX_SESSION_ID="+meta.SessionID,
        "BOTMUX_DAEMON_ADDR="+d.cfg.ListenAddr,
        "BOTMUX_CLI_TYPE="+meta.CliType,
        "BOTMUX_CLI_PATH="+meta.CliPath,
        "BOTMUX_WORKING_DIR="+meta.WorkingDir,
        "BOTMUX_STORE_DIR="+d.cfg.SessionsDir,
    )
    cmd.Stdout = os.Stdout
    cmd.Stderr = os.Stderr
    handle.Cmd = cmd

    if err := cmd.Start(); err != nil {
        log.Printf("[daemon] session %s: spawn worker failed: %v", safeShort(meta.SessionID), err)
        return err
    }
    handle.Pid = cmd.Process.Pid

    d.workersMu.Lock()
    d.workers[meta.SessionID] = handle
    d.workersMu.Unlock()

    d.store.UpdateWorkerPID(meta.SessionID, handle.Pid)

    // 更新 meta 状态
    d.sessionsMu.Lock()
    meta.Status = StatusSpawning
    d.sessionsMu.Unlock()

    go d.waitWorkerExit(handle)
    go d.monitorReady(handle, meta)

    log.Printf("[daemon] session %s spawned worker (pid=%d)", safeShort(meta.SessionID), handle.Pid)
    return nil
}

// 等待 Worker 就绪，超时则标记为 RECOVERING 让下一轮 Monitor 补
func (d *Daemon) monitorReady(handle *WorkerHandle, meta *SessionMeta) {
    select {
    case <-handle.Ready:
        d.sessionsMu.Lock()
        meta.Status = StatusReady
        d.sessionsMu.Unlock()
        log.Printf("[daemon] session %s -> READY", safeShort(meta.SessionID))
    case <-time.After(10 * time.Second):
        d.sessionsMu.Lock()
        meta.Status = StatusRecovering
        d.sessionsMu.Unlock()
        log.Printf("[daemon] session %s: worker ready timeout, will retry", safeShort(meta.SessionID))
    case <-d.ctx.Done():
    }
}
```

### 2.5 V3 状态机

```
                         Monitor 巡检发现 Worker 不存在/死了
                                        │
                                        ▼
    ┌───────────┐           spawnWorkerForSession()           ┌───────────┐
    │ CREATED   │ ──────────────────────────────────────────▶ │ SPAWNING  │
    │ (元数据)  │                                              │ (fork 中) │
    └───────────┘                                              └─────┬─────┘
                                                                     │
                                                        Worker 连上 + sendReady()
                                                        close(handle.Ready)
                                                                     ▼
    ┌───────────┐                                          ┌───────────┐
    │ CLOSED    │ ◀────────────── 主动关闭 / CloseSession ── │   READY   │
    └───────────┘                                            └─────┬─────┘
                                                                     │
                                                        Worker 断连 / hb 超时
                                                        Monitor 下一轮检测到
                                                                     ▼
    ┌───────────┐                                          ┌───────────┐
    │ RECOVERING│ ◀───────────── 标记状态                    │   STALE   │
    └─────┬─────┘                                          └───────────┘
          │
          └──── spawnWorkerForSession() → SPAWNING → READY
```

### 2.6 V3 全链路对比

#### 场景 1：新建会话（V3 vs V2）

```
V2 NewSession:
    1. 创建 WorkerSession（元+运行混合）
    2. sessions map[id] = ws
    3. 持久化 JSON
    4. exec.CommandContext fork Worker
    5. ws.Connected close → 通知 OnReady

V3 NewSession:
    1. 创建 SessionMeta
    2. sessions map[id] = meta（状态 CREATED）
    3. 持久化 JSON
    4. spawnWorkerForSession(meta) — 抽离的独立方法
       4a. 创建 WorkerHandle
       4b. workers map[id] = handle
       4c. exec.CommandContext fork Worker
       4d. monitorReady() 等待就绪 → meta.Status = READY
    5. OnReady 回调触发
```

#### 场景 2：Daemon 重启恢复（V3 核心改进）

```
V2:
    1. restoreSessions() — 只建 WorkerSession（元数据填，运行时空）
    2. sessions map 有条目但 Conn/Connected 全空
    3. SendInput → <-Connected 15s 超时 → 僵尸 Session ❌

V3:
    1. restoreSessions() — 只建 SessionMeta，状态 CREATED
    2. sessions map 有 meta（持久化元数据完整）
    3. workers map 空（运行时待重建）
    4. startSessionMonitor() 启动
    5. Monitor 第一轮巡检 → 发现 meta 无对应 Worker
    6. spawnWorkerForSession(meta) — 自动 fork
    7. 5 秒内 meta.Status = READY ✅
    8. 后续 SendInput → 正常收发
```

#### 场景 3：Worker panic 被杀（V3 自愈）

```
V2:
    1. Worker PID 96411 panic
    2. waitWorkerExit() → removeSession()
    3. sessions map 删条目 + store.MarkClosed()
    4. Session 永久死亡 ❌

V3:
    1. Worker PID 96411 panic
    2. waitWorkerExit(handle) — 只做：
       2a. workers map 删 handle（**不碰 sessions map！**）
       2b. meta.Status = RECOVERING
    3. Monitor 下一轮（1s 内）发现 workers 缺失
    4. spawnWorkerForSession(meta) 补新 Worker
    5. meta.Status = READY ✅
    6. 用户无感，消息继续收发
```

---

## 三、新增 CLI 命令（PR-C 交付）

### 3.1 命令清单

```bash
# 列出所有会话（含状态）
/tmp/botmux-go -cmd list
# 预期输出：
# SESSION_ID            BOT         CLI       STATUS    PID      LAST_ACTIVE
# demo-001              bot-mock    mock      READY     12345    2026-08-07 15:30:12
# old-sess              bot-mock    mock      RECOVERING -       2026-08-07 14:41:24
# closed-sess           bot-mock    mock      CLOSED    -       2026-08-06 10:00:00

# 查询会话历史
/tmp/botmux-go -cmd history demo-001
# 预期输出：
# [1] [mock-echo] hello
# [2] [mock-echo] message after restart
# ...

# 手动关闭会话
/tmp/botmux-go -cmd close demo-001 "user request"
# 预期输出：
# [cli] session demo-001 closed
```

### 3.2 新增协议消息

```go
// protocol.go 新增
const (
    MsgListSessions    Type = "list_sessions"     // 客户端 → Daemon
    MsgListSessionsRsp Type = "list_sessions_rsp" // Daemon → 客户端
    MsgHistory         Type = "history"           // 客户端 → Daemon
    MsgHistoryRsp      Type = "history_rsp"       // Daemon → 客户端
    MsgCloseSession    Type = "close_session"     // 客户端 → Daemon
    MsgCloseSessionAck Type = "close_session_ack" // Daemon → 客户端
)
```

---

## 四、实施步骤（3 个 PR，按顺序）

### PR-A：结构体拆分 + spawnWorkerForSession 抽离

**目标**：代码可编译，基础流程正常，状态机初步就位。不启动 Monitor。

| 步骤 | 文件 | 操作 | 改动量 |
|------|------|------|--------|
| A1 | `internal/daemon/session_meta.go` | **新增** | ~60 行 |
| A2 | `internal/daemon/worker_handle.go` | **新增** | ~50 行 |
| A3 | `internal/daemon/daemon.go` | **修改** | ~200 行 |
| A4 | `internal/protocol/protocol.go` | **修改** | ~20 行（状态常量） |

**关键改动映射**：

```
V2 函数                          V3 改造
─────────────────────────────────────────────────────
WorkerSession struct             → 删除，拆成 SessionMeta + WorkerHandle
daemon.sessions map              → 改为 sessions map[string]*SessionMeta
                                  + 新增 workers map[string]*WorkerHandle
NewSession()                     → 前半段建 SessionMeta（存 sessions map）
                                  + spawnWorkerForSession()（新增，建 WorkerHandle）
restoreSessions()                → 只填 sessions map（SessionMeta）
                                  workers map 保持空，等 Monitor 拉起
waitWorkerExit(ws)               → 改为 waitWorkerExit(handle)
                                  只删 workers map，不碰 sessions map
removeSession(id)                → 改为双 map 清理
                                  workers[id] delete（仅运行时）
                                  sessions[id] delete + store.MarkClosed
SendInput(id, input)             → sessions[id] 判存在
                                  workers[id] 判就绪，分离判定
                                  <-handle.Ready（替代 <-ws.Connected）
handleWorkerConn()               → 连接成功后建 WorkerHandle
                                  workers[sessionID] = handle
                                  close(handle.Ready)（通知就绪）
```

### PR-B：SessionMonitor + 自动自愈

**目标**：Daemon 重启后 5s 内自动恢复所有会话；Worker 异常死亡自动拉起。

| 步骤 | 文件 | 操作 | 改动量 |
|------|------|------|--------|
| B1 | `internal/daemon/session_monitor.go` | **新增** | ~80 行 |
| B2 | `internal/daemon/daemon.go` | **修改** | ~40 行（Start/Stop 启动 Monitor） |
| B3 | `internal/worker/worker.go` | **修改** | ~30 行（心跳时间戳写入） |

### PR-C：CLI 增强 + 协议扩展

**目标**：`list` / `history` / `close` 三个命令全部可用。

| 步骤 | 文件 | 操作 | 改动量 |
|------|------|------|--------|
| C1 | `internal/protocol/protocol.go` | **修改** | ~30 行（新增 6 种消息类型） |
| C2 | `internal/daemon/daemon.go` | **修改** | ~80 行（handleClientConn 处理 list/history/close） |
| C3 | `cmd/daemon/main.go` | **修改** | ~100 行（execCommand 新增 3 个子命令） |

---

## 五、验收标准（6 个 Case，全通过才算 V3 完成）

### Case 1：新建会话 + 发送消息（基础链路）
```bash
/tmp/botmux-go -cmd new demo bot-mock
/tmp/botmux-go -cmd send demo "hello"
```
✅ 输出：`<< [mock-echo] hello`

### Case 2：list 命令展示状态
```bash
/tmp/botmux-go -cmd list
```
✅ 输出表格：demo 显示 READY，PID 非空，last_output 1 条

### Case 3：history 命令展示历史
```bash
/tmp/botmux-go -cmd history demo
```
✅ 输出：`[1] [mock-echo] hello`

### Case 4：Worker 异常死亡自愈
```bash
# 找 Worker PID 并杀
kill $(pgrep -P $(pgrep -o -f botmux-go))
sleep 3
/tmp/botmux-go -cmd list
```
✅ demo 从 RECOVERING 变回 READY，PID 变成新值
```bash
/tmp/botmux-go -cmd send demo "after recover"
```
✅ 输出：`<< [mock-echo] after recover`

### Case 5：Daemon 重启恢复（最关键）
```bash
pkill -f botmux-go
sleep 2
/tmp/botmux-go -config ./configs/bots.json &
sleep 5
/tmp/botmux-go -cmd list
```
✅ demo 存在，状态 READY，LastOutput 还在
```bash
/tmp/botmux-go -cmd send demo "after daemon restart"
```
✅ 输出：`<< [mock-echo] after daemon restart`

### Case 6：close 命令真正关闭
```bash
/tmp/botmux-go -cmd close demo
/tmp/botmux-go -cmd list
```
✅ demo 标记 CLOSED，不再被 Monitor 拉起
```bash
/tmp/botmux-go -cmd send demo "x"
```
✅ 输出：`session demo is closed`（明确报错）

---

## 六、文件变更清单汇总

| 文件 | 操作 | 行数 | PR |
|------|------|------|-----|
| `internal/daemon/session_meta.go` | **新增** | ~60 | A |
| `internal/daemon/worker_handle.go` | **新增** | ~50 | A |
| `internal/daemon/session_monitor.go` | **新增** | ~80 | B |
| `internal/daemon/daemon.go` | **修改** | ~320（拆 map + spawnWorker + Monitor + close）| A+B |
| `internal/protocol/protocol.go` | **修改** | ~50（6 种消息 + 状态常量）| C |
| `internal/worker/worker.go` | **修改** | ~30（心跳 + Ready 通知）| B |
| `cmd/daemon/main.go` | **修改** | ~100（list/history/close 子命令）| C |
| **合计** | | **~690 行** | |

---

## 七、风险与对策

| 风险 | 说明 | 对策 |
|------|------|------|
| **死锁** | Monitor 和业务协程同时操作 sessions/workers map | 严格锁顺序：先 workersMu 再 sessionsMu；用 RLock 替代 Lock 读多写少 |
| **Worker 雪崩** | Monitor 同时补多个 Worker 导致资源耗尽 | 加 `maxConcurrentSpawns` 信号量（限 5 个并发 spawn） |
| **spawn 风暴** | Worker 反复启动失败 → Monitor 无限重试 | 每个 session 加 `spawnFailures` 计数，超过 5 次降级为报警日志 |
| **协议兼容** | 新消息类型 vs V2 老客户端 | 服务端忽略不识别的 MsgType（已有的 default 分支处理） |
| **重启风暴** | Daemon 刚起来 Monitor 立刻触发大量 spawn | Monitor 启动后等 3s 再开始巡检（给 Daemon 就绪时间） |

---

## 八、V3 → V4 演进方向

V3 完成后，V4 可以在此基础上平滑推进：

| 方向 | 说明 | V3 铺垫 |
|------|------|---------|
| **HTTP Dashboard** | `GET /api/sessions` 展示所有会话状态 | sessions + workers 双 map 天然适合暴露 REST |
| **多 Bot 路由** | 不同 Bot 类型路由到不同 Adapter | SessionMeta.BotID / CliType 已支持 |
| **真实 Agent 接入** | CodexAdapter / BashAdapter | WorkerHandle 已抽象，只需加 Adapter |
| **会话分组** | 按 BotID / 标签分组 | sessions map 可按 BotID 索引扩展 |
| **监控指标** | Prometheus / StatsD | Monitor 可暴露指标（spawn 次数、恢复耗时等） |

---

## 附录：V3 与 OpenClaw 架构对照

| 维度 | OpenClaw | botmux-go V3 | 对应关系 |
|------|----------|-------------|---------|
| 网关层 | Gateway（WebSockets） | Daemon（TCP Server） | 都是中枢 |
| Agent 层 | Pi Agent（嵌入式） | Worker（子进程） | 都是执行单元 |
| 持久化 | JSON 文件 | SessionStore（JSON） | 一致 |
| 自愈 | 心跳 + 重启 | SessionMonitor + spawnWorker | 实现策略不同但目标一致 |
| 通道 | 25+ Channel 适配器 | BotID 路由 + Adapter Factory | 可对标扩展 |

---

**状态**：📋 技术方案已就绪，等待确认后启动 PR-A 实现。
