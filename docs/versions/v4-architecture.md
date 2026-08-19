# botmux-go V4 技术设计文档 — HTTP Dashboard + REST API

> **版本**: V4（已发布）
> **日期**: 2026-08-17
> **主题**: 内嵌 HTTP Server + 8 个 REST API 端点 + go:embed Dashboard 单页
> **V3 文档**: [v3-architecture.md](v3-architecture.md)
> **代码仓库**: https://github.com/kon0di0da/botmux-go

---

## 一、V4 目标与边界

### 1.1 为什么做 V4

TS 原版 `src/dashboard` 在 2026 年 4-6 月内产生了 **357 次 commit**，是整个项目 TOP 4 大模块，仅次于 `src/core` / `src/adapters` / `src/im`。官方把 Dashboard 当成产品主入口来建设——从简单的会话列表页逐步扩展成 10+ 页面的管理后台。

Go 版 V1-V3 已完成核心调度（Daemon + Worker + Monitor 自愈 + CLI 管理命令），但所有交互只能通过 CLI 命令行。V4 把 CLI 的能力搬成 HTTP API + 可视化面板：
- 日常使用不必开终端敲命令，浏览器打开即可
- 未来飞书 Channel、Shell 脚本、第三方工具都能直接调 REST API
- 提供单二进制自包含部署（go:embed 把 HTML 编进二进制）

### 1.2 对标官方迭代阶段

| 阶段 | 时间 | commit 数 | 核心交付 | V4 对齐 |
|------|------|----------|---------|---------|
| **Stage 1：基础版** | 2026-04-30 | 5 次（1 天）| 会话列表页 + 群组页 + 定时任务页 + Aggregator + Registry + Auth | ✅ 对齐会话列表页（简化），跳过 SSE/Auth/多 Daemon |
| **Stage 2：扩展版** | 2026-05 中 | 10+ 次 | 总览页 + 工作流页 + Bot 接入向导 + 团队联邦 + Webhook | ❌ 不做，等有真实业务场景 |
| **Stage 3：高级版** | 2026-06 | 100+ 次 | i18n/7 套主题/HMAC 门控/文件沙盒/Session Discovery | ✅ 对齐 Session Discovery（查询参数过滤排序分页）|

### 1.3 为什么不用 SSE / 事件驱动 / 多 Daemon（YAGNI 原则）

官方 TS 版用 SSE + EventEmitter + 多 Daemon 聚合架构，我们选择**短轮询 + 单 Daemon**，理由：

1. **单 Daemon 场景**：botmux-go 是本地开发工具，1 个 daemon 进程足够，不需要聚合多个远端 daemon
2. **实时性要求不高**：对话消息 2 秒延迟可接受，SSE 长连接增加复杂度（断线重连、心跳、背压）
3. **代码简单**：短轮询 = `setInterval(fetch, 2000)`，20 行代码搞定；SSE 需要 EventSource + 服务端 flusher + 连接管理
4. **易调试**：curl 直接打 API 就能看到结果，不用构造 SSE 客户端
5. **增量演进**：未来需要实时推送可以无缝升级到 SSE，不影响现有 API 契约

### 1.4 目标

1. 给 Daemon 加**内嵌 HTTP 层**（`net/http` 标准库，0 第三方依赖），默认 `127.0.0.1:17891`，与 TCP CLI `127.0.0.1:17890` 双端口并存
2. 提供 **8 个 REST API 端点**：healthz / bots / sessions CRUD / send / history download
3. 内嵌**纯静态 Dashboard 单页**（HTML + 原生 JS fetch，0 npm 构建，0 框架），支持会话列表轮询、对话详情、发送消息、关闭会话、下载历史
4. 对齐 Session Discovery：`GET /api/sessions` 支持 `?bot_id=&status=&sort=&limit=&offset=` 查询参数
5. **单二进制部署**：`go build` 出的可执行文件自包含 HTML，部署时不需要拷贝静态资源

### 1.5 非目标（留给 V5+）

- 不做 Auth / HMAC / Token（默认 127.0.0.1 本机访问）
- 不做多 Daemon 聚合 / SSE 实时推送 / Registry 服务发现
- 不做 i18n / 主题切换 / tmux 终端卡片 / 文件上传
- 不做工作流 / Bot 接入向导 / 角色权限 / Webhook

---

## 二、架构设计

### 2.1 双端口并存架构图

```
┌─────────────────────────────────────────────────────────────────┐
│                     Daemon 进程 (PID XXXXX)                      │
│                                                                  │
│  ┌─────────────────┐     ┌─────────────────┐                     │
│  │  TCP Server      │     │  HTTP Server     │                     │
│  │  127.0.0.1:17890 │     │  127.0.0.1:17891 │                     │
│  │  V1-V3 CLI 协议  │     │  V4 新增          │                     │
│  │  JSON lines      │     │  net/http ServeMux│                     │
│  │  8+6 MsgType     │     │  8 REST endpoints │                     │
│  └────────┬────────┘     └────────┬────────┘                     │
│           │                       │                               │
│           │    ┌──────────────────┘                               │
│           ▼    ▼                                                  │
│  ┌──────────────────────────────────────────────────────┐        │
│  │  daemon.go 公共方法层（业务逻辑唯一入口）                │        │
│  │  NewSession() / Sessions() / GetSessionMeta()          │        │
│  │  SendInput() / CloseSession() / FindBot()              │        │
│  │  PurgeSession() / spawnWorkerForSession()              │        │
│  └──────┬──────────────┬──────────────┬─────────────────┘        │
│         │              │              │                           │
│         ▼              ▼              ▼                           │
│  ┌────────────┐ ┌────────────┐ ┌────────────────────┐             │
│  │ sessions   │ │ workers    │ │ SessionStore       │             │
│  │ map[sid]*  │ │ map[sid]*  │ │ (~/.botmux-go/     │             │
│  │ SessionMeta│ │ WorkerHandle││  sessions/*.json)  │             │
│  │  (含环形    │ │  (含conn/  │ │ 原子写 tmp+rename  │             │
│  │  outputBuf) │ │  ready/hb) │ │ 环形100行持久化    │             │
│  └────────────┘ └────────────┘ └────────────────────┘             │
│                                                                  │
│  ┌──────────────────────────────────────────────────────┐        │
│  │  SessionMonitor (reconcileSessions 每秒巡检)           │        │
│  │  5 状态机 + 5 层风暴防护 + 死亡自动 respawn            │        │
│  └──────────────────────────────────────────────────────┘        │
│                                                                  │
│  ┌──────────────────────────────────────────────────────┐        │
│  │  go:embed dashboard.html  (编译进二进制)                │        │
│  │  GET / → 浏览器渲染深色终端风格 SPA                     │        │
│  │  4 视图 hash 路由 + 2s 短轮询 + 增量 DOM 更新          │        │
│  └──────────────────────────────────────────────────────┘        │
└─────────────────────────────────────────────────────────────────┘
         ▲                ▲                ▲
         │                │                │
    ┌────┴────┐     ┌─────┴─────┐    ┌─────┴─────┐
    │ CLI     │     │ curl/脚本 │    │ 浏览器     │
    │ -cmd    │     │ 飞书Channel│    │ Dashboard │
    │ new/send│     │ REST API  │    │ 17891/    │
    │ list/   │     │           │    │ 单页应用   │
    │ close   │     │           │    │           │
    └─────────┘     └───────────┘    └───────────┘
```

**关键设计**：
- HTTP handler 不写业务逻辑，全部转调 daemon.go 已有公共方法
- 双端口完全隔离：TCP 走 JSON 行协议给 CLI/Worker，HTTP 走 REST 给浏览器/脚本
- go:embed 保证单二进制部署，HTML 不需要额外拷贝

### 2.2 HTTP 请求生命周期（GET /api/sessions 为例）

```
浏览器 2s 轮询
    │
    │ GET /api/sessions?sort=last_active_desc&status=active
    ▼
http_server.httpListSessions(w, r)
    │ 1. 解析 query 参数 (bot_id/status/sort/limit/offset)
    │ 2. 参数校验 (limit  clamp 1-500, offset >=0)
    │ 3. d.sessionsMu.RLock() → 遍历 d.sessions map
    │ 4. 过滤（bot_id/status/closed）
    │ 5. 排序（按 sort 参数）
    │ 6. 分页（offset/limit）
    │ 7. 构造 SessionListItem（含 outputs 预览）
    │ 8. d.sessionsMu.RUnlock()
    │ 9. d.writeJSON(200, resp)
    ▼
net/http 写入 TCP → 浏览器接收 JSON → 增量更新表格 DOM
```

### 2.3 发送消息数据流（POST /api/sessions/:sid/send）

```
浏览器输入 hello → Send 按钮点击
    │
    │ POST /api/sessions/s1/send  {"message":"hello"}
    ▼
http_server.handleSessionSend(w, r)
    │ 1. 解析 body，取 sid + message
    │ 2. 校验 message 非空
    │ 3. d.sessionsMu.RLock() → 查 meta
    │    - 不存在 → 404
    │    - closed → 400
    │ 4. d.workersMu.RLock() → 查 handle
    │    - no worker → 400 (status=CREATED)
    │    - !ready → 400
    │ 5. d.SendInput(sid, message)
    │    ├─ meta.AddOutput("[user] " + message)     ← V4 修复：用户输入写入 outputs
    │    ├─ d.store.UpdateOutput(sid, userLine)     ← 双写持久化到磁盘
    │    └─ h.Send(MsgUserInput) → Worker stdin → Adapter echo
    │ 6. sleep 400ms（等 worker 回显写入 outputBuf）
    │ 7. 返回 {ok, sent, outputs}
    ▼
浏览器拿到 outputs → 只更新 #chat-messages 区域（不碰 input）
```

### 2.4 前端轮询架构（增量 DOM 更新）

V4 最大的前端设计决策：**轮询不重建整个 DOM**。

```
                      ┌──────────────────────┐
                      │  window.load          │
                      │  → startPolling()     │
                      └──────────┬───────────┘
                                 │
                                 ▼
                      ┌──────────────────────┐
                      │  fullRender()         │  ← 仅在 hashchange 时调用
                      │  根据路由渲染完整页面：  │     （切页面/切 session）
                      │  - Home: 统计卡+表格   │
                      │  - Chat: 侧边栏+对话区  │
                      │  - New:  创建表单      │
                      │  - Bots: Bot 列表      │
                      └──────────┬───────────┘
                                 │
                                 ▼
                      ┌──────────────────────┐
                      │  setInterval(poll, 2s)│  ← 每 2 秒执行
                      └──────────┬───────────┘
                                 │
            ┌────────────────────┼────────────────────┐
            ▼                    ▼                    ▼
    ┌──────────────┐    ┌──────────────┐    ┌──────────────┐
    │ Home 页      │    │ Chat 页      │    │ New/Bots 页  │
    │ updateHome-  │    │ updateChat-  │    │ (不更新，     │
    │ Stats()      │    │ Header()     │    │  表单不碰)    │
    │ updateHome-  │    │ updateChat-  │    └──────────────┘
    │ Table()      │    │ Messages()   │
    │ (只更新 tbody)│    │ (output行数变 │
    └──────────────┘    │  了才更新)    │
                        │ updateChat-  │
                        │ Sidebar()    │
                        │ (只更新左侧   │
                        │  session 列表)│
                        │              │
                        │ 【不碰 input】 │ ← 关键：打字不丢
                        └──────────────┘
```

---

## 三、配置与结构体扩展

### 3.1 config.DaemonConfig 新增字段

[config.go#L36-L42](file:///Users/bytedance/botmux-go/internal/config/config.go#L36-L42)：

```go
type DaemonConfig struct {
    ListenAddr    string      `json:"listen_addr"`     // 原有，TCP CLI 默认 127.0.0.1:17890
    DashboardAddr string      `json:"dashboard_addr"`  // 【V4 新增】HTTP Dashboard 默认 127.0.0.1:17891
    LogLevel      string      `json:"log_level"`
    SessionsDir   string      `json:"sessions_dir"`
    Bots          []BotConfig `json:"bots"`
}
```

默认值两处设置：
- `DefaultConfig()`: `DashboardAddr: "127.0.0.1:17891"`
- `applyDefaults()`: 如果 JSON 中没配，fallback 到默认

### 3.2 Daemon struct 新增字段

[daemon.go#L20-L33](file:///Users/bytedance/botmux-go/internal/daemon/daemon.go#L20-L33)：

```go
type Daemon struct {
    cfg          *config.DaemonConfig
    listener     net.Listener   // 原有 TCP listener (17890)
    httpListener net.Listener   // 【V4 新增】HTTP listener (17891)
    startedAt    time.Time      // 【V4 新增】启动时刻（给 healthz 用）
    selfExe      string
    store        *SessionStore
    // ... sessions/workers/ctx/wg/closeMu 等原有字段不变
}
```

### 3.3 Start() / Stop() 生命周期

```go
func (d *Daemon) Start() error {
    // ... 原有：监听 TCP、restoreSessions、startSessionMonitor ...
    d.startedAt = time.Now()                              // V4: 记录启动时间
    d.wg.Add(3)                                           // V4: 2→3
    go d.acceptLoop()                                     // 原有：TCP accept
    go d.periodicGC()                                     // 原有：GC 协程
    go d.startHTTPServer()                                // V4: HTTP Server 协程
    return nil
}

func (d *Daemon) Stop() error {
    // ... 原有：closeMu.Lock、cancel、stopSessionMonitor ...
    if d.httpListener != nil {
        _ = d.httpListener.Close()                       // V4: 先关 HTTP
    }
    if d.listener != nil {
        _ = d.listener.Close()                           // 原有：再关 TCP
    }
    // ... 原有：CloseSession all、wg.Wait ...
}
```

### 3.4 startHTTPServer() 生命周期管理

[http_server.go#L12-L37](file:///Users/bytedance/botmux-go/internal/daemon/http_server.go#L12-L37)：

```go
func (d *Daemon) startHTTPServer() {
    defer d.wg.Done()
    ln, err := net.Listen("tcp", d.cfg.DashboardAddr)    // 监听 17891
    if err != nil { log.Printf(...); return }
    d.httpListener = ln                                   // 存引用供 Stop() 关闭

    mux := http.NewServeMux()
    mux.HandleFunc("/api/healthz", ...)                   // 注册路由
    mux.HandleFunc("/api/bots", ...)
    mux.HandleFunc("/api/sessions", ...)                  // 列表+新建（方法路由）
    mux.HandleFunc("/api/sessions/", d.handleSessionsSubrouter)  // :id 子路径手搓
    d.registerDashboardRoute(mux)                         // go:embed 静态文件（/ 根路径）

    srv := &http.Server{Handler: d.cors(mux)}             // CORS middleware 包裹
    go func() { _ = srv.Serve(ln) }()                     // Serve 阻塞，放新 goroutine

    <-d.ctx.Done()                                        // 等 daemon cancel
    _ = srv.Close()                                       // 优雅关闭
}
```

---

## 四、HTTP Server 核心实现

### 4.1 CORS 中间件

[http_server.go#L40-L59](file:///Users/bytedance/botmux-go/internal/daemon/http_server.go#L40-L59)：

```go
func (d *Daemon) cors(next http.Handler) http.Handler {
    return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
        w.Header().Set("Access-Control-Allow-Origin", "*")
        w.Header().Set("Access-Control-Allow-Methods", "GET, POST, DELETE, OPTIONS")
        w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
        if r.Method == http.MethodOptions {
            w.WriteHeader(http.StatusNoContent)
            return
        }
        next.ServeHTTP(w, r)
    })
}
```

自动响应 OPTIONS 预检请求，浏览器 Dashboard 的 fetch 不会被 CORS 拦截。

### 4.2 手搓子路由（stdlib 没有 chi/gin）

标准库 `http.ServeMux`（Go 1.23 之前）不支持路径参数 `/api/sessions/:sid`，我们手动解析：

[http_server.go#L63-L105](file:///Users/bytedance/botmux-go/internal/daemon/http_server.go#L63-L105)：

```go
func (d *Daemon) handleSessionsSubrouter(w http.ResponseWriter, r *http.Request) {
    // URL 格式: /api/sessions/{sid}  或  /api/sessions/{sid}/send  /history
    rest := strings.TrimPrefix(r.URL.Path, "/api/sessions/")
    parts := strings.SplitN(rest, "/", 2)
    sid := parts[0]
    sub := ""
    if len(parts) > 1 { sub = "/" + parts[1] }

    if sid == "" { /* 不应该到这里 */ }

    switch {
    case sub == "":
        // GET /api/sessions/:sid → handleGetSession
        // DELETE /api/sessions/:sid → httpCloseSession
        switch r.Method {
        case "GET":    d.handleGetSession(w, r, sid)
        case "DELETE": d.httpCloseSession(w, r, sid)
        default:       d.writeError(w, 405, "method not allowed")
        }
    case sub == "/send":
        if r.Method == "POST" { d.handleSessionSend(w, r, sid); return }
        d.writeError(w, 405, "method not allowed")
    case sub == "/history":
        if r.Method == "GET" { d.handleGetSessionHistory(w, r, sid); return }
        d.writeError(w, 405, "method not allowed")
    default:
        d.writeError(w, 404, "not found")
    }
}
```

### 4.3 统一响应工具

```go
func (d *Daemon) writeJSON(w http.ResponseWriter, status int, v any) {
    w.Header().Set("Content-Type", "application/json")  // 先设 header
    w.WriteHeader(status)                                // 再写 status（顺序不能反）
    _ = json.NewEncoder(w).Encode(v)
}

func (d *Daemon) writeError(w http.ResponseWriter, status int, msg string) {
    d.writeJSON(w, status, map[string]string{"error": msg})
}
```

**writeJSON 顺序**：HTTP 协议规定 Headers 必须在 Status Code 之前（Status Code 一发 header 就锁死了），所以必须先 `Header().Set()` 再 `WriteHeader()` 最后 `Encode()` body。

### 4.4 方法命名冲突修复

daemon.go 已有两个 TCP handler 方法叫 `handleListSessions()` 和 `handleCloseSession()`（处理 CLI `-cmd list/close`），Go 不允许同类型同名方法。HTTP 版重命名：
- `httpListSessions()` — HTTP GET /api/sessions
- `httpCloseSession()` — HTTP DELETE /api/sessions/:sid

---

## 五、REST API 规格

### 5.1 通用规范

- **Base URL**: `http://127.0.0.1:17891`
- **鉴权**: 无（127.0.0.1 本机绑定）
- **Content-Type**: `application/json`（除了 `/history?format=txt` 返回 `text/plain`）
- **错误响应**: `{"error": "<msg>"}` + 对应 HTTP status code
- **时间格式**: RFC3339 (`2026-08-17T14:30:00+08:00`)
- **CORS**: 所有响应带 `Access-Control-Allow-Origin: *`

### 5.2 端点一览

| # | Method | Path | Handler 位置 | 说明 |
|---|--------|------|-------------|------|
| 1 | GET | `/api/healthz` | [http_server.go#L122](file:///Users/bytedance/botmux-go/internal/daemon/http_server.go#L122) | 健康检查 |
| 2 | GET | `/api/bots` | [http_server.go#L508](file:///Users/bytedance/botmux-go/internal/daemon/http_server.go#L508) | Bot 列表 |
| 3 | GET | `/api/sessions` | [http_server.go#L177](file:///Users/bytedance/botmux-go/internal/daemon/http_server.go#L177) | Session 列表（过滤+排序+分页）|
| 4 | POST | `/api/sessions` | [http_server.go#L375](file:///Users/bytedance/botmux-go/internal/daemon/http_server.go#L375) | 新建会话 |
| 5 | GET | `/api/sessions/:sid` | [http_server.go#L297](file:///Users/bytedance/botmux-go/internal/daemon/http_server.go#L297) | 会话详情 |
| 6 | POST | `/api/sessions/:sid/send` | [http_server.go#L451](file:///Users/bytedance/botmux-go/internal/daemon/http_server.go#L451) | 发送消息 |
| 7 | GET | `/api/sessions/:sid/history` | [http_server.go#L335](file:///Users/bytedance/botmux-go/internal/daemon/http_server.go#L335) | 历史输出（JSON/TXT）|
| 8 | DELETE | `/api/sessions/:sid` | [http_server.go#L491](file:///Users/bytedance/botmux-go/internal/daemon/http_server.go#L491) | 关闭会话 |

### 5.3 错误码表

| HTTP | 场景 | 响应 body |
|------|------|----------|
| 400 | 参数/body 非法 / bot 不存在 / session 无 worker / message 空 | `{"error":"..."}` |
| 404 | session 不存在 / 路径不存在 | `{"error":"session not found: xxx"}` |
| 405 | 方法不对（如 POST /api/bots） | `{"error":"method not allowed: POST"}` |
| 409 | 创建时 session 已存在且未关闭 | `{"error":"session already exists: xxx","suggestion":"..."}` |
| 201 | 新建成功 | 业务 JSON |
| 200 | 其他成功 | 业务 JSON |

### 5.4 GET /api/healthz

**Response 200**：
```json
{
  "ok": true,
  "listen": "127.0.0.1:17890",
  "dashboard": "127.0.0.1:17891",
  "started_at": "2026-08-17T14:30:00+08:00",
  "sessions_count": 2,
  "workers_count": 1,
  "bots_count": 2,
  "version": "v4-pr-c"
}
```

### 5.5 GET /api/bots

**Response 200**：
```json
{
  "total": 2,
  "bots": [
    {"name":"mock-bot","bot_id":"bot-mock","cli_type":"mock","backend_type":"pty","working_dir":"~"},
    {"name":"codex-bot","bot_id":"bot-codex","cli_type":"codex","backend_type":"tmux","working_dir":"~/workspace"}
  ]
}
```

### 5.6 GET /api/sessions — 列表

**Query 参数（全部可选）**：

| 参数 | 类型 | 默认 | 说明 |
|------|------|------|------|
| bot_id | string | 不过滤 | 按 bot_id 精确匹配 |
| status | string | 不过滤 | `CREATED/SPAWNING/READY/RECOVERING/CLOSED`，小写 `closed` 也可以 |
| sort | string | `created_at_desc` | 排序方式：`created_at_asc/desc`, `last_active_asc/desc`, `session_id` |
| limit | int | 50 | 每页条数，自动 clamp 到 [1, 500] |
| offset | int | 0 | 分页偏移 |

**Response 200**：
```json
{
  "total": 3,
  "count": 1,
  "limit": 50,
  "offset": 0,
  "items": [{
    "session_id": "s1",
    "bot_id": "bot-mock",
    "cli_type": "mock",
    "status": "READY",
    "pid": 37890,
    "closed": false,
    "created_at": "2026-08-17T14:30:00+08:00",
    "last_active": "2026-08-17T14:35:00+08:00",
    "outputs": ["[user] hello", "[mock-echo] hello"]
  }]
}
```

### 5.7 POST /api/sessions — 新建

**Query 参数**：
- `force=true/1` — 强制清理已存在的同名 session（杀进程+删文件+清map），再重建

**Request Body**：
```json
{
  "session_id": "s1",      // 可选，缺则自动生成 s-<unixnano>
  "bot_id": "bot-mock"     // 必填
}
```

**Response 201 Created**：
```json
{
  "ok": true,
  "session_id": "s1",
  "bot_id": "bot-mock",
  "status": "READY",
  "ready": true
}
```
阻塞最多 10s 等待 worker OnReady，`ready=true` 表示 worker 就绪可以发消息。

**Response 409 Conflict**（已存在且未关闭）：
```json
{
  "error": "session already exists: s1",
  "session_id": "s1",
  "status": "READY",
  "closed": false,
  "suggestion": "use DELETE /api/sessions/s1 first, or POST with ?force=true"
}
```

### 5.8 GET /api/sessions/:sid — 详情

**Response 200**：
```json
{
  "session_id": "s1",
  "bot_id": "bot-mock",
  "cli_type": "mock",
  "cli_path": "",
  "working_dir": "~",
  "status": "READY",
  "pid": 37890,
  "closed": false,
  "created_at": "2026-08-17T14:30:00+08:00",
  "last_active": "2026-08-17T14:35:00+08:00",
  "outputs": ["[user] hello", "[mock-echo] hello"],
  "worker": {
    "pid": 37890,
    "ready": true,
    "last_hb": "2026-08-17T14:34:58+08:00"
  }
}
```
比列表项多：`cli_path`、`working_dir`、`worker` 快照（pid/ready/last_hb）。

### 5.9 POST /api/sessions/:sid/send — 发消息

**Request Body**：
```json
{"message": "hello"}
```

**行为**：调用 `SendInput()` 后 sleep 400ms 等待 bot 回显写入 outputBuf，返回最新 outputs。前端不应依赖这个接口同步拿全部输出，应该轮询 GET /api/sessions/:sid 获取新 outputs。

**Response 200**：
```json
{
  "ok": true,
  "session_id": "s1",
  "sent": "hello",
  "outputs": ["[user] hello", "[mock-echo] hello"]
}
```

### 5.10 GET /api/sessions/:sid/history — 历史输出

**Query 参数**：
- `format=json`（默认）— 返回 JSON
- `format=txt`/`text`/`plain` — 返回 `text/plain` + `Content-Disposition: attachment`，触发浏览器下载

**Response (JSON)**：
```json
{
  "session_id": "s1",
  "lines": 2,
  "outputs": ["[user] hello", "[mock-echo] hello"]
}
```

**Response (TXT)**：
```
Content-Type: text/plain; charset=utf-8
Content-Disposition: attachment; filename="s1-history.txt"

[1] [user] hello
[2] [mock-echo] hello
```

### 5.11 DELETE /api/sessions/:sid — 关闭

**Query 参数**：
- `reason=xxx` — 关闭原因（默认 `"dashboard request"`）

**行为**：异步执行 `go CloseSession(sid, reason)`（CloseSession 会 `Cmd.Wait()` 2s+ 等 worker 退出，不能阻塞 HTTP），返回后前端应轮询确认 `closed=true`。

**Response 200**：
```json
{
  "ok": true,
  "session_id": "s1",
  "reason": "dashboard done",
  "closed": true
}
```

---

## 六、Dashboard 单页实现

### 6.1 技术选型

| 决策 | 选择 | 理由 |
|------|------|------|
| 框架 | **无**（原生 JS） | 0 依赖、单文件、无需 npm 构建；页面逻辑简单（轮询+DOM 操作）|
| CSS | **内联 `<style>`** | 不依赖外部 CSS 框架；深色终端风格，GitHub Dark 配色 |
| 路由 | **hash 路由**（`location.hash` + `hashchange` 事件） | 不需要 History API，`file://` 协议也能工作，和 go:embed 完美配合 |
| 数据刷新 | **2s 短轮询** | 比 SSE 简单 10 倍；本地工具 2s 延迟可接受 |
| DOM 更新 | **增量更新** | 避免每 2s innerHTML 重建导致输入框内容丢失 |
| 字体 | `SF Mono, Menlo, Consolas, monospace` | macOS 原生等宽字体，终端感 |
| 打包方式 | **go:embed** | 编译进二进制，单文件部署 |

### 6.2 文件结构

```
internal/daemon/
├── embed_dashboard.go    ← 新文件：go:embed 指令 + 静态文件路由 (~35 行)
├── dashboard.html        ← 新文件：SPA 单页（HTML+CSS+JS，602 行）
├── http_server.go        ← 修改：注册 Dashboard 路由 + version 升级
└── ...（其他文件不变）
```

### 6.3 embed_dashboard.go

[embed_dashboard.go](file:///Users/bytedance/botmux-go/internal/daemon/embed_dashboard.go)：

```go
package daemon

import (
    "embed"
    "net/http"
)

//go:embed dashboard.html
var dashboardHTML string

func (d *Daemon) registerDashboardRoute(mux *http.ServeMux) {
    mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
        if r.URL.Path != "/" {
            http.NotFound(w, r)
            return
        }
        w.Header().Set("Content-Type", "text/html; charset=utf-8")
        w.Write([]byte(dashboardHTML))
    })
    mux.HandleFunc("/dashboard.html", func(w http.ResponseWriter, r *http.Request) {
        w.Header().Set("Content-Type", "text/html; charset=utf-8")
        w.Write([]byte(dashboardHTML))
    })
}
```

为什么用 `string` 而不是 `embed.FS`？因为只有 1 个文件，直接读成字符串最简单，不需要 `fs.Sub`。`/` 和 `/dashboard.html` 都返回同一个 HTML（hash 路由下浏览器端处理路由）。

### 6.4 页面布局

```
┌─────────────────────────────────────────────────────────────────┐
│  ⚫ botmux-go v4-pr-c    🟢 connected    s:2 w:1 b:2   up 5m3s  │ ← 顶栏
├─────────────────────────────────────────────────────────────────┤
│  [Sessions]  [+ New Session]  [Bots]                            │ ← Nav
├──────────┬──────────────────────────────────────────────────────┤
│          │  s1                      READY pid:37890 bot:mock  [⬇] [✕] │
│ s1  ●    │ ─────────────────────────────────────────────────────│
│ s2  ○    │ [1] YOU  hello world                                   │ ← 消息区
│          │ [2] BOT  hello world                                   │
│ s3  ●    │ [3] YOU  second msg                                    │
│          │ [4] BOT  second msg                                    │
│ + New    │                                                         │
│          │ ┌────────────────────────────────┬─────────┐          │
│          │ │ Type a message...               │  Send   │          │ ← 输入框
│          │ └────────────────────────────────┴─────────┘          │
└──────────┴──────────────────────────────────────────────────────┘
    ↑              ↑
 侧边栏         主内容区
(session列表)
```

### 6.5 4 个视图（hash 路由）

| Hash | 视图 | 内容 |
|------|------|------|
| `#/` | Sessions 首页 | 4 个统计卡片（Active/Closed/Workers/Bots）+ sessions 表格 + 每行 Open/Close 按钮 |
| `#/session/:sid` | 对话页 | 左侧 session 列表 + 右侧 header+消息区+输入框+Send/Close/Download |
| `#/new` | 新建页 | Session ID 输入 + Bot 下拉 + Create 按钮 |
| `#/bots` | Bots 页 | Bot 配置表格（name/bot_id/cli_type/backend_type/working_dir）|

### 6.6 消息显示（YOU/BOT/SYS 标签）

[dashboard.html#L309-L328](file:///Users/bytedance/botmux-go/internal/daemon/dashboard.html#L309-L328)：

前端 `parseChatLine()` 解析 output 行前缀，显示彩色标签：

| 行前缀 | 标签 | 颜色 |
|--------|------|------|
| `[user] ` | **YOU** | 蓝色 `#58a6ff` |
| `[mock-echo] ` / 其他 bot 输出 | **BOT** | 绿色 `#3fb950` |
| `[system] ` / `[daemon] ` | **SYS** | 黄色 `#d29922` |

后端 `SendInput()` 把用户消息以 `[user] ` 前缀写入 outputBuf（V4 修复：之前只有 bot 输出，没有用户输入）。

### 6.7 增量 DOM 更新（核心 bug 修复）

最初版本每个轮询周期都调用 `innerHTML` 重建整个页面，导致输入框里正在打的字每 2s 被清空。修复后拆分：

| 函数 | 调用时机 | 更新范围 |
|------|---------|---------|
| `fullRender()` | 页面加载 + hashchange | 整个 `#app` 容器重建 |
| `updateTopbar()` | 每次 poll | 只改顶栏数字（sessions/workers/bots/uptime/连接状态）|
| `updateHomeStats()` | Home 页 poll | 只改 4 个卡片的 `.value` 文本 |
| `updateHomeTable()` | Home 页 poll | 只改 `<tbody>` 内容 |
| `updateChatHeader()` | Chat 页 poll | 只改 header 里的 badge/pid/bot |
| `updateChatMessages()` | Chat 页 poll | **只在 outputs 行数变化时**更新消息区（`lastOutputLen` 缓存）|
| `updateChatSidebar()` | Chat 页 poll | 只改左侧 session 列表 |
| **输入框 `<input>`** | **永不**轮询更新 | ✅ 打字不丢失 |

### 6.8 状态颜色

| 状态 | 颜色 | badge class |
|------|------|-------------|
| READY | 绿色 `#3fb950` | `.sids`（绿底绿字）|
| SPAWNING/CREATED/RECOVERING | 黄色 `#d29922` | `.sids`（黄底黄字）|
| CLOSED | 红色 `#f85149` | （红底红字）|
| 连接中 | 绿点 `●` / 断开红点 `●` | `.dot-green` / `.dot-red` |

---

## 七、Daemon 层新增与修复

### 7.1 PurgeSession() — 彻底清理

[daemon.go#L367-L395](file:///Users/bytedance/botmux-go/internal/daemon/daemon.go#L367-L395)：

`?force=true` 创建时调用，彻底清理一个 session 的所有资源：
1. `Process.Kill()` 杀 worker 进程
2. 异步 `Cmd.Wait()` 回收（2s 超时防止卡死）
3. 关 worker conn
4. `delete(d.workers, sid)` 清 workers map
5. `delete(d.sessions, sid)` 清 sessions map
6. `d.store.remove(sid)` 删除磁盘 JSON 文件

### 7.2 spawnWorkerForSession 竞态修复（重连风暴根因）

V3 的 bug：`cmd.Start()` 之后才把 handle 放入 `d.workers` map。worker 进程启动极快（毫秒级），在 handle 注册之前就 TCP 回连上来，创建了一个孤儿 handle B，A.Conn=nil → IsAlive=false → reconcile 每秒 respawn → 重连风暴。

V4 修复：把 `d.workers[sid] = handle` **提前到 cmd.Start 之前**；如果 map 里已有旧 handle，先 Kill+Close 再替换。

```go
// V4 修复顺序：
if old, ok := d.workers[sid]; ok {
    if old.Cmd != nil && old.Cmd.Process != nil {
        _ = old.Cmd.Process.Kill()
    }
    _ = old.CloseConn()
}
d.workers[sid] = handle               // ← 先注册到 map
d.sessions[sid].Status = StatusSpawning
if err := handle.Cmd.Start(); err != nil {
    delete(d.workers, sid)            // ← 失败才回滚
    // ...
}
```

### 7.3 SendInput 写入用户消息

V4 修复（持久化用户输入）：
```go
func (d *Daemon) SendInput(id, input string) error {
    // ...
    msg := protocol.NewMessage(protocol.MsgUserInput, id, input)
    meta.touchActive()
    userLine := "[user] " + input
    meta.AddOutput(userLine)                    // 内存环形缓冲
    _ = d.store.UpdateOutput(id, userLine)       // 磁盘持久化（双写）
    return h.Send(msg)
}
```

### 7.4 removeSession 不删 map（V3 设计保持）

V4 一度错误地加了 `delete(d.sessions, id)`，破坏了 V3 设计（closed session 要留在内存供 `?status=closed` 查询）。已回退为只设 `Closed=true, Status=StatusClosed`，不 delete map。closed session 的磁盘 JSON 保留，支持 Download txt 下载历史。

---

## 八、3 PR 拆分记录

| PR | 代号 | 文件改动 | 交付 | 状态 |
|----|------|---------|------|------|
| **PR-A** | HTTP 架子 + healthz | 新：http_server.go<br/>改：config.go/main.go/daemon.go | 双端口启动、healthz 端点、-dashboard flag | ✅ 已合入 master |
| **PR-B** | 完整 REST API | 改：http_server.go（+435 行）<br/>改：daemon.go（spawn竞态/Purge/removeSession） | 8 端点、CORS、409 幂等、PurgeSession、spawn竞态修复、用户消息持久化 | ✅ 已合入 master |
| **PR-C** | Dashboard 单页 | 新：embed_dashboard.go（35行）<br/>新：dashboard.html（602行）<br/>改：http_server.go（路由注册） | go:embed、4 视图 SPA、增量 DOM 更新、YOU/BOT 标签、输入框不丢失 | ✅ 验收通过 |

### 代码量统计

| 文件 | 行数 | 说明 |
|------|------|------|
| internal/daemon/http_server.go | 553 | HTTP Server + 8 个 handler + CORS + 路由 |
| internal/daemon/embed_dashboard.go | 35 | go:embed + 静态路由 |
| internal/daemon/dashboard.html | 602 | 单页 SPA（HTML+CSS+JS） |
| internal/daemon/daemon.go | +80 行修改 | spawn竞态/PurgeSession/SendInput双写/Start-http/Stop-http |
| internal/config/config.go | +12 行 | DashboardAddr 字段+默认 |
| cmd/daemon/main.go | +10 行 | -dashboard flag + banner |
| **V4 新增代码合计** | **~740 行 Go + 602 行 HTML** | **总计 ~1342 行** |

整个 botmux-go 代码量：**3419 行 Go + 602 行 HTML = 4021 行**，0 第三方依赖。

---

## 九、验收 Case 记录

### 9.1 后端 API 验收（curl 冒烟）

| # | Case | 结果 |
|---|------|------|
| 1 | GET /api/healthz → ok=true, version=v4-pr-c | ✅ |
| 2 | GET /api/bots → total=2 | ✅ |
| 3 | GET /api/sessions 空表 → total=0 | ✅ |
| 4 | POST /api/sessions 创建 s-r3 → 201 ready=true | ✅ |
| 5 | POST 重复创建 → 409 Conflict + suggestion | ✅ |
| 6 | POST ?force=true 覆盖重建 → 201, 新 pid | ✅ |
| 7 | POST send → outputs 有 [user]+[mock-echo] 两行 | ✅ |
| 8 | GET detail → status=READY, pid≠0, worker.ready=true | ✅ |
| 9 | GET history JSON → lines=N, outputs 数组 | ✅ |
| 10 | GET history?format=txt → Content-Disposition 附件 | ✅ |
| 11 | DELETE close → closed=true | ✅ |
| 12 | GET ?status=closed&bot_id=&sort=&limit= → 过滤组合正确 | ✅ |
| 13 | GET /sessions/NOT-EXIST → 404 | ✅ |
| 14 | daemon log 无重连风暴（日志干净）| ✅ |
| 15 | 进程检查：1 个 daemon，0 个孤儿 worker | ✅ |

### 9.2 前端 Dashboard 验收

| # | Case | 结果 |
|---|------|------|
| A1 | 打开 `/` 页面加载成功，深色风格，绿点 connected | ✅ |
| A2 | 顶栏数字正确（sessions/workers/bots/uptime 每秒更新）| ✅ |
| A3 | 刷新浏览器不报错，状态保留 | ✅ |
| B1-B4 | 导航 Sessions/New/Bots 切换正常，hash 路由正确 | ✅ |
| C1-C3 | 创建 session 成功，自动跳转对话页，toast 提示 | ✅ |
| **D1** | **打字等 5s 不按回车，字符不消失**（核心 bug 修复验证）| ✅ |
| D2 | Send 后 YOU/BOT 彩色标签交替显示 | ✅ |
| D3 | 连续发多条消息自动滚动到底部 | ✅ |
| E1 | Download .txt 下载完整对话（含 [user] 输入）| ✅ |
| F1-F2 | 多 session 创建+切换，各自历史独立 | ✅ |
| G1-G2 | Close 后跳回首页，badge 变红，Active/Closed 计数正确 | ✅ |

---

## 十、V4 过程中修复的 Bug 汇总

| # | Bug | 严重度 | 修复位置 |
|---|-----|--------|---------|
| 1 | startedAt 字段未初始化（编译错误）| 高 | daemon.go struct + Start() |
| 2 | HTTP handler 与 TCP handler 同名（编译错误）| 高 | http_server.go 重命名为 httpListSessions/httpCloseSession |
| 3 | **spawnWorkerForSession 重连风暴**（cmd.Start 后才注册 handle）| 🔴 严重 | daemon.go 提前注册 handle + 清理旧 handle |
| 4 | PurgeSession 不杀进程（force=true 后孤儿 worker 重连）| 🔴 严重 | daemon.go 加 Process.Kill()+Wait+超时 |
| 5 | removeSession 错误 delete map（破坏 V3 closed 查询设计）| 高 | daemon.go 回退 delete，只设 Closed=true |
| 6 | 用户输入不写入 outputs（历史只有 bot 回复）| 高 | daemon.go SendInput 加 AddOutput+UpdateOutput 双写 |
| 7 | **输入框字符每 2s 消失**（poll 用 innerHTML 重建整个 DOM）| 🔴 严重 | dashboard.html 拆分 fullRender/update* 增量更新 |
| 8 | 首页轮询 innerHTML 重建表格（交互状态丢失）| 中 | dashboard.html updateHomeStats/updateHomeTable 局部更新 |
| 9 | 根路径 `/` 返回 FileServer 目录列表（不是 index.html）| 高 | embed_dashboard.go 显式 write dashboardHTML 内容 |

V4 共 **9 个 bug**，其中 3 个是严重问题（重连风暴、输入框丢失、根路径 404）。

---

## 十一、与官方 Dashboard 演进对齐

| 官方功能 | V4 对齐 |
|---------|---------|
| SPA shell + sessions 页面 | ✅ PR-C 深色终端风格 SPA |
| Session Discovery 查询参数 | ✅ PR-B bot_id/status/sort/limit/offset |
| 会话关闭/历史下载 | ✅ DELETE + /history?format=txt |
| 发送消息交互 | ✅ POST /send + YOU/BOT 彩色标签 |
| 多 Daemon 聚合 / SSE | ❌ 不做（YAGNI）|
| HMAC 门控 / Auth | ❌ 不做（127.0.0.1 安全）|
| i18n / 主题皮肤 | ❌ 不做（一套深色默认）|
| tmux 终端卡片 / WebSocket | ❌ 不做（留 V5+）|

---

## 十二、后续方向（V5 候选）

1. **SSE 实时推送**：替换 2s 轮询为 `/api/events` SSE 流，消息实时到达
2. **tmux 终端卡片**：xterm.js 嵌入网页，浏览器里直接操作终端
3. **完整对话历史**：环形缓冲改为 append-only log 文件，支持无限历史
4. **飞书 Channel Bot**：飞书消息回调 API，在飞书里和 bot 对话
5. **Bot 配置热加载**：Dashboard 上编辑 bots.json 无需重启 daemon
6. **Auth Token**：支持 `--auth-token` 启动参数，非本机访问需要 Bearer Token
