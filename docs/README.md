# botmux-go 学习笔记目录

> **项目定位**：用 Go 从零搭建「对外暴露 CLI、对内封装多种 AI Agent Adapter」的 IM-CLI 桥接服务  
> **开发方式**：按版本 V1 → V2 → V3 → ... 渐进迭代，每版本独立文档 + 独立飞书文档 + 独立架构图  
> **TS 原版仓库**：`/Users/bytedance/botmux`（TypeScript + Node.js 实现的生产级版本）

---

## 📚 文档结构

```
docs/
├── README.md                    ← 本文件（学习笔记目录）
└── versions/
    ├── v1-architecture.md       V1 学习笔记（脚手架）
    ├── v2-architecture.md       V2 学习笔记（会话持久化与恢复）
    ├── v3-architecture.md       V3 学习笔记（Session↔Worker 解耦与自愈）
    ├── v4-architecture.md       V4 技术方案（HTTP Dashboard + REST API，方案阶段）
    └── ...
```

---

## 🏗️ 版本总览

| 版本 | 代号 | 核心交付 | 代码量 | 本地文档 | 飞书文档 | 状态 |
|------|------|---------|--------|---------|---------|------|
| **V1** | 脚手架搭建 | Daemon + Worker 双进程、TCP JSON 行协议、MockAdapter Factory、`-cmd new/send` 命令 | ~1000 行 | [v1-architecture.md](versions/v1-architecture.md) | [飞书 Wiki](https://bytedance.larkoffice.com/wiki/KAECwXNiPi08eEkEpg6cggNGnhe) | ✅ 完成 |
| **V2** | 会话持久化与恢复 | SessionStore JSON 持久化、`restoreSessions()` 启动恢复、Worker 指数退避重连、tmux Adapter | ~716 行（改动） | [v2-architecture.md](versions/v2-architecture.md) | [飞书 Wiki](https://bytedance.larkoffice.com/wiki/WbpwwpjI1itL1fk7Cg7cYICAnTd) | ✅ 完成 |
| **V3** | Session↔Worker 解耦 + 自动自愈 | SessionMeta/WorkerHandle 双 map 解耦、SessionMonitor 每秒 reconcile 自动拉 Worker、`-cmd list/history/close`、6 状态状态机、5 层风暴防护 | ~1250 行 | [v3-architecture.md](versions/v3-architecture.md) | [飞书 Wiki](https://bytedance.larkoffice.com/wiki/I30SwgAlFi8eS5kKfgIcLOHznlc) | ✅ 完成 |
| V4 | HTTP Dashboard + REST API | 内嵌 HTTP Server（17891）、6 个 REST 端点（healthz/sessions/bots）、纯静态 Dashboard 单页、`?bot_id/status/sort` 查询参数对齐官方 Session Discovery | ~865 行 | [v4-architecture.md](versions/v4-architecture.md) | （待创建）| 📋 方案已定 |
| V5 | 真实 Agent CLI 接入 | CodexAdapter / BashAdapter / Provider Router | （待估） | （待写） | （待创建） | 📋 待规划 |
| V6 | 飞书/Lark Channel 接入 | `@bot` mention 路由、卡片流式更新、工具调用按钮 | （待估） | （待写） | （待创建） | 📋 待规划 |

---

## 🔗 飞书文档永久索引（按版本）

> 所有飞书 Wiki 文档请务必收藏。以后每次完成一版，自动在下表追加。

| 版本 | 文档标题（建议） | 飞书链接 | 创建日期 |
|------|-----------------|---------|---------|
| **V1** | botmux-go v1 学习笔记 & 架构演进 | https://bytedance.larkoffice.com/wiki/KAECwXNiPi08eEkEpg6cggNGnhe | 2026-08-06 |
| **V2** | botmux-go v2 会话持久化与恢复 | https://bytedance.larkoffice.com/wiki/WbpwwpjI1itL1fk7Cg7cYICAnTd | 2026-08-06 |
| **V3** | botmux-go v3 Session↔Worker 解耦与自愈 | https://bytedance.larkoffice.com/wiki/I30SwgAlFi8eS5kKfgIcLOHznlc | 2026-08-12 |
| **V4** | botmux-go v4 HTTP Dashboard + REST API | （待创建后填入） | — |
| **V5** | botmux-go v5 真实 Agent CLI 接入 | （待创建后填入） | — |
| **V6** | botmux-go v6 飞书 Channel 接入 | （待创建后填入） | — |

---

## 🧭 学习路线建议（推荐顺序）

1. **架构入门**：先通读 V1 文档的 3 张 Mermaid 架构图建立心智模型
2. **代码起步**：按 V1「4 步快速跑通」把 Daemon + new + send 链路跑通
3. **深入 V2**：按 V2 的「§十一 发现的问题」自己复现僵尸 Session，体会「为什么需要解耦」
4. **进入 V3**：读 V3 文档的「§二 架构演进图」+「§三 状态机」+「§五 reconcile 机制」，带 V2 痛点理解双 map 解耦的必要性
5. **动手 V3**：按 V3 文档的「§八 测试 SOP」跑 6 个 Case，重点体验 Case 4（Worker 被杀自愈）和 Case 6（close 竞争防护）
6. **横向对标**：同步参考 TS 原版 `botmux/src/daemon.ts`、`botmux/src/core/worker-pool.ts` 对比 Go 实现差异
7. **扩展动手**：V4/V5 任选一个方向做（Dashboard 或真实 CLI Adapter）

---

## 🛠️ 通用快速启动（所有版本通用）

```bash
cd /Users/bytedance/botmux-go
export BOTMUX_SESSIONS_DIR=/tmp/botmux-sessions
go build -o /tmp/botmux-go ./cmd/daemon

# 终端 1：Daemon
rm -rf $BOTMUX_SESSIONS_DIR && mkdir -p $BOTMUX_SESSIONS_DIR
/tmp/botmux-go -config ./configs/bots.json

# 终端 2：测试命令
/tmp/botmux-go -cmd new demo bot-mock
/tmp/botmux-go -cmd send demo "hello botmux"
```

更多命令、报错排查见对应版本的「快速参考手册」章节。

---

## 📝 新增版本模板（下次使用）

完成 Vn 后，按以下步骤更新此目录：

```
1. 新建 docs/versions/v{n}-architecture.md
2. 生成新飞书文档（复制本版本内容）
3. 在本文件的「版本总览」表加一行
4. 在本文件的「飞书文档永久索引」表加一行
5. 项目记忆 project_memory.md 追加 V{n} 飞书链接
6. 通知用户 "本地文档已更新，请复制到飞书"
```

---

最后更新：2026-08-12（V3 完成 + 双 map 解耦 + Monitor 自愈 + CLI 管理命令 + 6 Case 全回归）
