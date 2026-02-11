# aggr-server 代码阅读 TODO

## 0. 启动前准备（10 分钟）
- [x] 阅读 `README.md`，明确项目目标：多交易所实时成交采集 + 持久化 + 历史查询 API。
- [x] 看 `package.json`，确认运行方式：`node index`（主入口）、`npm run dev`（pm2）。
- [x] 看 `src/config.js`，先理解三个关键开关：`collect`、`api`、`storage`。

## 1. 先跑通主链路（30~45 分钟）
- [ ] 从 `index.js` 开始，画出初始化顺序：`config -> exchanges -> new Server() -> SIGINT 清理`。
- [ ] 读 `src/server.js` 的构造函数，确认两条主路径：
  - 采集路径：连接交易所 + 收到 trades + 周期备份。
  - API 路径：`/products`、`/historical`、`/alert`。
- [ ] 跟踪 `dispatchRawTrades()` 到 `backupTrades()`，理解内存 `chunk` 如何落盘。

## 2. 吃透“交易所接入抽象层”（45~60 分钟）
- [ ] 读 `src/exchange.js`（最重要基类），重点看：
  - `link / unlink / resolveApi / createWs`
  - WebSocket 生命周期与重连逻辑
  - `emitTrades()` 数据输出格式
- [ ] 用 `src/exchanges/binance.js` 作为样例，理解子类只需要做三件事：
  - 定义产品端点 + WS 地址
  - 实现 `subscribe / unsubscribe / onMessage`
  - 把交易所原始消息转成统一 Trade 结构
- [ ] 快速浏览 `src/exchanges/*`，总结“新增交易所模板”。

## 3. 理解存储与查询（45~60 分钟）
- [ ] 读 `src/storage/files.js`：按时间窗口写文件、流复用、过期 gzip。
- [ ] 读 `src/storage/influx.js`：Trade -> Bar 聚合、Retention Policy、`fetch()` 返回历史点位。
- [ ] 回到 `src/server.js` 的 `/historical` 路由，确认如何选择兼容存储（`format === 'point'`）。

## 4. 理解运行期状态管理（30 分钟）
- [ ] 读 `src/services/connections.js`：连接恢复、动态重连阈值、指数（index）聚合。
- [ ] 读 `src/services/socket.js`：`influxCollectors` 集群模式（API 节点 vs Collector 节点）。
- [ ] 读 `src/services/alert.js`：告警注册/触发/持久化、集群转发流程。

## 5. 快速验证理解（20 分钟）
- [ ] 运行服务后调用：
  - `GET /products`
  - `GET /historical/{from}/{to}/{timeframe}/{markets}`
- [ ] 人工验证一个市场的数据流：交易所消息 -> Trade -> Storage -> Historical API 输出。
- [ ] 记录 3 个你最想改进的问题（如：容错、性能、可观测性）。

## 6. 二刷建议（深入实现原理）
- [ ] 以“单条 trade 的生命周期”为主线，逐函数追踪并做时序图。
- [ ] 以“故障恢复”为主线，追踪断线重连、missing trades 恢复、SIGINT 退出备份。
- [ ] 以“扩展性”为主线，尝试设计一个新交易所接入草稿（不写代码先画接口）。

## 推荐阅读顺序（最短路径）
1. `index.js`
2. `src/config.js`
3. `src/server.js`
4. `src/exchange.js`
5. `src/exchanges/binance.js`
6. `src/storage/files.js`
7. `src/storage/influx.js`
8. `src/services/connections.js`
9. `src/services/socket.js`
10. `src/services/alert.js`
