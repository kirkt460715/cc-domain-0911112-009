# fieldledger 数据接入

项目使用 Node.js 22 内置模块，不依赖第三方服务。现有领域接口、版本记录、错误处理和回归测试均可离线运行。采集端点的合成服务与业务接收模块分别运行。

## 测试与产品演示

    npm test
    npm run build
    npm run acceptance
    node --test synthetic_exchange/smoke.test.js

## 可复现容器

    docker build -t local/fieldledger .
    docker run --rm --network none local/fieldledger

镜像使用固定摘要，依赖在构建前准备。run_checks.sh 完整执行测试、语法检查及产品演示，正常用时少于 90 秒，使用临时数据目录。联调服务可在同容器中监听 loopback，不需要公网。

## 网关协议

GET /observations?source=s1 前两次返回 429，s2 立即返回观测。/history 提供历史数据，/permanent 返回不可重试错误。响应只描述请求结果，不实现客户端调度。

    node synthetic_exchange/server.js

## 可恢复采集调度器

实时观测与历史补采进入同一条队列、共用同一预算，按 host 与凭据组分别限流，同一凭据组下的多个来源按公平计数轮转，避免一个来源占满名额。任务带来源、站点、时间窗、优先级和估计成本；只有网关数据通过现有批次导入落盘后，任务才标记完成，revision、迟到数据与隔离规则全部沿用既有逻辑。

调度状态写在 `--root/scheduler/scheduler-journal.jsonl`，与业务 journal 一样采用带哈希链的追加事务和写入锁，崩溃后重放即可恢复，过期租约会被接管。每个租约带 fencing 代号：来源暂停或租约被接管后，旧执行者迟到的响应只能产生 `progress.rejected`，不能再确认任务、写批次或推进进度。

响应分三类处理：429 读取 Retry-After 并退避，408/5xx 和网络错误按指数退避重试，4xx 等永久失败立即终止；重试超过 `maxAttempts` 进入 `exhausted`，与 `permanent_failed` 区分。

    node src/cli.js scheduler-enqueue --root DATA --input task.json
    node src/cli.js scheduler-status --root DATA
    node src/cli.js scheduler-pause   --root DATA --input '{"source":"s1"}'
    node src/cli.js scheduler-resume  --root DATA --input '{"source":"s1"}'
    node src/cli.js scheduler-takeover --root DATA --input '{"id":"task-1"}'
    node src/cli.js scheduler-verify  --root DATA
    node src/cli.js scheduler-run --root DATA --config scheduler.json --port 8080

HTTP 服务同样暴露 `/scheduler/tasks`、`/scheduler/status`、`/scheduler/sources/pause`、`/scheduler/sources/resume` 和 `/scheduler/tasks/takeover`。状态视图包含各状态计数、按来源/凭据组/host 的占用、共享预算（预留、已花、可用）、阻塞原因和最近的过期进度拒绝。

针对合成网关的完整演练（一个源反复 429 时，另一个实时源和历史补采照常完成）：

    node synthetic_exchange/scheduler_drill.js
    node --test synthetic_exchange/scheduler_smoke.test.js

原有命令帮助：

    node src/cli.js help
