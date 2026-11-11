# 网关联调协议

GET /observations?source=s1 前两次返回 429，s2 立即返回观测。/history 提供历史数据，/permanent 返回不可重试错误。响应只描述请求结果，不实现客户端调度。

/_requests 提供实际请求记录。可以复制场景并改变返回次数、数据和状态码。监听地址限于 loopback，示例不是在线账号。

    node synthetic_exchange/server.js
    node --test synthetic_exchange/smoke.test.js

## 调度器公平性演练

`scheduler_drill.js` 会在临时目录启动 Ledger 与采集调度器，对接本目录的合成场景：s1 反复返回 429，s2 立即返回观测，/history 提供补采数据。演练验证实时任务与历史补采不会被持续限流的来源饿死，429 走 Retry-After 退避，预算和队列状态可查。

    node synthetic_exchange/scheduler_drill.js
    node --test synthetic_exchange/scheduler_smoke.test.js
