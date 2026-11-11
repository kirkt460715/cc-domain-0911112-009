# 网关联调协议

GET /observations?source=s1 前两次返回 429，s2 立即返回观测。/history 提供历史数据，/permanent 返回不可重试错误。响应只描述请求结果，不实现客户端调度。

/_requests 提供实际请求记录。可以复制场景并改变返回次数、数据和状态码。监听地址限于 loopback，示例不是在线账号。

    node synthetic_exchange/server.js
    node --test synthetic_exchange/smoke.test.js
