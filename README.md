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

原有命令帮助：

    node src/cli.js help
