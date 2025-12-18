# rabbit-log-writer (Go)

一个简单的 UDP 日志收集器：监听 UDP（默认 `:516`），把收到的每条消息加上时间戳后写入 RabbitMQ 队列（默认 `mikrotik`）。

## 环境变量

- **UDP_ADDR**: UDP 监听地址，默认 `:516`
- **UDP_READ_BUFFER**: 每次读取最大字节数，默认 `1024`
- **HTTP_ADDR**: HTTP 监听地址（健康检查/指标），默认 `:9793`
- **BUFFER_SIZE**: 内存缓冲队列大小（UDP→Rabbit），默认 `1000`
- **QUEUE_NAME**: RabbitMQ 队列名，默认 `mikrotik`
- **PUBLISH_RETRY_INTERVAL**: 连接/发布失败后的重试间隔，默认 `5s`（也支持只写数字秒，比如 `5`）

本地缓存（spool，用于 Rabbit 断线缓存并恢复后补发）：

- **SPOOL_DIR**: spool 目录，默认 `/tmp/udp-logger-spool`
- **SPOOL_MAX_BYTES**: spool 最大占用字节数（0 表示不限制），默认 `1073741824`（1GiB）
- **SPOOL_SEGMENT_BYTES**: 单个 segment 文件上限，默认 `16777216`（16MiB）
- **SPOOL_FSYNC**: `true` 时每条消息落盘 `fsync`（更可靠但更慢），默认 `false`
- **SPOOL_LOG_INTERVAL**: 定时打印 spool 缓存状态（0 关闭），默认 `30s`

RabbitMQ：

- **RABBITMQ_HOST**: 默认 `localhost`
- **RABBITMQ_PORT**: 默认 `5672`
- **RABBITMQ_USER**: 默认 `guest`
- **RABBITMQ_PASSWORD**: 默认 `guest`
- **RABBITMQ_VHOST**: 默认 `/`

TLS（可选）：

- **RABBITMQ_TLS**: `true/false`；未设置时会在 `RABBITMQ_PORT=5671` 自动开启
- **CERTS**: 证书目录（可选），会自动拼出：
  - `${CERTS}/ca.pem`
  - `${CERTS}/tls.crt`
  - `${CERTS}/tls.key`
- **RABBITMQ_CA_FILE / RABBITMQ_CERT_FILE / RABBITMQ_KEY_FILE**: 显式指定证书路径
- **RABBITMQ_TLS_SERVER_NAME**: 可选
- **RABBITMQ_TLS_INSECURE_SKIP_VERIFY**: `true` 时跳过证书校验（不推荐）

## Docker 构建

在仓库根目录执行：

```bash
docker build -t udp-logger:go .
```

## Kubernetes

示例清单在 `k8s/`：

- `k8s/deployment.yaml`：包含 `udp-logger` + `x509exporter` 两个 container，并对齐你的 Vault Agent 注解、证书等待逻辑、9793 探针
- `k8s/udp-socket-vault-agent-configmap.yaml`：Vault Agent 配置（生成 `tls.crt/tls.key/ca.pem`）
- `k8s/vault-secrets-wait-script-configmap.yaml`：x509 exporter 等待证书脚本


# rabbitmq-log-writer
