# rabbit-log-writer (Go)

Простой UDP-логгер: слушает UDP (по умолчанию `:516`), добавляет временную метку к каждому полученному сообщению и записывает его в очередь RabbitMQ (по умолчанию `mikrotik`).

## Переменные окружения

- **UDP_ADDR**: UDP адрес для прослушивания, по умолчанию `:516`
- **UDP_READ_BUFFER**: Максимальный размер буфера для чтения, по умолчанию `1024` байт
- **HTTP_ADDR**: HTTP адрес для прослушивания (health check/метрики), по умолчанию `:9793`
- **BUFFER_SIZE**: Размер буфера в памяти (UDP→Rabbit), по умолчанию `1000`
- **QUEUE_NAME**: Имя очереди RabbitMQ, по умолчанию `mikrotik`
- **PUBLISH_RETRY_INTERVAL**: Интервал повтора при ошибках подключения/публикации, по умолчанию `5s` (также поддерживается просто число секунд, например `5`)

Локальный кэш (spool, для буферизации при отключении Rabbit и последующей отправки):

- **SPOOL_DIR**: Директория spool, по умолчанию `/tmp/udp-logger-spool`
- **SPOOL_MAX_BYTES**: Максимальный размер spool в байтах (0 = без ограничений), по умолчанию `1073741824` (1GiB)
- **SPOOL_SEGMENT_BYTES**: Максимальный размер одного segment-файла, по умолчанию `16777216` (16MiB)
- **SPOOL_FSYNC**: `true` для `fsync` каждой записи на диск (надежнее, но медленнее), по умолчанию `false`
- **SPOOL_LOG_INTERVAL**: Интервал логирования состояния spool (0 = отключено), по умолчанию `30s`

RabbitMQ:

- **RABBITMQ_HOST**: по умолчанию `localhost`
- **RABBITMQ_PORT**: по умолчанию `5672`
- **RABBITMQ_USER**: по умолчанию `guest`
- **RABBITMQ_PASSWORD**: по умолчанию `guest`
- **RABBITMQ_VHOST**: по умолчанию `/`

TLS (опционально):

- **RABBITMQ_TLS**: `true/false`; если не установлено, автоматически включается при `RABBITMQ_PORT=5671`
- **CERTS**: Директория с сертификатами (опционально), автоматически формируются пути:
  - `${CERTS}/ca.pem`
  - `${CERTS}/tls.crt`
  - `${CERTS}/tls.key`
- **RABBITMQ_CA_FILE / RABBITMQ_CERT_FILE / RABBITMQ_KEY_FILE**: Явное указание путей к сертификатам
- **RABBITMQ_TLS_SERVER_NAME**: Опционально
- **RABBITMQ_TLS_INSECURE_SKIP_VERIFY**: `true` для пропуска проверки сертификата (не рекомендуется)

## Сборка Docker

В корне репозитория выполните:

```bash
docker build -t udp-logger:go .
```

## Kubernetes

Примеры манифестов в `k8s/`:

- `k8s/deployment.yaml`: Содержит два контейнера `udp-logger` + `x509exporter`, настроен под ваши аннотации Vault Agent, логику ожидания сертификатов и пробы на порту 9793
- `k8s/service.yaml`: Service для доступа к метрикам и UDP порту
- `k8s/udp-socket-vault-agent-configmap.yaml`: Конфигурация Vault Agent (генерирует `tls.crt/tls.key/ca.pem`)
- `k8s/vault-secrets-wait-script-configmap.yaml`: Скрипт ожидания сертификатов для x509 exporter
- `k8s/monitoring/vmagent-inline-scrape-snippet.yaml`: Пример конфигурации VMAgent для сбора метрик

## Мониторинг

### Grafana Dashboard

Импортируйте `grafana-dashboard.json` в Grafana для визуализации всех метрик сервиса.

### Алерты

Алерты находятся в `alerts/k8s/udp-logger/`:

- **UdpLoggerRabbitMQDisconnected**: Критический - RabbitMQ отключен более 2 минут
- **UdpLoggerSpoolQueueBacklog**: Предупреждение - Очередь spool превышает 10,000 сообщений
- **UdpLoggerSpoolDiskUsageHigh**: Предупреждение - Использование диска spool превышает 768MB
- **UdpLoggerUdpMessagesDropped**: Критический - UDP сообщения отбрасываются из-за полного буфера
- **UdpLoggerRabbitPublishErrors**: Предупреждение - Ошибки публикации в RabbitMQ
- **UdpLoggerPodDown**: Критический - Pod недоступен
- **UdpLoggerSpoolMessagesDropped**: Критический - Сообщения отбрасываются из spool из-за лимита диска
- **UdpLoggerPublishRateLag**: Предупреждение - Скорость публикации отстает от скорости приема

## Метрики

Сервис предоставляет следующие Prometheus метрики на порту `:9794`:

- `udp_logger_udp_received_total` - Всего получено UDP сообщений
- `udp_logger_udp_dropped_total` - Всего отброшено UDP сообщений
- `udp_logger_rabbit_published_total` - Всего опубликовано в RabbitMQ
- `udp_logger_rabbit_connect_errors_total` - Ошибки подключения к RabbitMQ
- `udp_logger_rabbit_publish_errors_total` - Ошибки публикации в RabbitMQ
- `udp_logger_rabbit_connected` - Статус подключения к RabbitMQ (1 = подключен, 0 = отключен)
- `udp_logger_spool_queued` - Количество сообщений в очереди spool
- `udp_logger_spool_bytes` - Размер spool в байтах
- `udp_logger_spool_dropped_total` - Всего отброшено сообщений из spool

## Локальный запуск

```bash
cd /home/hhuser/bogdan/bogdan-repo/rabbit-log-writer
go run ./cmd/udp-logger
```

Отправка тестового UDP сообщения:

```bash
echo "test message" | nc -u -w1 127.0.0.1 516
```
