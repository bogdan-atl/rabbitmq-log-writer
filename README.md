# rabbit-log-writer (Go)

Простой UDP-логгер: слушает UDP (по умолчанию `:516`), добавляет временную метку к каждому полученному сообщению и записывает его в очередь RabbitMQ (по умолчанию `mikrotik`).

## Режимы работы

Сервис поддерживает три режима работы:

1. **Standalone** (по умолчанию): UDP → Spool → RabbitMQ
2. **Client**: UDP → Spool → TCP (зашифровано) → Master → RabbitMQ
3. **Master**: TCP Server (от клиентов) + UDP (fallback) → Spool → RabbitMQ

## Переменные окружения

### Базовые настройки

- **UDP_ADDR**: UDP адрес для прослушивания, по умолчанию `:516`
- **UDP_READ_BUFFER**: Максимальный размер буфера для чтения, по умолчанию `1024` байт
- **HTTP_ADDR**: HTTP адрес для прослушивания (health check/метрики), по умолчанию `:9793`
- **BUFFER_SIZE**: Размер буфера в памяти (UDP→Rabbit), по умолчанию `1000`
- **QUEUE_NAME**: Имя очереди RabbitMQ, по умолчанию `mikrotik`
- **PUBLISH_RETRY_INTERVAL**: Интервал повтора при ошибках подключения/публикации, по умолчанию `5s` (также поддерживается просто число секунд, например `5`)

### Режим кластера

- **CLUSTER_MODE**: Режим работы - `client`, `master` или пусто (standalone). Если установлен `MASTER_ADDR`, автоматически включается режим `client`
- **MASTER_ADDR**: IP адрес или hostname Master сервера (для Client режима)
- **MASTER_PORT**: TCP порт Master сервера, по умолчанию `9999`
- **CLUSTER_TLS**: Включить TLS шифрование между Client и Master, по умолчанию `true`

**TLS конфигурация (для Client):**

Для **одностороннего TLS** (только проверка сервера, без клиентского сертификата):
- **CLUSTER_CA_FILE**: Путь к CA сертификату для проверки Master (обязательно для TLS)

Для **двустороннего TLS (mTLS)** (с клиентским сертификатом):
- **CLUSTER_CA_FILE**: Путь к CA сертификату (по умолчанию `${CERTS}/ca.pem`)
- **CLUSTER_CERT_FILE**: Путь к клиентскому сертификату (по умолчанию `${CERTS}/tls.crt`)
- **CLUSTER_KEY_FILE**: Путь к клиентскому ключу (по умолчанию `${CERTS}/tls.key`)

**Дополнительные опции:**
- **CLUSTER_TLS_SERVER_NAME**: Имя сервера для TLS проверки (опционально)
- **CLUSTER_TLS_INSECURE_SKIP_VERIFY**: Пропустить проверку сертификата (не рекомендуется)

**Примечание:** Если указан только `CLUSTER_CA_FILE`, используется односторонний TLS (клиент проверяет только сервер). Если также указаны `CLUSTER_CERT_FILE` и `CLUSTER_KEY_FILE`, используется двусторонний TLS (mTLS).

Локальный кэш (spool, для буферизации при отключении Rabbit и последующей отправки):

- **QUEUE_BACKEND**: backend для буфера неотправленных сообщений: `spool` (по умолчанию) или `redis`
- **SPOOL_DIR**: Директория spool, по умолчанию `/tmp/udp-logger-spool`
- **SPOOL_MAX_BYTES**: Максимальный размер spool в байтах (0 = без ограничений), по умолчанию `1073741824` (1GiB)
- **SPOOL_SEGMENT_BYTES**: Максимальный размер одного segment-файла, по умолчанию `16777216` (16MiB)
- **SPOOL_FSYNC**: `true` для `fsync` каждой записи на диск (надежнее, но медленнее), по умолчанию `false`
- **SPOOL_LOG_INTERVAL**: Интервал логирования состояния spool (0 = отключено), по умолчанию `30s`

Redis (используется только при `QUEUE_BACKEND=redis`):

- **REDIS_ADDR**: адрес Redis, по умолчанию `localhost:6379`
- **REDIS_PASSWORD**: пароль Redis, по умолчанию пусто
- **REDIS_DB**: номер БД Redis, по умолчанию `0`
- **REDIS_QUEUE_KEY**: ключ списка с неотправленными сообщениями, по умолчанию `udp-logger:queue`
- **REDIS_PROCESSING_KEY**: ключ списка "в обработке", по умолчанию `udp-logger:queue:processing`
- **REDIS_DEAD_LETTER_KEY**: ключ списка dead-letter, по умолчанию `udp-logger:queue:dead-letter`
- **REDIS_MAX_RETRIES**: максимальное число попыток отправки перед dead-letter, по умолчанию `10`
- **REDIS_VISIBILITY_TIMEOUT**: через сколько сообщение в `processing` считается "зависшим", по умолчанию `30s`
- **REDIS_REAPER_INTERVAL**: интервал фоновой проверки зависших сообщений, по умолчанию `5s`

RabbitMQ:

- **RABBITMQ_HOST**: по умолчанию `localhost`
- **RABBITMQ_PORT**: по умолчанию `5672`
- **RABBITMQ_USER**: по умолчанию `guest`
- **RABBITMQ_PASSWORD**: по умолчанию `guest`
- **RABBITMQ_VHOST**: по умолчанию `/`

Важно: сервис не создает очередь в RabbitMQ автоматически. Очередь из `QUEUE_NAME` должна быть создана заранее (например, как `quorum`/durable согласно вашей политике RabbitMQ).

TLS (опционально):

- **RABBITMQ_TLS**: `true/false`; если не установлено, автоматически включается при `RABBITMQ_PORT=5671`
- **CERTS**: Директория с сертификатами (опционально), автоматически формируются пути:
  - `${CERTS}/ca.pem`
  - `${CERTS}/tls.crt`
  - `${CERTS}/tls.key`
- **RABBITMQ_CA_FILE / RABBITMQ_CERT_FILE / RABBITMQ_KEY_FILE**: Явное указание путей к сертификатам
- **RABBITMQ_TLS_SERVER_NAME**: Опционально
- **RABBITMQ_TLS_INSECURE_SKIP_VERIFY**: `true` для пропуска проверки сертификата (не рекомендуется)

## Сборка

### Docker образ

В корне репозитория выполните:

```bash
docker build -t udp-logger:go .
```

### Бинарный файл для Linux

```bash
go build -o udp-logger ./cmd/udp-logger
```

Или для конкретной архитектуры:

```bash
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w" -o udp-logger ./cmd/udp-logger
```

## Kubernetes

Примеры манифестов в `k8s/`:

### Standalone режим (по умолчанию)

- `k8s/deployment.yaml`: Содержит два контейнера `udp-logger` + `x509exporter`, настроен под ваши аннотации Vault Agent, логику ожидания сертификатов и пробы на порту 9794
- `k8s/service.yaml`: Service для доступа к метрикам и UDP порту

### Кластерный режим

- `k8s/deployment-master.yaml`: Deployment для Master режима (принимает TCP соединения от клиентов на порту 9999)
- `k8s/service-master.yaml`: Service для Master с портом 9999 для клиентских соединений
- `k8s/deployment-client.yaml`: Deployment для Client режима (подключается к Master, не требует порта 9999)

**Важно для Master режима:**
- В `deployment-master.yaml` добавлен `containerPort: 9999` (Master TCP порт)
- В `service-master.yaml` добавлен порт `9999` для доступа клиентов к Master

**Важно для Client режима:**
- Client не требует порта 9999 (только исходящие соединения)
- `MASTER_ADDR` должен указывать на Service Master (например, `udp-logger-master-service.niva-port-controller`)

### Общие файлы

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
- `udp_logger_queue_processing` - Количество сообщений в стадии processing (Redis backend)
- `udp_logger_queue_requeued_total` - Сколько сообщений возвращено из processing обратно в очередь
- `udp_logger_queue_dead_letter_total` - Сколько сообщений ушло в dead-letter после исчерпания retry
- `udp_logger_queue_last_problem_unix` - Unix-время последнего инцидента с зависшим processing

## Примеры конфигурации

### Standalone режим (по умолчанию)

```bash
export UDP_ADDR=":516"
export RABBITMQ_HOST="localhost"
export RABBITMQ_PORT="5672"
export QUEUE_NAME="mikrotik"
export QUEUE_BACKEND="spool"
go run ./cmd/udp-logger
```

### Standalone с Redis для неотправленных сообщений

```bash
export UDP_ADDR=":516"
export RABBITMQ_HOST="localhost"
export RABBITMQ_PORT="5672"
export QUEUE_NAME="mikrotik"
export QUEUE_BACKEND="redis"
export REDIS_ADDR="127.0.0.1:6379"
export REDIS_PASSWORD=""
export REDIS_DB="0"
export REDIS_MAX_RETRIES="10"
export REDIS_VISIBILITY_TIMEOUT="30s"
export REDIS_REAPER_INTERVAL="5s"
go run ./cmd/udp-logger
```

### Client режим

**Односторонний TLS (только CA файл):**

```bash
export CLUSTER_MODE="client"
export MASTER_ADDR="master.example.com"
export MASTER_PORT="9999"
export CLUSTER_TLS="true"
export CLUSTER_CA_FILE="/path/to/ca.pem"  # только CA для проверки сервера
export UDP_ADDR=":516"
go run ./cmd/udp-logger
```

**Двусторонний TLS (mTLS с клиентским сертификатом):**

```bash
export CLUSTER_MODE="client"
export MASTER_ADDR="master.example.com"
export MASTER_PORT="9999"
export CLUSTER_TLS="true"
export CLUSTER_CA_FILE="/path/to/ca.pem"
export CLUSTER_CERT_FILE="/path/to/client.crt"
export CLUSTER_KEY_FILE="/path/to/client.key"
export UDP_ADDR=":516"
go run ./cmd/udp-logger
```

**Или автоматически (если установлен `MASTER_ADDR`):**

```bash
export MASTER_ADDR="master.example.com"
export MASTER_PORT="9999"
export CLUSTER_CA_FILE="/path/to/ca.pem"  # минимальная конфигурация
go run ./cmd/udp-logger
```

### Master режим

```bash
export CLUSTER_MODE="master"
export MASTER_ADDR="0.0.0.0"  # или конкретный IP
export MASTER_PORT="9999"
export CLUSTER_TLS="true"
export CERTS="/path/to/certs/"
export UDP_ADDR=":516"  # fallback, если клиенты недоступны
export RABBITMQ_HOST="localhost"
export RABBITMQ_PORT="5672"
export QUEUE_NAME="mikrotik"
go run ./cmd/udp-logger
```

## Архитектура кластера

```
┌─────────────┐         ┌─────────────┐         ┌─────────────┐
│   Client 1  │         │   Client 2  │         │   Client N  │
│             │         │             │         │             │
│ UDP :516    │         │ UDP :516    │         │ UDP :516    │
│   ↓         │         │   ↓         │         │   ↓         │
│ Spool       │         │ Spool       │         │ Spool       │
│   ↓         │         │   ↓         │         │   ↓         │
│ TCP+TLS ────┼─────────┼─ TCP+TLS ───┼─────────┼─ TCP+TLS    │
└─────────────┘         └─────────────┘         └─────────────┘
                                │
                                │ (зашифровано)
                                ↓
                        ┌─────────────┐
                        │   Master    │
                        │             │
                        │ TCP :9999   │ ← от клиентов
                        │ UDP :516    │ ← fallback
                        │   ↓         │
                        │ Spool       │
                        │   ↓         │
                        │ RabbitMQ    │
                        └─────────────┘
```

**Преимущества кластерного режима:**

- **Масштабируемость**: Несколько Client узлов могут отправлять на один Master
- **Надежность**: Client имеет локальный spool, если Master недоступен - сообщения кэшируются
- **Безопасность**: Все сообщения между Client и Master зашифрованы через TLS
- **Отказоустойчивость**: Master может принимать UDP напрямую, если все клиенты недоступны

## Установка на Linux сервере

### Client режим

Для установки Client на Linux сервере см. подробную инструкцию:

📖 **[Установка Client на Linux](scripts/README-client-linux.md)**

Быстрый старт:

```bash
# 1. Соберите бинарный файл
go build -o udp-logger ./cmd/udp-logger

# 2. Запустите скрипт установки
sudo ./scripts/install-client.sh

# 3. Настройте конфигурацию
sudo nano /etc/udp-logger/client.env

# 4. Скопируйте CA сертификат
sudo cp ca.pem /etc/udp-logger/certs/

# 5. Запустите сервис
sudo systemctl start udp-logger-client
sudo systemctl enable udp-logger-client
```

## Локальный запуск (для тестирования)

### Standalone режим

```bash
cd /home/hhuser/bogdan/bogdan-repo/rabbit-log-writer
go run ./cmd/udp-logger
```

### Client режим (через go run)

**Вариант 1: Использовать скрипт**

```bash
./scripts/run-client.sh
```

**Вариант 2: Вручную с переменными окружения**

```bash
export CLUSTER_MODE=client
export MASTER_ADDR=master.example.com
export MASTER_PORT=9999
export CLUSTER_TLS=true
export CLUSTER_CA_FILE=/path/to/ca.pem
export UDP_ADDR=:516
export SPOOL_DIR=/tmp/udp-logger-spool
export QUEUE_BACKEND=spool

go run ./cmd/udp-logger
```

**Вариант 3: Одной строкой**

```bash
CLUSTER_MODE=client MASTER_ADDR=master.example.com CLUSTER_CA_FILE=./certs/ca.pem UDP_ADDR=:516 go run ./cmd/udp-logger
```

Client с Redis-буфером:

```bash
CLUSTER_MODE=client MASTER_ADDR=master.example.com CLUSTER_CA_FILE=./certs/ca.pem UDP_ADDR=:516 QUEUE_BACKEND=redis REDIS_ADDR=127.0.0.1:6379 go run ./cmd/udp-logger
```

### Master режим (через go run)

```bash
export CLUSTER_MODE=master
export MASTER_ADDR=0.0.0.0
export MASTER_PORT=9999
export CLUSTER_TLS=true
export CLUSTER_CA_FILE=/path/to/ca.pem
export CLUSTER_CERT_FILE=/path/to/tls.crt
export CLUSTER_KEY_FILE=/path/to/tls.key
export RABBITMQ_HOST=localhost
export RABBITMQ_PORT=5672
export UDP_ADDR=:516

go run ./cmd/udp-logger
```

### Тестирование

Отправка тестового UDP сообщения:

```bash
echo "test message" | nc -u -w1 127.0.0.1 516
```
