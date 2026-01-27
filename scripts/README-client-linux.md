# Установка UDP Logger Client на Linux сервере

## Быстрый запуск через go run (для тестирования)

Если вы хотите быстро протестировать Client режим без установки:

```bash
# Вариант 1: Использовать готовый скрипт
./scripts/run-client.sh

# Вариант 2: Вручную с переменными окружения
export CLUSTER_MODE=client
export MASTER_ADDR=master.example.com
export MASTER_PORT=9999
export CLUSTER_TLS=true
export CLUSTER_CA_FILE=/path/to/ca.pem
export UDP_ADDR=:516
export SPOOL_DIR=/tmp/udp-logger-spool

go run ./cmd/udp-logger
```

Или одной строкой:

```bash
CLUSTER_MODE=client MASTER_ADDR=master.example.com CLUSTER_CA_FILE=./certs/ca.pem go run ./cmd/udp-logger
```

## Быстрая установка (production)

### 1. Сборка бинарного файла

```bash
cd /path/to/rabbit-log-writer
go build -o udp-logger ./cmd/udp-logger
```

Или используйте Docker для сборки:

```bash
docker build -t udp-logger:build .
docker create --name temp udp-logger:build
docker cp temp:/app/udp-logger ./udp-logger
docker rm temp
```

### 2. Установка через скрипт

```bash
sudo ./scripts/install-client.sh
```

### 3. Настройка конфигурации

Отредактируйте файл конфигурации:

```bash
sudo nano /etc/udp-logger/client.env
```

Обязательные параметры:
- `MASTER_ADDR` - адрес Master сервера
- `CLUSTER_CA_FILE` - путь к CA сертификату

Пример:

```bash
CLUSTER_MODE=client
MASTER_ADDR=master.example.com
MASTER_PORT=9999
CLUSTER_TLS=true
CLUSTER_CA_FILE=/etc/udp-logger/certs/ca.pem
UDP_ADDR=:516
SPOOL_DIR=/var/lib/udp-logger/spool
```

### 4. Копирование сертификата

```bash
sudo mkdir -p /etc/udp-logger/certs
sudo cp /path/to/ca.pem /etc/udp-logger/certs/ca.pem
sudo chmod 644 /etc/udp-logger/certs/ca.pem
```

### 5. Запуск сервиса

```bash
sudo systemctl start udp-logger-client
sudo systemctl enable udp-logger-client  # автозапуск при загрузке
```

### 6. Проверка статуса

```bash
# Статус сервиса
sudo systemctl status udp-logger-client

# Логи
sudo journalctl -u udp-logger-client -f

# Метрики (если HTTP_ADDR настроен)
curl http://localhost:9794/metrics
curl http://localhost:9794/healthz
```

## Ручная установка

Если не хотите использовать скрипт установки:

### 1. Создание пользователя

```bash
sudo useradd -r -s /bin/false -d /var/lib/udp-logger udp-logger
```

### 2. Создание директорий

```bash
sudo mkdir -p /opt/udp-logger
sudo mkdir -p /etc/udp-logger
sudo mkdir -p /var/lib/udp-logger/spool
sudo mkdir -p /etc/udp-logger/certs
```

### 3. Копирование файлов

```bash
sudo cp udp-logger /opt/udp-logger/
sudo chmod +x /opt/udp-logger/udp-logger
sudo cp scripts/client.env.example /etc/udp-logger/client.env
sudo cp scripts/udp-logger-client.service /etc/systemd/system/
```

### 4. Настройка прав доступа

```bash
sudo chown -R udp-logger:udp-logger /var/lib/udp-logger
sudo chmod 755 /var/lib/udp-logger/spool
```

### 5. Редактирование конфигурации

```bash
sudo nano /etc/udp-logger/client.env
```

### 6. Активация сервиса

```bash
sudo systemctl daemon-reload
sudo systemctl enable udp-logger-client
sudo systemctl start udp-logger-client
```

## Управление сервисом

```bash
# Запуск
sudo systemctl start udp-logger-client

# Остановка
sudo systemctl stop udp-logger-client

# Перезапуск
sudo systemctl restart udp-logger-client

# Статус
sudo systemctl status udp-logger-client

# Логи
sudo journalctl -u udp-logger-client -f
sudo journalctl -u udp-logger-client --since "1 hour ago"
```

## Проверка работы

### Тест UDP приема

```bash
echo "test message" | nc -u -w1 localhost 516
```

### Проверка метрик

```bash
curl http://localhost:9794/metrics | grep udp_logger
```

### Проверка подключения к Master

В логах должно быть:
```
client: connecting to master at master.example.com:9999
client: connected to master at master.example.com:9999
```

## Устранение неполадок

### Сервис не запускается

```bash
# Проверьте конфигурацию
sudo systemctl status udp-logger-client
sudo journalctl -u udp-logger-client -n 50

# Проверьте права доступа
ls -la /opt/udp-logger/udp-logger
ls -la /var/lib/udp-logger/spool
ls -la /etc/udp-logger/certs/ca.pem
```

### Не может подключиться к Master

1. Проверьте сетевую связность:
```bash
telnet master.example.com 9999
# или
nc -zv master.example.com 9999
```

2. Проверьте TLS сертификат:
```bash
openssl x509 -in /etc/udp-logger/certs/ca.pem -text -noout
```

3. Проверьте конфигурацию в `/etc/udp-logger/client.env`

### Проблемы с правами доступа

```bash
sudo chown -R udp-logger:udp-logger /var/lib/udp-logger
sudo chmod 755 /var/lib/udp-logger/spool
```

## Обновление

```bash
# Остановите сервис
sudo systemctl stop udp-logger-client

# Замените бинарный файл
sudo cp new-udp-logger /opt/udp-logger/udp-logger
sudo chmod +x /opt/udp-logger/udp-logger

# Запустите сервис
sudo systemctl start udp-logger-client
```

## Удаление

```bash
sudo systemctl stop udp-logger-client
sudo systemctl disable udp-logger-client
sudo rm /etc/systemd/system/udp-logger-client.service
sudo systemctl daemon-reload
sudo rm -rf /opt/udp-logger
sudo rm -rf /etc/udp-logger
sudo rm -rf /var/lib/udp-logger
sudo userdel udp-logger
```

