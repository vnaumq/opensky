#!/bin/bash

# Скрипт для настройки Debezium Connector

DEBEZIUM_URL="http://localhost:8083"
CONNECTOR_NAME="opensky-postgres-connector"

echo "Настройка Debezium Connector..."

# Проверяем доступность Debezium Connect
echo "Проверка доступности Debezium Connect..."
until curl -f $DEBEZIUM_URL/connectors > /dev/null 2>&1; do
    echo "Ожидание запуска Debezium Connect..."
    sleep 5
done

echo "Debezium Connect доступен"

# Проверяем, существует ли уже connector
if curl -f "$DEBEZIUM_URL/connectors/$CONNECTOR_NAME" > /dev/null 2>&1; then
    echo "Connector уже существует. Удаляем старый..."
    curl -X DELETE "$DEBEZIUM_URL/connectors/$CONNECTOR_NAME"
    sleep 2
fi

# Создаем connector
echo "Создание нового connector..."
curl -X POST "$DEBEZIUM_URL/connectors" \
    -H "Content-Type: application/json" \
    -d @- <<EOF
{
  "name": "$CONNECTOR_NAME",
  "config": {
    "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname": "postgres",
    "database.port": "5432",
    "database.user": "opensky_user",
    "database.password": "opensky_password",
    "database.dbname": "opensky",
    "database.server.name": "opensky",
    "table.include.list": "public.processed_flights,public.flight_statistics",
    "plugin.name": "pgoutput",
    "slot.name": "debezium_slot",
    "publication.name": "dbz_publication",
    "publication.autocreate.mode": "filtered",
    "key.converter": "org.apache.kafka.connect.json.JsonConverter",
    "value.converter": "org.apache.kafka.connect.json.JsonConverter",
    "key.converter.schemas.enable": "true",
    "value.converter.schemas.enable": "true",
    "transforms": "unwrap",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "transforms.unwrap.drop.tombstones": "false",
    "transforms.unwrap.delete.handling.mode": "rewrite",
    "transforms.unwrap.add.fields": "op,source.ts_ms"
  }
}
EOF

echo ""
echo "Проверка статуса connector..."
sleep 3
curl -s "$DEBEZIUM_URL/connectors/$CONNECTOR_NAME/status" | python3 -m json.tool

echo ""
echo "Debezium Connector настроен!"
echo "Топики в Kafka будут созданы автоматически:"
echo "  - opensky.public.processed_flights"
echo "  - opensky.public.flight_statistics"

