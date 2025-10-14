#!/bin/bash

# Скрипт настройки проекта OpenSky

set -e

echo "🚀 Настройка проекта OpenSky..."

# Проверка наличия Docker и Docker Compose
if ! command -v docker &> /dev/null; then
    echo "❌ Docker не установлен. Пожалуйста, установите Docker."
    exit 1
fi

if ! command -v docker-compose &> /dev/null; then
    echo "❌ Docker Compose не установлен. Пожалуйста, установите Docker Compose."
    exit 1
fi

# Создание .env файла если его нет
if [ ! -f .env ]; then
    echo "📝 Создание .env файла..."
    cp .env.example .env
    echo "⚠️  Пожалуйста, отредактируйте .env файл с вашими настройками"
fi

# Создание необходимых директорий
echo "📁 Создание директорий..."
mkdir -p data/raw
mkdir -p data/processed
mkdir -p logs
mkdir -p monitoring/grafana/dashboards
mkdir -p monitoring/grafana/datasources

# Установка прав доступа
echo "🔐 Настройка прав доступа..."
chmod +x scripts/*.sh

# Сборка Docker образов
echo "🐳 Сборка Docker образов..."
docker-compose build

# Запуск сервисов
echo "🚀 Запуск сервисов..."
docker-compose up -d

# Ожидание готовности сервисов
echo "⏳ Ожидание готовности сервисов..."
sleep 30

# Проверка статуса сервисов
echo "🔍 Проверка статуса сервисов..."
docker-compose ps

echo "✅ Настройка завершена!"
echo ""
echo "🌐 Доступные сервисы:"
echo "  - Airflow UI: http://localhost:8080 (admin/admin)"
echo "  - Grafana: http://localhost:3000 (admin/admin)"
echo "  - Prometheus: http://localhost:9090"
echo "  - PostgreSQL: localhost:5432"
echo ""
echo "📊 Для начала работы:"
echo "  1. Настройте credentials в .env файле"
echo "  2. Запустите DAG в Airflow UI"
echo "  3. Проверьте дашборды в Grafana"
