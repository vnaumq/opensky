import folium
import json
from kafka import KafkaConsumer
from datetime import datetime
import time
import os

print("🔄 ИНТЕРАКТИВНАЯ КАРТА В РЕАЛЬНОМ ВРЕМЕНИ")
print("=" * 50)

def create_flight_map(flights):
    """Создает карту с полетами"""
    # Создаем карту
    m = folium.Map(
        location=[50.0, 10.0],
        zoom_start=4,
        tiles='CartoDB positron'
    )

    # Добавляем полеты
    for flight in flights:
        # Определяем цвет по скорости
        speed = flight.get('speed', 0)
        if speed > 250:
            color = 'red'
        elif speed > 150:
            color = 'orange'
        else:
            color = 'blue'

        # Создаем маркер
        folium.Marker(
            location=[flight['lat'], flight['lon']],
            popup=f"<b>{flight.get('callsign', 'N/A')}</b><br>"
                  f"Скорость: {speed:.0f} м/с<br>"
                  f"Высота: {flight.get('alt', 0):.0f} м",
            icon=folium.Icon(color=color, icon='plane', prefix='fa')
        ).add_to(m)

    return m

def get_latest_flights():
    """Получает последние полеты из Kafka"""
    consumer = KafkaConsumer(
        'opensky-auto',
        bootstrap_servers=['localhost:9092'],
        auto_offset_reset='latest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        group_id='realtime-map',
        consumer_timeout_ms=5000  # 5 секунд
    )

    flights = []
    try:
        for message in consumer:
            flight = message.value
            if flight.get('lat') and flight.get('lon'):
                flights.append(flight)
    finally:
        consumer.close()

    return flights

# Главный цикл обновления
print("🚀 Запускаю обновление карты каждые 10 секунд")
print("Нажмите Ctrl+C для остановки")

update_count = 0

try:
    while True:
        update_count += 1
        print(f"\n📡 Обновление #{update_count} - {datetime.now().strftime('%H:%M:%S')}")

        # Получаем данные
        flights = get_latest_flights()
        print(f"📊 Найдено полетов: {len(flights)}")

        if flights:
            # Создаем новую карту
            flight_map = create_flight_map(flights)

            # Сохраняем
            map_file = 'flights_realtime.html'
            flight_map.save(map_file)

            print(f"🗺️  Карта обновлена: {map_file}")
            print("   Откройте файл в браузере и обновите страницу (F5)")
        else:
            print("⚠️  Нет данных для карты")

        # Ждем 10 секунд
        print("⏳ Жду 10 секунд...")
        time.sleep(10)

except KeyboardInterrupt:
    print("\n🛑 Остановка...")

print("\n" + "=" * 50)
print("🎉 Карта готова!")
print("=" * 50)