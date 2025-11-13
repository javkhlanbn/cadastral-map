#!/usr/bin/env python3
import requests

# Тест API для одного участка
cadastral_number = "16:33:060205:216"
url = f"https://pkk.rosreestr.ru/api/features/1/{cadastral_number}"

headers = {
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
}

print(f"Запрос к: {url}")
response = requests.get(url, headers=headers, timeout=10)
print(f"Статус: {response.status_code}")

if response.status_code == 200:
    data = response.json()
    print(f"Ответ: {data}")

    if 'feature' in data and data['feature']:
        feature = data['feature']
        if 'center' in feature:
            center = feature['center']
            print(f"\nКоординаты найдены!")
            print(f"  Широта: {center['y']}")
            print(f"  Долгота: {center['x']}")
        else:
            print("\nЦентр не найден в ответе")
    else:
        print("\nFeature не найден")
else:
    print(f"Ошибка: {response.text}")
