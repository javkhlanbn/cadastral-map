#!/usr/bin/env python3
"""
Скрипт для получения РЕАЛЬНЫХ координат через прямой API Росреестра (ПКК)
"""
import pandas as pd
import json
import re
import time
import requests

def extract_cadastral_numbers(text):
    """Извлечение кадастровых номеров из текста"""
    if pd.isna(text):
        return []
    pattern = r'\d+:\d+:\d+:\d+'
    return re.findall(pattern, str(text))

def process_excel(excel_path):
    """Обработка Excel файла"""
    print("Читаем Excel файл...")
    df = pd.read_excel(excel_path, header=1)

    print(f"Найдено {len(df)} записей в Excel")

    lots = []

    for idx, row in df.iterrows():
        # Извлекаем кадастровый номер
        desc = row.get('Характеристики имущества', '')
        cadastral_numbers = extract_cadastral_numbers(desc)

        if not cadastral_numbers:
            continue

        cadastral_number = cadastral_numbers[0]

        # Извлекаем площадь
        area_match = re.search(r'площадью?\s+([\d\s,]+)\s*(?:кв\.?\s*м|м)', str(desc), re.IGNORECASE)
        area = None
        if area_match:
            area_str = area_match.group(1).replace(' ', '').replace(',', '.')
            try:
                area = float(area_str)
            except:
                pass

        # Извлекаем использование
        usage_match = re.search(r'использование[^\w]+([\w\s]+?)(?:\.|Вид)', str(desc), re.IGNORECASE)
        usage = usage_match.group(1).strip() if usage_match else None

        lot_data = {
            'id': idx,
            'cadastral_number': cadastral_number,
            'area': area,
            'usage': usage,
            'address': str(row.get('Местонахождение имущества', '')),
            'description': str(row.get('Описание лота', ''))[:500] if pd.notna(row.get('Описание лота')) else '',
            'price': float(row.get('Начальная цена')) if pd.notna(row.get('Начальная цена')) else None,
            'final_price': float(row.get('Итоговая цена')) if pd.notna(row.get('Итоговая цена')) else None,
            'deposit': float(row.get('Размер задатка')) if pd.notna(row.get('Размер задатка')) else None,
            'auction_step': float(row.get('Шаг аукциона')) if pd.notna(row.get('Шаг аукциона')) else None,
            'status': str(row.get('Статус лота', '')),
            'lot_number': row.get('Номер лота', ''),
            'auction_number': str(row.get('Номер извещения', '')),
            'auction_type': str(row.get('Вид торгов', '')),
            'ownership_form': str(row.get('Форма собственности', '')),
            'subject_rf': str(row.get('Субъект РФ', '')),
            'organizer': str(row.get('Наименование организации', '')),
            'organizer_inn': row.get('ИНН', ''),
            'owner': str(row.get('Наименование организации.1', '')),
            'owner_inn': row.get('ИНН.1', ''),
            'link': str(row.get('Ссылка на лот в ОЧ Реестра лотов', '')),
        }

        lots.append(lot_data)

    print(f"Обработано {len(lots)} участков с кадастровыми номерами")
    return lots

def get_coordinates_from_pkk(cadastral_number):
    """Получение координат через публичный API ПКК Росреестра"""
    try:
        # Публичная кадастровая карта API
        url = f"https://pkk.rosreestr.ru/api/features/1/{cadastral_number}"

        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
        }

        response = requests.get(url, headers=headers, timeout=10)

        if response.status_code == 200:
            data = response.json()

            if 'feature' in data and data['feature']:
                feature = data['feature']

                # Извлекаем координаты центра
                if 'center' in feature and feature['center']:
                    center = feature['center']
                    return {
                        'lat': center['y'],
                        'lng': center['x'],
                        'approximate': False
                    }

        return None

    except Exception as e:
        return None

def main():
    excel_file = 'Выгрузка результатов поиска в реестре лотов.xlsx'

    # Обрабатываем Excel
    lots = process_excel(excel_file)

    print(f"\n{'='*60}")
    print("Получаем РЕАЛЬНЫЕ координаты из ПКК Росреестра...")
    print("Это может занять несколько минут...")
    print(f"{'='*60}\n")

    total = len(lots)
    success = 0
    failed = 0

    for i, lot in enumerate(lots):
        cadastral_number = lot['cadastral_number']
        print(f"[{i+1}/{total}] {cadastral_number}...", end=' ', flush=True)

        coordinates = get_coordinates_from_pkk(cadastral_number)

        if coordinates:
            lot['coordinates'] = coordinates
            success += 1
            print("✓ OK")
        else:
            failed += 1
            print("✗ Не найдено")

        # Задержка между запросами
        if i < total - 1:
            time.sleep(0.3)

    print(f"\n{'='*60}")
    print(f"Результаты:")
    print(f"  Успешно получено: {success}/{total} ({success*100//total if total > 0 else 0}%)")
    print(f"  Не найдено: {failed}/{total}")
    print(f"{'='*60}\n")

    # Сохраняем в JSON
    output_file = 'cadastral_data_with_coords.json'
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(lots, f, ensure_ascii=False, indent=2)

    print(f"✓ Данные сохранены в {output_file}")
    print(f"  Всего участков: {len(lots)}")
    print(f"  С координатами: {success}")
    print(f"\nТеперь откройте карту: http://localhost:8000/cadastral_map_final.html")
    print("\nОбновите страницу в браузере (Ctrl+Shift+R или Cmd+Shift+R)")

if __name__ == "__main__":
    main()
