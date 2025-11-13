#!/usr/bin/env python3
"""
Скрипт для обработки ВСЕХ участков из Excel с геокодированием адресов
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

def geocode_address(address):
    """Геокодирование адреса через Nominatim OSM"""
    try:
        # Очищаем адрес
        clean_address = address.replace('Респ', 'Республика')
        clean_address = clean_address.replace('м.р-н', 'район')
        clean_address = clean_address.replace('с.п.', 'сельское поселение')

        url = "https://nominatim.openstreetmap.org/search"
        params = {
            'q': clean_address,
            'format': 'json',
            'limit': 1,
            'addressdetails': 1
        }

        headers = {
            'User-Agent': 'Cadastral Map App/1.0'
        }

        response = requests.get(url, params=params, headers=headers, timeout=10)

        if response.status_code == 200:
            results = response.json()
            if results and len(results) > 0:
                result = results[0]
                return {
                    'lat': float(result['lat']),
                    'lng': float(result['lon']),
                    'approximate': True,  # Это координаты района/населенного пункта
                    'display_name': result.get('display_name', '')
                }

        return None

    except Exception as e:
        return None

def process_excel(excel_path):
    """Обработка Excel файла"""
    print("Читаем Excel файл...")
    df = pd.read_excel(excel_path, header=1)

    print(f"Найдено {len(df)} записей в Excel\n")

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

    print(f"Обработано {len(lots)} участков с кадастровыми номерами\n")
    return lots

def main():
    excel_file = 'Выгрузка результатов поиска в реестре лотов.xlsx'

    # Обрабатываем Excel
    lots = process_excel(excel_file)

    print(f"{'='*60}")
    print("Геокодирование адресов через OpenStreetMap...")
    print("Это займет некоторое время (~ 1 сек на адрес)")
    print(f"{'='*60}\n")

    total = len(lots)
    success = 0
    failed = 0

    # Кэш для адресов
    address_cache = {}

    for i, lot in enumerate(lots):
        address = lot['address']
        print(f"[{i+1}/{total}] {lot['cadastral_number']}...", end=' ', flush=True)

        # Проверяем кэш
        if address in address_cache:
            coordinates = address_cache[address]
        else:
            coordinates = geocode_address(address)
            address_cache[address] = coordinates
            # Задержка только для новых адресов (правила Nominatim)
            time.sleep(1)

        if coordinates:
            lot['coordinates'] = coordinates
            success += 1
            print("✓ OK")
        else:
            failed += 1
            print("✗ Не найдено")

    print(f"\n{'='*60}")
    print(f"Результаты:")
    print(f"  Успешно геокодировано: {success}/{total} ({success*100//total if total > 0 else 0}%)")
    print(f"  Не найдено: {failed}/{total}")
    print(f"  Уникальных адресов: {len(address_cache)}")
    print(f"{'='*60}\n")

    # Сохраняем в JSON
    output_file = 'cadastral_data_with_coords.json'
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(lots, f, ensure_ascii=False, indent=2)

    print(f"✓ Данные сохранены в {output_file}")
    print(f"  Всего участков: {len(lots)}")
    print(f"  С координатами: {success}")
    print(f"\nТеперь откройте карту: http://localhost:8000/cadastral_map_final.html")
    print("Обновите страницу в браузере (Cmd+Shift+R)")

if __name__ == "__main__":
    main()
