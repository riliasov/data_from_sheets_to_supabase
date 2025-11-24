"""
Финальная проверка всех источников данных перед согласованием схемы БД.
Читает данные и создает детальный отчет.
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from src.config import load_config
from src.sheets import get_sheets_client, read_sheet_data
import json


def final_data_check():
    """
    Финальная проверка всех источников перед согласованием БД.
    """
    print("=" * 80)
    print("ФИНАЛЬНАЯ ПРОВЕРКА ВСЕХ ИСТОЧНИКОВ ДАННЫХ")
    print("=" * 80)
    
    config = load_config()
    gc = get_sheets_client(config)
    sources = config.get('SOURCES', {})
    
    results = {}
    total_sources = len(sources)
    successful = 0
    failed = 0
    
    for i, (source_name, source_config) in enumerate(sources.items(), 1):
        print(f"\n[{i}/{total_sources}] {source_name}")
        print("-" * 80)
        
        spreadsheet_id = source_config.get('spreadsheet_id')
        sheet_identifiers = source_config.get('sheet_identifiers', [])
        ranges = source_config.get('ranges', {})
        use_gid = source_config.get('use_gid', False)
        hint = source_config.get('_hint', '')
        
        if spreadsheet_id in ["SPREADSHEET_ID_HERE", "ПРОВЕРЬТЕ_ДОСТУП"]:
            print(f"⚠️  Пропускаем - не настроен spreadsheet_id")
            failed += 1
            continue
        
        source_result = {
            'use_gid': use_gid,
            'hint': hint,
            'sheets': {}
        }
        
        try:
            for sheet_id in sheet_identifiers:
                range_str = ranges.get(sheet_id)
                
                try:
                    data = read_sheet_data(gc, spreadsheet_id, sheet_id, range_str, use_gid)
                    
                    if not data:
                        print(f"   ⚠️  {hint or sheet_id}: диапазон пустой")
                        continue
                    
                    rows = len(data)
                    cols = len(data[0]) if data else 0
                    headers = data[0] if data else []
                    
                    print(f"   ✅ {hint or sheet_id}: {rows} строк × {cols} колонок")
                    print(f"      Range: {range_str}")
                    print(f"      Колонки: {', '.join(headers[:5])}...")
                    
                    source_result['sheets'][sheet_id] = {
                        'rows': rows,
                        'columns': cols,
                        'headers': headers,
                        'range': range_str
                    }
                    
                    successful += 1
                    
                except Exception as e:
                    print(f"   ❌ Ошибка: {e}")
                    failed += 1
            
            results[source_name] = source_result
            
        except Exception as e:
            print(f"   ❌ Критическая ошибка: {e}")
            failed += 1
    
    # Сохраняем отчет
    output_path = 'tests/final_sources_report.json'
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    
    print(f"\n{'=' * 80}")
    print("ИТОГО")
    print(f"{'=' * 80}")
    print(f"✅ Успешно: {successful}")
    print(f"❌ Ошибки: {failed}")
    print(f"\n📄 Отчет сохранен: {output_path}")
    print(f"{'=' * 80}\n")
    
    # Выводим сводку по структуре
    print("СВОДКА ПО СТРУКТУРЕ ДАННЫХ:")
    print("-" * 80)
    for source_name, source_data in results.items():
        hint = source_data.get('hint', '')
        sheets = source_data.get('sheets', {})
        
        for sheet_id, sheet_info in sheets.items():
            rows = sheet_info.get('rows', 0)
            cols = sheet_info.get('columns', 0)
            print(f"{source_name:25} ({hint:20}): {rows:5} строк × {cols:2} колонок")


if __name__ == '__main__':
    final_data_check()
