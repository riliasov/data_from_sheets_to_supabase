"""
Модуль для очистки и нормализации данных перед загрузкой в БД.
"""
import pandas as pd


def clean_dataframe(df, table_name):
    """
    Очищает данные перед загрузкой:
    1. Числа: удаляет пробелы, конвертирует.
    2. Даты: конвертирует в datetime.
    3. Boolean: маппит.
    4. Текст: strip(), пустые -> None.
    
    Args:
        df (pd.DataFrame): Данные для очистки
        table_name (str): Имя целевой таблицы (для контекста)
        
    Returns:
        pd.DataFrame: Очищенный DataFrame
    """
    numeric_keywords = [
        'stoimost', 'summa', 'kolichestvo', 'bonus', 'nalichnye', 
        'perevod', 'terminal', 'vdolg', 'zp', 'oplata', 'stavka', 'spisano',
        'god', 'mesyats', 'chasy'
    ]
    
    boolean_cols = ['probili_na_evotore', 'vnesli_v_crm', 'relevant', 'zamena']
    
    for col in df.columns:
        # Пропускаем служебные
        if col in ['source_row_id', 'row_hash']:
            continue
            
        # 1. Даты
        date_keywords = ['data', 'date', 'zapis']
        if any(k in col for k in date_keywords):
            # dayfirst=True для DD.MM.YYYY
            df[col] = pd.to_datetime(df[col], dayfirst=True, errors='coerce')
            continue

        # 2. Числа
        is_numeric = any(k in col for k in numeric_keywords)
        if is_numeric:
            if df[col].dtype == 'object':
                # Удаляем пробелы только для чисел и меняем запятую на точку
                df[col] = df[col].astype(str).str.replace('\xa0', '').str.replace(' ', '').str.replace(',', '.').str.strip()
            # Конвертируем
            df[col] = pd.to_numeric(df[col], errors='coerce')
            continue
            
        # 3. Boolean
        if col in boolean_cols:
            df[col] = df[col].map({
                'TRUE': True, 'True': True, 'true': True, '1': True, 1: True,
                'FALSE': False, 'False': False, 'false': False, '0': False, 0: False,
                None: None
            })
            continue

        # 4. Остальной текст (клиенты, комментарии и т.д.)
        if df[col].dtype == 'object':
            df[col] = df[col].astype(str).str.strip()
            df[col] = df[col].replace({'': None, 'nan': None, 'None': None})
            
    return df
