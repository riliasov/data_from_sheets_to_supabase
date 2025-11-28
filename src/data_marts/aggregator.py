"""
Aggregator - Pandas-based агрегация данных для витрин.

Использует pandas для локальной обработки (минимум нагрузки на Supabase).
"""
import pandas as pd
from sqlalchemy import Engine
import re
from src.logger import get_logger

logger = get_logger(__name__)


def normalize_client_name(name):
    """
    Нормализует имя клиента для устранения дубликатов.
    """
    if pd.isna(name) or name is None:
        return ""
    
    # Убираем все пробелы, tab, переносы
    normalized = re.sub(r'\s+', '', str(name))
    
    # Убираем лишние символы на краях
    normalized = normalized.strip()
    
    return normalized if normalized else ""


def get_clients_info(engine: Engine) -> pd.DataFrame:
    """
    Получает справочную информацию по клиентам (телефон, дети).
    """
    query = """
    SELECT klient, mobilnyy as mob, data_rozhdeniya_rebenka as dob, imya_rebenka as child_name
    FROM staging.clients_hst
    WHERE klient IS NOT NULL
    """
    df = pd.read_sql(query, engine)
    
    # Создаем канонический ключ
    df['klient_canonical'] = df['klient'].apply(lambda x: normalize_client_name(str(x)).lower())
    
    # Удаляем дубликаты по каноническому ключу (берем первую запись, где есть телефон)
    # Сортируем так, чтобы записи с телефоном были выше
    df['has_mobile'] = df['mob'].notna()
    df = df.sort_values('has_mobile', ascending=False)
    
    # Группируем
    result = df.groupby('klient_canonical', as_index=False).first()
    
    return result[['klient_canonical', 'klient', 'mob', 'dob', 'child_name']]


def aggregate_client_sales(engine: Engine) -> pd.DataFrame:
    """
    Агрегирует продажи по клиентам с разделением на категории и типы.
    """
    query = """
    SELECT klient, tip as type, kategoriya as category, produkt, kolichestvo, okonchatelnaya_stoimost, data::TEXT as date_str
    FROM staging.sales_hst
    UNION ALL
    SELECT klient, tip as type, kategoriya as category, produkt, kolichestvo, okonchatelnaya_stoimost, data as date_str
    FROM staging.sales_cur
    WHERE klient IS NOT NULL AND tip IS NOT NULL AND data IS NOT NULL
    """
    df = pd.read_sql(query, engine)
    
    # Дедупликация по контенту (включая дату)
    # Используем subset из всех значимых полей
    dedup_cols = ['klient', 'type', 'category', 'produkt', 'kolichestvo', 'okonchatelnaya_stoimost', 'date_str']
    
    # Заполняем пропуски для корректного сравнения
    for col in dedup_cols:
        if col in df.columns:
            df[col] = df[col].fillna('')
            
    df = df.drop_duplicates(subset=dedup_cols)
    
    df['klient_canonical'] = df['klient'].apply(lambda x: normalize_client_name(str(x)).lower())
    
    # Фильтруем только Бассейн и Ванны для основной логики
    df = df[df['type'].isin(['Бассейн', 'Ванны'])].copy()
    
    # Заполняем category
    df['category'] = df['category'].fillna('Unknown')
    
    # Заполняем пропуски
    df['produkt'] = df['produkt'].fillna('')
    df['kolichestvo'] = pd.to_numeric(df['kolichestvo'], errors='coerce').fillna(0)
    df['okonchatelnaya_stoimost'] = pd.to_numeric(df['okonchatelnaya_stoimost'], errors='coerce').fillna(0)
    
    # Категоризация строк
    def categorize(row):
        prod = str(row['produkt']).lower()
        if 'возврат абонемента' in prod:
            return 'vozvrashcheno'
        if 'сгорел' in prod:
            return 'sgorelo'
        if 'перерасчёт' in prod or 'перерасчет' in prod:
            return 'pereraschet'
        if 'подарок' in prod:
            return 'podareno'
        return 'priobreteno' # Все остальное считаем покупкой
        
    df['sales_category'] = df.apply(categorize, axis=1)
    
    # Агрегация
    # 1. Pivot для количеств
    pivot_cols = df.pivot_table(
        index=['klient_canonical', 'type', 'category'], 
        columns='sales_category', 
        values='kolichestvo', 
        aggfunc='sum', 
        fill_value=0
    ).reset_index()
    
    # Убедимся, что все колонки есть
    for col in ['priobreteno', 'vozvrashcheno', 'sgorelo', 'pereraschet', 'podareno']:
        if col not in pivot_cols.columns:
            pivot_cols[col] = 0
            
    # 2. Агрегация общей суммы и имени
    agg_meta = df.groupby(['klient_canonical', 'type', 'category'], as_index=False).agg({
        'okonchatelnaya_stoimost': 'sum',
        'klient': lambda x: max(x, key=len) if not x.empty else '' 
    })
    agg_meta = agg_meta.rename(columns={'okonchatelnaya_stoimost': 'total_summa', 'klient': 'sales_name_candidate'})
    
    # 3. Merge
    result = pd.merge(pivot_cols, agg_meta, on=['klient_canonical', 'type', 'category'], how='outer')
    
    return result


def aggregate_client_trainings(engine: Engine) -> pd.DataFrame:
    """
    Агрегирует тренировки по клиентам с разбивкой по статусам и категориям.
    """
    # 1. Trainings HST
    query_hst = """
    SELECT klient, tip as type, status, kategoriya as category, data::TEXT as date_str
    FROM staging.trainings_hst
    WHERE klient IS NOT NULL 
      AND tip IN ('Бассейн', 'Ванны')
    """
    df_hst = pd.read_sql(query_hst, engine)
    
    # 2. Trainings CUR (структура: col_5=klient, col_6=status, col_7=tip, col_8=kategoriya)
    # ВАЖНО: включаем source_row_id для дедупликации
    query_cur = """
    SELECT col_5 as klient, col_7 as type, col_6 as status, col_8 as category, source_row_id, col_2 as date_str
    FROM staging.trainings_cur
    WHERE col_5 IS NOT NULL 
      AND col_7 IN ('Бассейн', 'Ванны')
    """
    df_cur = pd.read_sql(query_cur, engine)
    
    # Дедупликация по source_row_id (в trainings_cur есть дубликаты)
    df_cur = df_cur.drop_duplicates(subset=['source_row_id'], keep='first')
    df_cur = df_cur.drop(columns=['source_row_id'])  # Удаляем служебную колонку
    
    # Объединяем
    df = pd.concat([df_hst, df_cur], ignore_index=True)
    
    # Дедупликация по контенту (включая дату) - аналогично sales
    dedup_cols = ['klient', 'type', 'status', 'category', 'date_str']
    
    # Заполняем пропуски для корректного сравнения
    for col in dedup_cols:
        if col in df.columns:
            df[col] = df[col].fillna('')
    
    df = df.drop_duplicates(subset=dedup_cols)
    
    # Фильтруем Администратор и другие служебные записи
    df = df[df['klient'] != 'Администратор'].copy()
    
    df['klient_canonical'] = df['klient'].apply(lambda x: normalize_client_name(str(x)).lower())
    
    # Заполняем пропущенные значения
    df['status'] = df['status'].fillna('Unknown')
    df['category'] = df['category'].fillna('Unknown')
    
    # Создаем pivot table: группируем по klient_canonical, type, category
    # и считаем количество для каждого статуса
    pivot = df.pivot_table(
        index=['klient_canonical', 'type', 'category'],
        columns='status',
        aggfunc='size',
        fill_value=0
    ).reset_index()
    
    # Убедимся что все нужные колонки статусов присутствуют
    status_columns = {
        'Посетили': 'experience',
        'Пропуск': 'propusk',
        'Лояльный пропуск': 'loyalnyy_propusk',
        'Отмена по вине центра': 'otmena_po_vine_tsentra',
        'Пропуск без списания': 'propusk_bez_spisaniya'
    }
    
    for old_col, new_col in status_columns.items():
        if old_col in pivot.columns:
            pivot = pivot.rename(columns={old_col: new_col})
        else:
            pivot[new_col] = 0
    
    # Сохраним имя клиента (самое длинное из группы)
    name_map = df.groupby('klient_canonical')['klient'].apply(lambda x: max(x, key=len)).to_dict()
    pivot['trainings_name_candidate'] = pivot['klient_canonical'].map(name_map)
    
    # Выбираем нужные колонки
    result_cols = ['klient_canonical', 'type', 'category', 'experience', 'propusk', 
                   'loyalnyy_propusk', 'otmena_po_vine_tsentra', 'propusk_bez_spisaniya',
                   'trainings_name_candidate']
    
    return pivot[result_cols]


def calculate_client_balance(sales_df: pd.DataFrame, trainings_df: pd.DataFrame, clients_info_df: pd.DataFrame) -> pd.DataFrame:
    """
    Сводит продажи, тренировки и инфо по клиентам.
    """
    # Merge Sales & Trainings по klient_canonical, tip, и kategoriya
    # 3. Merge
    # Используем outer join, чтобы не потерять клиентов
    merged = pd.merge(sales_df, trainings_df, on=['klient_canonical', 'type', 'category'], how='outer')
    
    # Заполнение нулей для продаж
    sales_cols = ['priobreteno', 'vozvrashcheno', 'sgorelo', 'pereraschet', 'podareno', 'total_summa']
    for col in sales_cols:
        if col in merged.columns:
            merged[col] = pd.to_numeric(merged[col], errors='coerce').fillna(0)
        else:
            merged[col] = 0
    
    # Заполнение нулей для тренировок
    training_cols = ['experience', 'propusk', 'loyalnyy_propusk', 'otmena_po_vine_tsentra', 'propusk_bez_spisaniya']
    for col in training_cols:
        if col in merged.columns:
            merged[col] = pd.to_numeric(merged[col], errors='coerce').fillna(0)
        else:
            merged[col] = 0
    
    # Заполняем category если отсутствует
    if 'category' not in merged.columns:
        merged['category'] = ''
    else:
        merged['category'] = merged['category'].fillna('')
            
    # Формула Остатка
    # Остаток = Приобретено - Experience - Propusk + Возвращено + Перерасчёт + Подарено + Сгорело
    merged['ostatok'] = (
        merged['priobreteno'] 
        - merged['experience']  # Посетили (списываются)
        - merged['propusk']      # Пропуск (тоже списываются)
        + merged['vozvrashcheno'] 
        + merged['pereraschet'] 
        + merged['podareno'] 
        + merged['sgorelo']
    )
    
    # Приведение к int (кроме суммы денег)
    int_cols = ['priobreteno', 'vozvrashcheno', 'pereraschet', 'podareno', 'sgorelo', 'ostatok',
                'experience', 'propusk', 'loyalnyy_propusk', 'otmena_po_vine_tsentra', 'propusk_bez_spisaniya']
    for col in int_cols:
        if col in merged.columns:
            merged[col] = merged[col].astype(int)
        
    # Имя кандидата
    merged['sales_name_candidate'] = merged['sales_name_candidate'].fillna('')
    merged['trainings_name_candidate'] = merged['trainings_name_candidate'].fillna('')
    merged['temp_name'] = merged.apply(
        lambda row: row['sales_name_candidate'] if len(str(row['sales_name_candidate'])) > len(str(row['trainings_name_candidate'])) 
        else row['trainings_name_candidate'], axis=1
    )
    
    # Merge с Clients Info
    final_df = pd.merge(merged, clients_info_df, on='klient_canonical', how='left')
    
    # Финализация имени
    final_df['klient_id'] = final_df['klient'].fillna(final_df['temp_name'])
    final_df['klient_id'] = final_df.apply(lambda x: x['klient_id'] if x['klient_id'] else x['klient_canonical'], axis=1)
    
    # Расчет возраста
    def calculate_age(birthdate):
        """Рассчитывает возраст. 
        - До 1 года: 'N мес'
        - 1-4 года: 'N года M мес' (или '1 год...')
        - 5+ лет: 'N лет' (только целые)
        """
        if pd.isna(birthdate):
            return ''
        
        from datetime import datetime, date
        
        # Преобразуем birthdate в date если это datetime
        if isinstance(birthdate, datetime):
            birthdate = birthdate.date()
        elif isinstance(birthdate, str):
            try:
                # Пробуем разные форматы
                for fmt in ['%Y-%m-%d', '%d.%m.%Y', '%d.%m.%y']:
                    try:
                        birthdate = datetime.strptime(birthdate, fmt).date()
                        break
                    except:
                        continue
                else:
                    return ''
            except:
                return ''
        
        today = date.today()
        
        # Рассчитываем разницу
        years = today.year - birthdate.year
        months = today.month - birthdate.month
        
        # Корректируем если месяц рождения еще не наступил в этом году
        if months < 0:
            years -= 1
            months += 12
        
        # Если меньше года - показываем только месяцы
        if years == 0:
            return f"{months} мес"
        
        # Если 5 лет и больше - только годы и слово "лет"
        if years >= 5:
            return f"{years} лет"
            
        # От 1 до 4 лет - десятичный формат (например, 1,9 года)
        if months == 0:
            if years == 1: return "1 год"
            return f"{years} года"
            
        # Считаем десятичную часть: месяцы / 12
        decimal_age = years + (months / 12)
        # Форматируем до 1 знака после запятой и меняем точку на запятую
        age_str = f"{decimal_age:.1f}".replace('.', ',')
        
        return f"{age_str} года"
    
    final_df['age'] = final_df['dob'].apply(calculate_age)
    
    # Расчет сообщения о ДР
    def calculate_birthday_message(row):
        dob_val = row['dob']
        child_name = str(row['child_name']) if pd.notna(row['child_name']) else ''
        
        if pd.isna(dob_val) or not dob_val:
            return ''
            
        from datetime import datetime, date, timedelta
        
        # Парсим дату
        birthdate = None
        if isinstance(dob_val, datetime):
            birthdate = dob_val.date()
        elif isinstance(dob_val, date):
            birthdate = dob_val
        elif isinstance(dob_val, str):
            for fmt in ['%Y-%m-%d', '%d.%m.%Y', '%d.%m.%y']:
                try:
                    birthdate = datetime.strptime(dob_val, fmt).date()
                    break
                except:
                    continue
        
        if not birthdate:
            return ''
            
        # Если имя ребенка не найдено, пробуем взять последнее слово из имени клиента (fallback)
        if not child_name:
            klient_name = str(row['klient_id'])
            child_name = klient_name.strip().split()[-1] if klient_name else ''
        
        # Месяц на русском
        months_ru = {
            1: 'января', 2: 'февраля', 3: 'марта', 4: 'апреля', 5: 'мая', 6: 'июня',
            7: 'июля', 8: 'августа', 9: 'сентября', 10: 'октября', 11: 'ноября', 12: 'декабря'
        }
        month_name = months_ru.get(birthdate.month, '')
        
        # Возраст в этом году
        today = datetime.now().date()
        turning_age = today.year - birthdate.year
        
        # Проверка интервала: Today - 3 <= Birthday <= Today + 7
        try:
            bday_this_year = birthdate.replace(year=today.year)
        except ValueError: # 29 февраля
            bday_this_year = birthdate.replace(year=today.year, day=28)
            
        candidates = [
            bday_this_year,
            bday_this_year.replace(year=today.year + 1),
            bday_this_year.replace(year=today.year - 1)
        ]
        
        is_in_range = False
        for bday in candidates:
            delta = (bday - today).days
            if -3 <= delta <= 7:
                is_in_range = True
                break
        
        if not is_in_range:
            return ''
        
        return f"{birthdate.day} {month_name} {child_name} отмечает ДР. Исполняется {turning_age}!"

    final_df['birthday_message'] = final_df.apply(calculate_birthday_message, axis=1)
    
    # Выбор и сортировка колонок
    # Порядок: Клиент, Мобильный, Тип, Категория, Сумма, Приобретено, Experience, Пропуски, Остаток, ДР, Возраст, Сообщение ДР
    cols = [
        'klient_id', 'mob', 'type', 'category',
        'total_summa', 'priobreteno', 'experience', 
        'propusk', 'loyalnyy_propusk', 'otmena_po_vine_tsentra', 'propusk_bez_spisaniya',
        'vozvrashcheno', 'pereraschet', 'podareno', 'sgorelo', 
        'ostatok',
        'dob', 'age', 'birthday_message'
    ]
    final_df = final_df[cols]
    
    # Сортировка: Клиент -> Тип -> Категория
    final_df = final_df.sort_values(['klient_id', 'type', 'category']).reset_index(drop=True)
    
    return final_df


def build_all_datamarts(engine: Engine) -> dict:
    """Построение всех витрин данных."""
    clients_info = get_clients_info(engine)
    sales = aggregate_client_sales(engine)
    trainings = aggregate_client_trainings(engine)
    balance = calculate_client_balance(sales, trainings, clients_info)
    
    return {
        'sales': sales,
        'trainings': trainings,
        'balance': balance
    }
