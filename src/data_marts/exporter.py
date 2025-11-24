"""
Exporter - Экспорт витрин данных в Google Sheets.
"""
import pandas as pd
import gspread
import json
from src.logger import get_logger

logger = get_logger(__name__)
from typing import Optional


def export_dataframe_to_sheet(
    gc: gspread.Client,
    df: pd.DataFrame,
    spreadsheet_id: str,
    gid: str,
    clear_first: bool = True
) -> None:
    """
    Экспортирует pandas DataFrame в Google Sheets по gid.
    
    Args:
        gc: Google Sheets client
        df: DataFrame для экспорта
        spreadsheet_id: ID целевой таблицы
        gid: GID целевого листа
        clear_first: Очищать диапазон данных перед записью
    """
    # Открываем spreadsheet
    spreadsheet = gc.open_by_key(spreadsheet_id)
    
    # Находим worksheet по gid
    worksheet = None
    for ws in spreadsheet.worksheets():
        if str(ws.id) == str(gid):
            worksheet = ws
            break
    
    if worksheet is None:
        raise ValueError(f"Лист с gid={gid} не найден в spreadsheet {spreadsheet_id}")
    
    # Get worksheet
    
    # Подготовка данных: headers + rows
    df_export = df.copy()
    
    # Обрабатываем мобильные номера - оставляем как есть (число)
    # Google Sheets сам решит как отображать
    if 'mob' in df_export.columns:
        df_export['mob'] = df_export['mob'].fillna('')
    
    # Обрабатываем даты - конвертируем в формат DD.MM.YYYY (строка)
    for col in df_export.columns:
        if pd.api.types.is_datetime64_any_dtype(df_export[col]):
            df_export[col] = df_export[col].dt.strftime('%d.%m.%Y')
        elif df_export[col].dtype == 'object' and col == 'dob':
            df_export[col] = df_export[col].apply(
                lambda x: x.strftime('%d.%m.%Y') if hasattr(x, 'strftime') else (str(x) if pd.notna(x) else '')
            )

    values = [df_export.columns.tolist()] + df_export.values.tolist()
    
    # Определяем диапазон для обновления (только колонки с данными)
    num_cols = len(df.columns)
    num_rows = len(df) + 1  # +1 для заголовка
    
    # Преобразуем число колонок в букву (A=1, B=2, ..., Z=26, AA=27, etc)
    def col_num_to_letter(n):
        result = ''
        while n > 0:
            n -= 1
            result = chr(65 + (n % 26)) + result
            n //= 26
        return result
    
    last_col = col_num_to_letter(num_cols)
    range_name = f'A1:{last_col}{num_rows}'
    
    # Очистка только диапазона с данными (не весь лист, не трогаем формулы справа)
    if clear_first:
        worksheet.batch_clear([range_name])
    
    # Update cells
    # USER_ENTERED позволяет Google Sheets интерпретировать данные (числа как числа, даты как даты)
    worksheet.update(range_name, values, value_input_option='USER_ENTERED')
    


def export_balance_to_sheets(
    gc: gspread.Client,
    balance_df: pd.DataFrame,
    spreadsheet_id: str = "1-kEt2r-mzqI6PmtFqcFaS7XVAPdlde5FxYMv4DXwd94",
    gid: str = "1868616984"
) -> None:
    """
    Экспортирует витрину баланса клиентов в Google Sheets.
    
    Args:
        gc: Google Sheets client
        balance_df: DataFrame с балансом
        spreadsheet_id: ID целевой таблицы (по умолчанию из sources.json)
        gid: GID целевого листа
    """
    export_dataframe_to_sheet(gc, balance_df, spreadsheet_id, gid)


def export_all_datamarts(
    gc: gspread.Client,
    datamarts: dict,
    spreadsheet_id: str = "1-kEt2r-mzqI6PmtFqcFaS7XVAPdlde5FxYMv4DXwd94",
    balance_gid: str = "1868616984"
) -> None:
    """
    Экспортирует все витрины в Google Sheets.
    
    Args:
        gc: Google Sheets client  
        datamarts: dict с ключами 'sales', 'trainings', 'balance'
        spreadsheet_id: ID целевой таблицы
        balance_gid: GID для листа с балансом
    """
    # Экспорт balance
    export_balance_to_sheets(
        gc,
        datamarts['balance'],
        spreadsheet_id,
        balance_gid
    )
