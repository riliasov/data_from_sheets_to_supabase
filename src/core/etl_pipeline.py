from abc import ABC, abstractmethod
from typing import Dict, List, Optional
import sqlalchemy
from src.etl.loader import DataLoader
from src.etl.data_cleaner import clean_dataframe
from src.core.sheets_processor import SheetsProcessor
from src.logger import get_logger

class ETLPipeline(ABC):
    """Базовый класс для ETL пайплайнов."""
    
    def __init__(self, config: Dict, engine: sqlalchemy.Engine):
        self.config = config
        self.engine = engine
        self.loader = DataLoader(engine)
        self.sheets_processor = SheetsProcessor(config)
        self.logger = get_logger(self.__class__.__name__)
    
    @abstractmethod
    def get_source_mapping(self) -> Dict[str, str]:
        """Возвращает маппинг source_name -> target_table."""
        pass
    
    @abstractmethod
    def get_column_mappings(self) -> Dict[str, Dict[str, str]]:
        """Возвращает маппинг колонок для каждой таблицы."""
        pass
    
    def run(self):
        """Запускает пайплайн."""
        self.logger.info(f"🚀 Запуск {self.__class__.__name__}")
        
        sources = self.config.get('SOURCES', {})
        source_mapping = self.get_source_mapping()
        
        for source_name, target_table in source_mapping.items():
            if source_name in sources:
                self._process_source(
                    sources[source_name],
                    source_name,
                    target_table
                )
    
    def _process_source(self, source_config: Dict, source_name: str, target_table: str):
        """Обрабатывает один источник данных."""
        df = self.sheets_processor.read_and_transform(
            source_config,
            target_table,
            self.get_column_mappings().get(target_table, {})
        )
        
        if df is not None and not df.empty:
            df_cleaned = clean_dataframe(df, target_table)
            self.loader.load_staging(df_cleaned, target_table, source_name)
