"""
Пайплайн для синхронизации справочников.
Источники:
- references (Прайс)
- rates (Ставки)

Режим: Full Reload.
"""
from src.logger import get_logger

logger = get_logger(__name__)


def run_references_sync():
    logger.info("Запуск синхронизации справочников...")
    # TODO: Реализовать логику
    pass
