import sys
import os
import sqlalchemy
from sqlalchemy import text

# Добавляем корень проекта в путь
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.config import load_config

def test_connection():
    print("🔌 Проверка подключения к базе данных Supabase...")
    
    config = load_config()
    db_url = config.get('SUPABASE_DB_URL')
    
    if not db_url:
        print("❌ ОШИБКА: Переменная SUPABASE_DB_URL не найдена в конфигурации (.env).")
        print("   Убедитесь, что в файле .env есть строка:")
        print("   SUPABASE_DB_URL=postgresql://postgres:[PASSWORD]@db.[PROJECT].supabase.co:5432/postgres")
        return False

    # Маскируем пароль для вывода
    masked_url = db_url
    if '@' in db_url:
        part1 = db_url.split('@')[0]
        part2 = db_url.split('@')[1]
        if ':' in part1:
            user = part1.split(':')[0] # postgresql://user
            masked_url = f"{user}:****@{part2}"
    
    print(f"   URL: {masked_url}")

    try:
        # Анализ порта для рекомендаций по пулингу
        port = '5432'
        if ':' in db_url.split('/')[-1]: # Простая проверка, может быть неточной если есть спецсимволы в пароле
             pass # Сложно распарсить без urlparse, но попробуем найти порт
        
        if ':6543' in db_url:
            port = '6543'
            print("ℹ️  Обнаружен порт 6543 (Transaction Pooler).")
            print("   ⚠️  Внимание: Для выполнения миграций (создания таблиц) рекомендуется использовать порт 5432 (Session Mode).")
            print("   Для работы ETL скриптов порт 6543 подходит отлично.")
        elif ':5432' in db_url:
            print("ℹ️  Обнаружен порт 5432 (Session Mode / Direct).")
            print("   ✅ Отлично подходит для создания таблиц (DDL).")
            print("   ℹ️  Для высоконагруженного ETL рекомендуется использовать Transaction Pooler (порт 6543).")
        
        # Попытка 1: Прямое подключение (как в конфиге)
        print(f"\n🔄 Попытка подключения к {masked_url}...")
        try:
            engine = sqlalchemy.create_engine(db_url, connect_args={"connect_timeout": 5})
            with engine.connect() as connection:
                result = connection.execute(text("SELECT version();"))
                version = result.fetchone()[0]
                print(f"✅ УСПЕХ! Подключение установлено (Порт {port}).")
                print(f"   Версия: {version}")
                return True
        except Exception as e:
            print(f"❌ Не удалось подключиться: {e}")
            
        # Попытка 2: Если был порт 5432, пробуем 6543 (Pooler)
        if port == '5432':
            print(f"\n🔄 Пробуем через Transaction Pooler (порт 6543)...")
            pooler_url = db_url.replace(':5432', ':6543')
            try:
                engine = sqlalchemy.create_engine(pooler_url, connect_args={"connect_timeout": 5})
                with engine.connect() as connection:
                    result = connection.execute(text("SELECT version();"))
                    version = result.fetchone()[0]
                    print(f"✅ УСПЕХ! Подключение через Pooler (6543) работает.")
                    print(f"   Версия: {version}")
                    print("   💡 РЕКОМЕНДАЦИЯ: Используйте порт 6543 в .env для ETL задач.")
                    return True
            except Exception as e:
                print(f"❌ Не удалось подключиться через Pooler: {e}")

        # Попытка 3: DNS Debug
        print(f"\n🔍 Диагностика DNS:")
        import socket
        hostname = db_url.split('@')[1].split(':')[0]
        try:
            ip_list = socket.getaddrinfo(hostname, None)
            for item in ip_list:
                family = "IPv6" if item[0] == socket.AF_INET6 else "IPv4"
                print(f"   - {family}: {item[4][0]}")
        except Exception as e:
            print(f"   ❌ Ошибка DNS: {e}")

        return False
    except Exception as e:
        print(f"❌ КРИТИЧЕСКАЯ ОШИБКА СКРИПТА: {e}")
        return False

if __name__ == "__main__":
    test_connection()
