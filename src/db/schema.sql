-- Схема базы данных Planeta (v2)
-- Добавлено: Справочники, Валидация, Client ID на основе телефона

-- ============================================================================
-- 1. Схема RAW (Сырые данные) - без изменений
-- ============================================================================
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.sales (
    id SERIAL PRIMARY KEY,
    source VARCHAR(50),
    spreadsheet_id VARCHAR(100),
    sheet_id VARCHAR(100),
    row_number INTEGER,
    raw_data JSONB,
    imported_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.clients (
    id SERIAL PRIMARY KEY,
    source VARCHAR(50),
    spreadsheet_id VARCHAR(100),
    sheet_id VARCHAR(100),
    row_number INTEGER,
    raw_data JSONB,
    imported_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.expenses (
    id SERIAL PRIMARY KEY,
    source VARCHAR(50),
    spreadsheet_id VARCHAR(100),
    sheet_id VARCHAR(100),
    row_number INTEGER,
    raw_data JSONB,
    imported_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.trainings (
    id SERIAL PRIMARY KEY,
    source VARCHAR(50),
    spreadsheet_id VARCHAR(100),
    sheet_id VARCHAR(100),
    row_number INTEGER,
    raw_data JSONB,
    imported_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.rates (
    id SERIAL PRIMARY KEY,
    spreadsheet_id VARCHAR(100),
    sheet_id VARCHAR(100),
    row_number INTEGER,
    raw_data JSONB,
    imported_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.products (
    id SERIAL PRIMARY KEY,
    spreadsheet_id VARCHAR(100),
    sheet_id VARCHAR(100),
    row_number INTEGER,
    raw_data JSONB,
    imported_at TIMESTAMP DEFAULT NOW()
);


-- ============================================================================
-- 2. Схема REFERENCES (Справочники для валидации)
-- ============================================================================
CREATE SCHEMA IF NOT EXISTS "references";

-- Справочник сотрудников (Тренеры, Админы)
CREATE TABLE IF NOT EXISTS "references".employees (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) UNIQUE NOT NULL,  -- Эталонное имя
    role VARCHAR(50),                   -- 'trainer', 'admin', 'both'
    aliases TEXT[],                     -- Возможные варианты написания (синонимы)
    is_active BOOLEAN DEFAULT TRUE
);

-- Справочник продуктов (Типы абонементов)
CREATE TABLE IF NOT EXISTS "references".products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) UNIQUE NOT NULL,
    type VARCHAR(100),                  -- Бассейн, Ванны
    category VARCHAR(100),              -- Индивидуальный, Групповой
    aliases TEXT[],
    is_active BOOLEAN DEFAULT TRUE
);

-- Справочник категорий расходов
CREATE TABLE IF NOT EXISTS "references".expense_categories (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) UNIQUE NOT NULL,
    type VARCHAR(50),                   -- Постоянные, Переменные
    aliases TEXT[],
    is_active BOOLEAN DEFAULT TRUE
);

-- Таблица для "Карантина" (Неизвестные значения)
CREATE TABLE IF NOT EXISTS "references".unknown_values (
    id SERIAL PRIMARY KEY,
    entity_type VARCHAR(50),            -- 'employee', 'product', 'expense_category'
    raw_value TEXT,                     -- Значение из источника
    source_table VARCHAR(50),           -- Где встретилось (raw.sales и т.д.)
    row_id INTEGER,                     -- ID строки в raw таблице
    detected_at TIMESTAMP DEFAULT NOW(),
    resolution VARCHAR(50) DEFAULT 'pending' -- 'approved', 'mapped', 'ignored'
);


-- ============================================================================
-- 3. Схема NORMALIZED (Основные таблицы)
-- ============================================================================
CREATE SCHEMA IF NOT EXISTS normalized;

-- Клиенты
CREATE TABLE IF NOT EXISTS normalized.clients (
    id SERIAL PRIMARY KEY,
    
    -- Идентификаторы
    -- Формат: 79170000000_childname (транслит/lowercase)
    client_id VARCHAR(100) UNIQUE NOT NULL, 
    
    -- Основные данные
    mobile VARCHAR(20) NOT NULL,
    child_name VARCHAR(255) NOT NULL,
    child_birthdate DATE,
    
    -- Дополнительные данные
    assigned_trainer VARCHAR(100),      -- Ссылка на references.employees(name) ? Нет, лучше текстом, но валидированным
    source_origin VARCHAR(100),
    status VARCHAR(100),
    
    -- Финансы
    total_purchases NUMERIC(10,2) DEFAULT 0,
    balance NUMERIC(10,2) DEFAULT 0,
    
    -- Метаданные
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    deleted_at TIMESTAMP NULL,
    
    -- Constraint: Уникальная связка телефон + ребенок
    CONSTRAINT unique_mobile_child UNIQUE (mobile, child_name)
);

-- Продажи
CREATE TABLE IF NOT EXISTS normalized.sales (
    id SERIAL PRIMARY KEY,
    
    -- Связи
    client_id INTEGER REFERENCES normalized.clients(id),
    product_name VARCHAR(255),          -- Валидированное имя продукта
    
    -- Данные продажи
    sale_date DATE NOT NULL,
    amount NUMERIC(10,2),
    payment_type VARCHAR(50),
    
    -- Сотрудники (Валидированные имена)
    admin_name VARCHAR(100),
    trainer_name VARCHAR(100),
    
    -- Статус валидации
    validation_status VARCHAR(20) DEFAULT 'valid', -- 'valid', 'warning'
    
    -- Метаданные
    source VARCHAR(50),
    original_row_id INTEGER,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Тренировки
CREATE TABLE IF NOT EXISTS normalized.trainings (
    id SERIAL PRIMARY KEY,
    client_id INTEGER REFERENCES normalized.clients(id),
    
    training_date DATE NOT NULL,
    training_time TIME,
    status VARCHAR(50),
    
    trainer_name VARCHAR(100),          -- Валидированное имя
    
    validation_status VARCHAR(20) DEFAULT 'valid',
    
    source VARCHAR(50),
    original_row_id INTEGER,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

-- Расходы
CREATE TABLE IF NOT EXISTS normalized.expenses (
    id SERIAL PRIMARY KEY,
    expense_date DATE NOT NULL,
    amount NUMERIC(10,2),
    
    category VARCHAR(100),              -- Валидированная категория
    description TEXT,
    
    validation_status VARCHAR(20) DEFAULT 'valid',
    
    source VARCHAR(50),
    created_at TIMESTAMP DEFAULT NOW()
);

-- Ставки
CREATE TABLE IF NOT EXISTS normalized.rates (
    id SERIAL PRIMARY KEY,
    employee_name VARCHAR(100),
    rate_type VARCHAR(100),
    amount NUMERIC(10,2),
    valid_from DATE,
    created_at TIMESTAMP DEFAULT NOW()
);


-- ============================================================================
-- 4. Схема ANALYTICS (Представления)
-- ============================================================================
CREATE SCHEMA IF NOT EXISTS analytics;

CREATE OR REPLACE VIEW analytics.monthly_revenue AS
SELECT 
    DATE_TRUNC('month', sale_date)::DATE as month,
    SUM(amount) as total_revenue,
    COUNT(*) as sales_count
FROM normalized.sales
WHERE validation_status = 'valid'       -- Только валидные данные
GROUP BY 1
ORDER BY 1 DESC;

-- Индексы
CREATE INDEX IF NOT EXISTS idx_clients_mobile ON normalized.clients(mobile);
CREATE INDEX IF NOT EXISTS idx_clients_client_id ON normalized.clients(client_id);
CREATE INDEX IF NOT EXISTS idx_sales_date ON normalized.sales(sale_date);
