-- Финальная схема базы данных Planeta (v3)
-- Особенности:
-- 1. Схемы: raw, staging, references, core, analytics
-- 2. Таблицы staging сгруппированы по сущностям (sales_hst, sales_cur)
-- 3. Инкрементальное обновление через row_hash

-- ============================================================================
-- 1. Схема RAW (Сырые данные - JSONB)
-- ============================================================================
CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.sales_hst (
    id SERIAL PRIMARY KEY,
    spreadsheet_id VARCHAR(100),
    sheet_id VARCHAR(100),
    row_number INTEGER,
    raw_data JSONB,
    imported_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.sales_cur (
    id SERIAL PRIMARY KEY,
    spreadsheet_id VARCHAR(100),
    sheet_id VARCHAR(100),
    row_number INTEGER,
    raw_data JSONB,
    imported_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.clients_hst (
    id SERIAL PRIMARY KEY,
    spreadsheet_id VARCHAR(100),
    sheet_id VARCHAR(100),
    row_number INTEGER,
    raw_data JSONB,
    imported_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.expenses_hst (
    id SERIAL PRIMARY KEY,
    spreadsheet_id VARCHAR(100),
    sheet_id VARCHAR(100),
    row_number INTEGER,
    raw_data JSONB,
    imported_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.expenses_cur (
    id SERIAL PRIMARY KEY,
    spreadsheet_id VARCHAR(100),
    sheet_id VARCHAR(100),
    row_number INTEGER,
    raw_data JSONB,
    imported_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.trainings_hst (
    id SERIAL PRIMARY KEY,
    spreadsheet_id VARCHAR(100),
    sheet_id VARCHAR(100),
    row_number INTEGER,
    raw_data JSONB,
    imported_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw.trainings_cur (
    id SERIAL PRIMARY KEY,
    spreadsheet_id VARCHAR(100),
    sheet_id VARCHAR(100),
    row_number INTEGER,
    raw_data JSONB,
    imported_at TIMESTAMP DEFAULT NOW()
);


-- ============================================================================
-- 2. Схема STAGING (Типизированные данные + row_hash)
-- ============================================================================
CREATE SCHEMA IF NOT EXISTS staging;

-- Продажи (История)
CREATE TABLE IF NOT EXISTS staging.sales_hst (
    id SERIAL PRIMARY KEY,
    source_row_id INTEGER,
    row_hash VARCHAR(64),          -- Для инкрементального обновления
    
    data                           DATE,
    klient                         TEXT,
    produkt                        TEXT,
    tip                            TEXT,
    kategoriya                     TEXT,
    kolichestvo                    INTEGER,
    polnaya_stoimost               INTEGER,
    skidka                         TEXT,
    okonchatelnaya_stoimost        INTEGER,
    nalichnye                      INTEGER,
    perevod                        INTEGER,
    terminal                       INTEGER,
    vdolg                          INTEGER,
    admin                          TEXT,
    trener                         TEXT,
    kommentariy                    TEXT,
    bonus_admina                   INTEGER,
    bonus_trenera                  INTEGER,
    
    imported_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX idx_staging_sales_hst_hash ON staging.sales_hst(row_hash);

-- Продажи (Текущие)
CREATE TABLE IF NOT EXISTS staging.sales_cur (
    id SERIAL PRIMARY KEY,
    source_row_id INTEGER,
    row_hash VARCHAR(64),
    
    data                           TEXT, -- В текущих дата может быть текстом
    klient                         TEXT,
    produkt                        TEXT,
    tip                            TEXT,
    kategoriya                     TEXT,
    kolichestvo                    INTEGER,
    polnaya_stoimost               INTEGER,
    skidka                         TEXT,
    okonchatelnaya_stoimost        INTEGER,
    nalichnye                      INTEGER,
    perevod                        INTEGER,
    terminal                       INTEGER,
    vdolg                          INTEGER,
    admin                          TEXT,
    trener                         TEXT,
    kommentariy                    TEXT,
    bonus_admina                   INTEGER,
    bonus_trenera                  INTEGER,
    probili_na_evotore             BOOLEAN,
    vnesli_v_crm                   BOOLEAN,
    
    imported_at TIMESTAMP DEFAULT NOW()
);
CREATE INDEX idx_staging_sales_cur_hash ON staging.sales_cur(row_hash);

-- Клиенты
CREATE TABLE IF NOT EXISTS staging.clients_hst (
    id SERIAL PRIMARY KEY,
    source_row_id INTEGER,
    row_hash VARCHAR(64),
    
    klient                         TEXT,
    data_obrascheniya              DATE,
    mobilnyy                       TEXT,
    zapros_pri_obraschenii         TEXT,
    kto_vnyos_informatsiyu_ob_obraschenii TEXT,
    familiya_vzroslogo             TEXT,
    imya_vzroslogo                 TEXT,
    imya_rebenka                   TEXT,
    data_rozhdeniya_rebenka        DATE,
    pol_rebyonka                   TEXT,
    tip                            TEXT,
    kto_sozdal                     TEXT,
    zapis_na                       DATE,
    istochnik                      TEXT,
    kommentariy_pri_zapisi         TEXT,
    kto_zapisal                    TEXT,
    tsena_probnogo                 TEXT,
    kto_oformil_prodazhu_probnogo  TEXT,
    instruktor                     TEXT,
    kommentariy_posle_probnogo     TEXT,
    priobretennyy_abonement        TEXT,
    admin_v_den_vizita             TEXT,
    
    imported_at TIMESTAMP DEFAULT NOW()
);

-- Расходы (История)
CREATE TABLE IF NOT EXISTS staging.expenses_hst (
    id SERIAL PRIMARY KEY,
    source_row_id INTEGER,
    row_hash VARCHAR(64),
    
    god                            INTEGER,
    mesyats                        INTEGER,
    mesyats_1                      INTEGER,
    uch_mesyats                    TEXT,
    data                           DATE,
    summa                          INTEGER,
    tip_zatrat                     TEXT,
    kategoriya_zatrat              TEXT,
    sotrudnik_kontragent           TEXT,
    opisanieperiod_naimenovanie_kolichestvo TEXT,
    oplacheno                      TEXT,
    klient                         TEXT,
    raspredelenie                  TEXT,
    relevant                       BOOLEAN,
    
    imported_at TIMESTAMP DEFAULT NOW()
);

-- Расходы (Текущие)
CREATE TABLE IF NOT EXISTS staging.expenses_cur (
    id SERIAL PRIMARY KEY,
    source_row_id INTEGER,
    row_hash VARCHAR(64),
    
    god                            INTEGER,
    mesyats                        INTEGER,
    mesyats_1                      INTEGER,
    uch_mesyats                    TEXT,
    data                           DATE,
    summa                          INTEGER,
    tip_zatrat                     TEXT,
    kategoriya_zatrat              TEXT,
    sotrudnik_kontragent           TEXT,
    opisanieperiod_naimenovanie_kolichestvo TEXT,
    oplacheno                      TEXT,
    klient                         TEXT,
    raspredelenie                  TEXT,
    relevant                       BOOLEAN,
    
    imported_at TIMESTAMP DEFAULT NOW()
);

-- Тренировки (История)
CREATE TABLE IF NOT EXISTS staging.trainings_hst (
    id SERIAL PRIMARY KEY,
    source_row_id INTEGER,
    row_hash VARCHAR(64),
    
    data                           DATE,
    nachalo                        TEXT,
    konets                         TEXT,
    sotrudnik                      TEXT,
    klient                         TEXT,
    status                         TEXT,
    tip                            TEXT,
    kategoriya                     TEXT,
    zamena                         BOOLEAN,
    kommentariy                    TEXT,
    chasy                          NUMERIC(10,2),
    kolichestvo                    NUMERIC(10,2),
    spisano                        INTEGER,
    oplata                         INTEGER,
    stavka                         INTEGER,
    stavka_na_zamene               INTEGER,
    stavka_propusk                 INTEGER,
    zp                             INTEGER,
    
    imported_at TIMESTAMP DEFAULT NOW()
);

-- Тренировки (Текущие) - структура отличается от истории (меньше полей)
CREATE TABLE IF NOT EXISTS staging.trainings_cur (
    id SERIAL PRIMARY KEY,
    source_row_id INTEGER,
    row_hash VARCHAR(64),
    
    -- Поля из current_trainings (нужно уточнить, если они отличаются)
    -- Пока берем базовые, предполагая схожесть, или используем JSONB если структура плавает
    -- В отчете было 9 колонок.
    -- Заголовки были: сб 01.11, 14:00, 17:30, Алмаз, Администратор...
    -- Это требует отдельного маппинга, пока создадим generic поля
    col_1 TEXT, col_2 TEXT, col_3 TEXT, col_4 TEXT, col_5 TEXT,
    col_6 TEXT, col_7 TEXT, col_8 TEXT, col_9 TEXT,
    
    imported_at TIMESTAMP DEFAULT NOW()
);


-- ============================================================================
-- 3. Схема REFERENCES (Справочники)
-- ============================================================================
CREATE SCHEMA IF NOT EXISTS "references";

CREATE TABLE IF NOT EXISTS "references".employees (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) UNIQUE NOT NULL,
    role VARCHAR(50),
    aliases TEXT[],
    is_active BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS "references".products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) UNIQUE NOT NULL,
    type VARCHAR(100),
    category VARCHAR(100),
    aliases TEXT[],
    is_active BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS "references".expense_categories (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) UNIQUE NOT NULL,
    type VARCHAR(50),
    aliases TEXT[],
    is_active BOOLEAN DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS "references".unknown_values (
    id SERIAL PRIMARY KEY,
    entity_type VARCHAR(50),
    raw_value TEXT,
    source_table VARCHAR(50),
    row_id INTEGER,
    detected_at TIMESTAMP DEFAULT NOW(),
    resolution VARCHAR(50) DEFAULT 'pending'
);


-- ============================================================================
-- 4. Схема CORE (Нормализованные данные)
-- ============================================================================
CREATE SCHEMA IF NOT EXISTS core;

CREATE TABLE IF NOT EXISTS core.clients (
    id SERIAL PRIMARY KEY,
    client_id VARCHAR(100) UNIQUE NOT NULL, -- mobile + child_name
    mobile VARCHAR(20),
    child_name VARCHAR(255),
    child_birthdate DATE,
    assigned_trainer VARCHAR(100),
    status VARCHAR(50),
    balance NUMERIC(10,2) DEFAULT 0,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS core.sales (
    id SERIAL PRIMARY KEY,
    client_id INTEGER REFERENCES core.clients(id),
    sale_date DATE NOT NULL,
    product_name VARCHAR(255),
    amount NUMERIC(10,2),
    payment_type VARCHAR(50),
    trainer_name VARCHAR(100),
    admin_name VARCHAR(100),
    source VARCHAR(50), -- 'hst' or 'cur'
    validation_status VARCHAR(20),
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS core.expenses (
    id SERIAL PRIMARY KEY,
    expense_date DATE NOT NULL,
    amount NUMERIC(10,2),
    category VARCHAR(100),
    description TEXT,
    source VARCHAR(50),
    validation_status VARCHAR(20),
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS core.trainings (
    id SERIAL PRIMARY KEY,
    client_id INTEGER REFERENCES core.clients(id),
    training_date DATE NOT NULL,
    training_time TIME,
    trainer_name VARCHAR(100),
    status VARCHAR(50),
    source VARCHAR(50),
    validation_status VARCHAR(20),
    created_at TIMESTAMP DEFAULT NOW()
);


-- ============================================================================
-- 5. Схема ANALYTICS (Представления)
-- ============================================================================
CREATE SCHEMA IF NOT EXISTS analytics;

CREATE OR REPLACE VIEW analytics.monthly_revenue AS
SELECT 
    DATE_TRUNC('month', sale_date)::DATE as month,
    SUM(amount) as total_revenue,
    COUNT(*) as sales_count
FROM core.sales
WHERE validation_status = 'valid'
GROUP BY 1
ORDER BY 1 DESC;
