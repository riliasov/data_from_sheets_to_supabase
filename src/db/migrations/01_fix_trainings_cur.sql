-- Миграция: Расширение staging.trainings_cur
-- Причина: В источнике больше колонок, чем ожидалось (10 вместо 9)

DROP TABLE IF EXISTS staging.trainings_cur;

CREATE TABLE staging.trainings_cur (
    id SERIAL PRIMARY KEY,
    source_row_id INTEGER,
    row_hash VARCHAR(64),
    
    -- Генерируем с запасом до 20 колонок
    col_1 TEXT, col_2 TEXT, col_3 TEXT, col_4 TEXT, col_5 TEXT,
    col_6 TEXT, col_7 TEXT, col_8 TEXT, col_9 TEXT, col_10 TEXT,
    col_11 TEXT, col_12 TEXT, col_13 TEXT, col_14 TEXT, col_15 TEXT,
    col_16 TEXT, col_17 TEXT, col_18 TEXT, col_19 TEXT, col_20 TEXT,
    
    imported_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX idx_staging_trainings_cur_hash ON staging.trainings_cur(row_hash);
