CREATE SCHEMA IF NOT EXISTS staging;

-- Таблица для historical_sales
CREATE TABLE IF NOT EXISTS staging.historical_sales (
    id SERIAL PRIMARY KEY,
    source_row_id INTEGER,
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

-- Таблица для clients_data
CREATE TABLE IF NOT EXISTS staging.clients_data (
    id SERIAL PRIMARY KEY,
    source_row_id INTEGER,
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

-- Таблица для historical_expenses
CREATE TABLE IF NOT EXISTS staging.historical_expenses (
    id SERIAL PRIMARY KEY,
    source_row_id INTEGER,
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

-- Таблица для historical_trainings
CREATE TABLE IF NOT EXISTS staging.historical_trainings (
    id SERIAL PRIMARY KEY,
    source_row_id INTEGER,
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

-- Таблица для current_sales
CREATE TABLE IF NOT EXISTS staging.current_sales (
    id SERIAL PRIMARY KEY,
    source_row_id INTEGER,
    data                           TEXT,
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

-- Таблица для current_expenses
CREATE TABLE IF NOT EXISTS staging.current_expenses (
    id SERIAL PRIMARY KEY,
    source_row_id INTEGER,
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

