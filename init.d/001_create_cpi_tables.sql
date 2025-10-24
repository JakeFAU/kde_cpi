CREATE TABLE IF NOT EXISTS cpi_area (
    area_code TEXT PRIMARY KEY,
    area_name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS cpi_item (
    item_code TEXT PRIMARY KEY,
    item_name TEXT NOT NULL,
    display_level INTEGER NOT NULL,
    selectable BOOLEAN NOT NULL,
    sort_sequence INTEGER NOT NULL
);

ALTER TABLE cpi_item
    ADD COLUMN IF NOT EXISTS display_level INTEGER NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS selectable BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS sort_sequence INTEGER NOT NULL DEFAULT 0;

ALTER TABLE cpi_item
    ALTER COLUMN display_level DROP DEFAULT,
    ALTER COLUMN selectable DROP DEFAULT,
    ALTER COLUMN sort_sequence DROP DEFAULT;

CREATE TABLE IF NOT EXISTS cpi_period (
    period_code TEXT PRIMARY KEY,
    period_abbr TEXT NOT NULL,
    period_name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS cpi_footnote (
    footnote_code TEXT PRIMARY KEY,
    footnote_text TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS cpi_series (
    series_id TEXT PRIMARY KEY,
    area_code TEXT NOT NULL REFERENCES cpi_area (area_code),
    item_code TEXT NOT NULL REFERENCES cpi_item (item_code),
    seasonal TEXT NOT NULL,
    periodicity_code TEXT NOT NULL,
    base_code TEXT NOT NULL,
    base_period TEXT NOT NULL,
    begin_year INTEGER NOT NULL,
    begin_period TEXT NOT NULL,
    end_year INTEGER NOT NULL,
    end_period TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS cpi_observation (
    series_id TEXT NOT NULL REFERENCES cpi_series (series_id) ON DELETE CASCADE,
    year INTEGER NOT NULL,
    period TEXT NOT NULL,
    value NUMERIC,
    footnotes TEXT[],
    PRIMARY KEY (series_id, year, period)
);

CREATE INDEX IF NOT EXISTS idx_cpi_item_display_level ON cpi_item (display_level);
CREATE INDEX IF NOT EXISTS idx_cpi_series_item_code ON cpi_series (item_code);
CREATE INDEX IF NOT EXISTS idx_cpi_observation_series_period ON cpi_observation (series_id, year, period);
CREATE INDEX IF NOT EXISTS idx_cpi_observation_year_period ON cpi_observation (year, period);
