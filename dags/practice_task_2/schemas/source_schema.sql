-- =========================
-- SOURCE SCHEMA
-- =========================

CREATE SCHEMA IF NOT EXISTS source;

CREATE TABLE IF NOT EXISTS source.books (
    id                  BIGSERIAL PRIMARY KEY,
    product_link        TEXT NOT NULL,
    image_link          TEXT,
    category            TEXT,
    title               TEXT,
    price               NUMERIC(10, 2),
    availability        TEXT,
    rating              NUMERIC(2, 1),
    description         TEXT,
    upc                 TEXT,
    product_type        TEXT,
    price_excl_tax      NUMERIC(10, 2),
    price_incl_tax      NUMERIC(10, 2),
    tax                 NUMERIC(10, 2),
    number_of_reviews   INTEGER,
);
