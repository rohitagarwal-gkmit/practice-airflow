-- =========================
-- TARGET SCHEMA
-- =========================

CREATE SCHEMA IF NOT EXISTS target;

CREATE TABLE IF NOT EXISTS target.categories (
    category_id     BIGSERIAL PRIMARY KEY,
    category_name   TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS target.products (
    product_id      BIGSERIAL PRIMARY KEY,
    title           TEXT NOT NULL,
    product_link    TEXT UNIQUE NOT NULL,
    upc             TEXT UNIQUE,
    product_type    TEXT,
    description     TEXT,
    category_id     BIGINT NOT NULL,

    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_category
        FOREIGN KEY (category_id)
        REFERENCES target.categories(category_id)
);

CREATE TABLE IF NOT EXISTS target.product_images (
    image_id        BIGSERIAL PRIMARY KEY,
    product_id      BIGINT NOT NULL,
    image_link      TEXT NOT NULL,

    CONSTRAINT fk_product_image
        FOREIGN KEY (product_id)
        REFERENCES target.products(product_id)
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS target.product_prices (
    price_id        BIGSERIAL PRIMARY KEY,
    product_id      BIGINT NOT NULL,

    price_excl_tax  NUMERIC(10, 2),
    price_incl_tax  NUMERIC(10, 2),
    tax             NUMERIC(10, 2),
    currency        CHAR(3) DEFAULT 'GBP',

    effective_from  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_product_price
        FOREIGN KEY (product_id)
        REFERENCES target.products(product_id)
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS target.product_inventory (
    inventory_id        BIGSERIAL PRIMARY KEY,
    product_id          BIGINT NOT NULL,
    availability_status TEXT NOT NULL,

    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_product_inventory
        FOREIGN KEY (product_id)
        REFERENCES target.products(product_id)
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS target.ratings (
    rating_id      BIGSERIAL PRIMARY KEY,
    product_id     BIGINT NOT NULL,
    rating_value   NUMERIC(2, 1),

    CONSTRAINT fk_product_rating
        FOREIGN KEY (product_id)
        REFERENCES target.products(product_id)
        ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS target.reviews_summary (
    review_id           BIGSERIAL PRIMARY KEY,
    product_id          BIGINT NOT NULL,
    number_of_reviews   INTEGER,

    CONSTRAINT fk_product_reviews
        FOREIGN KEY (product_id)
        REFERENCES target.products(product_id)
        ON DELETE CASCADE
);
