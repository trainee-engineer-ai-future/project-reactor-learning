DROP TABLE IF EXISTS order_items;
DROP TABLE IF EXISTS orders;
DROP TABLE IF EXISTS products;

CREATE TABLE products (
    id          BIGSERIAL PRIMARY KEY,
    name        VARCHAR(255) NOT NULL,
    price       NUMERIC(12, 2) NOT NULL,
    stock       INT NOT NULL CHECK (stock >= 0)
);

CREATE TABLE orders (
    id          BIGSERIAL PRIMARY KEY,
    user_id     BIGINT NOT NULL,
    status      VARCHAR(32) NOT NULL,
    total       NUMERIC(12, 2) NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE order_items (
    id          BIGSERIAL PRIMARY KEY,
    order_id    BIGINT NOT NULL REFERENCES orders(id),
    product_id  BIGINT NOT NULL REFERENCES products(id),
    quantity    INT NOT NULL CHECK (quantity > 0),
    price       NUMERIC(12, 2) NOT NULL
);

INSERT INTO products (name, price, stock) VALUES
    ('Keyboard', 59.99, 100),
    ('Mouse', 29.99, 150),
    ('Monitor', 199.99, 40),
    ('USB Cable', 9.99, 300),
    ('Headset', 79.99, 60);
