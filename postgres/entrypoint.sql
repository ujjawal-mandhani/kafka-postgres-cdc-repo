CREATE TABLE if not exists customers (
	id int GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
	name text
);

ALTER TABLE
	customers REPLICA IDENTITY USING INDEX customers_pkey;

INSERT INTO customers (name)
SELECT 'MANDHANI'
WHERE NOT EXISTS (
    SELECT 1
    FROM customers
    WHERE name = 'MANDHANI'
);
