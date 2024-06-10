---- Model events as facts ----
-- Create a table in ksqlDB to store the facts
CREATE TABLE items (
                       id BIGINT PRIMARY KEY,
                       price DECIMAL(10, 2),
                       name STRING,
                       description STRING,
                       brand_id BIGINT,
                       tax_status_id BIGINT
) WITH (
      KAFKA_TOPIC = 'items',
      VALUE_FORMAT = 'AVRO',
      PARTITIONS = 6
      );

-- Insert data into the stream backing the table.
INSERT INTO items (id, price, name, description, brand_id, tax_status_id)
VALUES (1, 9.99, 'Baseball Trading Cards', 'Premium Ol Slugger baseball trading cards!', 401, 778);
INSERT INTO items (id, price, name, description, brand_id, tax_status_id)
VALUES (2, .99, 'Football Trading Cards', 'Premium NFL 2022 football trading cards!', 402, 778);
INSERT INTO items (id, price, name, description, brand_id, tax_status_id)
VALUES (3, 19.99, 'Hockey Trading Cards', 'Premium NHL hockey trading cards!', 403, 778);
INSERT INTO items (id, price, name, description, brand_id, tax_status_id)
VALUES (4, 49.99, 'Basketball Trading Cards', 'Premium NBA basketball trading cards!', 404, 778);

-- Create a KTABLE from the topic to get the latest data for each item.
CREATE TABLE all_items WITH (
                           KAFKA_TOPIC = 'ksqdb_table_all_items',
                           PARTITIONS = 6,
                           REPLICAS = 3
                           ) AS
SELECT *
FROM items
WHERE true
    EMIT CHANGES;

-- Verify the contents of the all_items table:
SELECT * from all_items EMIT CHANGES;

-- Update the price of the baseball trading cards
INSERT INTO Items (id, price, name, description, brand_id, tax_status_id)
VALUES (1, 14.99, 'Baseball Trading Cards', 'Premium Ol Slugger baseball trading cards!', 401, 778);

-- Verify the all_items table reflects the updated price:
SELECT * from all_items EMIT CHANGES;
---------------------------------

---- Model events as deltas ----
-- Create and use a ksqlDB STREAM of item_added delta events
CREATE STREAM item_added (
  cart_id BIGINT key,
  item_id BIGINT
) WITH (
  KAFKA_TOPIC = 'item_added',
  VALUE_FORMAT = 'AVRO',
  PARTITIONS = 6
);

-- Create a KTABLE from the item_added stream to get the latest data for each cart:
CREATE TABLE items_per_cart AS
SELECT cart_id, COUNT(*) as items_in_cart
FROM item_added
GROUP BY cart_id
    EMIT CHANGES;

-- Insert data into the item_added stream to add items to several carts:
INSERT INTO item_added (cart_id, item_id) VALUES (200, 1);
INSERT INTO item_added (cart_id, item_id) VALUES (201, 4);
INSERT INTO item_added (cart_id, item_id) VALUES (202, 3);
INSERT INTO item_added (cart_id, item_id) VALUES (201, 2);
INSERT INTO item_added (cart_id, item_id) VALUES (200, 4);

-- Verify how many items are currently in each cart:
SELECT * FROM items_per_cart where cart_id=200 EMIT CHANGES;
---------------------------------