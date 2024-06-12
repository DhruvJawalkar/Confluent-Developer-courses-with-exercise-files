-- In this exercise we merge multiple event types (item_added and item_removed) into a single stream (merged_cart_actions) and finally group events by (cart_id, item_id) and aggregate an overall count
-- Create the item_added stream:
CREATE STREAM item_added (
  cart_id BIGINT key,
  item_id BIGINT
) WITH (
  KAFKA_TOPIC = 'item_added',
  VALUE_FORMAT = 'AVRO',
  PARTITIONS = 6);

-- Create the item_removed stream:
CREATE STREAM item_removed (
  cart_id BIGINT key,
  item_id BIGINT
) WITH (
  KAFKA_TOPIC = 'item_removed',
  VALUE_FORMAT = 'AVRO',
  PARTITIONS = 6);

-- Create the merged_cart_actions stream:
CREATE STREAM merged_cart_actions (
  cart_id BIGINT key,
  item_id BIGINT,
  action STRING
) WITH (
  KAFKA_TOPIC = 'merged_cart_actions',
  VALUE_FORMAT = 'AVRO',
  PARTITIONS = 6);

-- Merge the events from the item_added into the new merged_cart_actions stream using a simple streaming processor for each ADD.
INSERT INTO merged_cart_actions
SELECT cart_id, item_id, 'ADD' AS action FROM item_added;

-- Merge the events from the item_removed into the merged_cart_actions stream using a simple streaming processor for each REMOVE.
INSERT INTO merged_cart_actions
SELECT cart_id, item_id, 'REMOVE' AS action FROM item_removed;

-- Create a table of (cart, item) tuples, and aggregate up the ADD and REMOVE actions into a list.
CREATE TABLE item_cart_actions
    WITH (KEY_FORMAT='JSON', VALUE_FORMAT='AVRO')
AS SELECT cart_id, item_id, collect_list(action) AS actions
   FROM merged_cart_actions
   GROUP BY (cart_id, item_id)
       EMIT CHANGES;

-- Reducing the list down to a numeric quantity, while grouping the items and the final quantity of those items together into a single STRUCT
CREATE TABLE shopping_cart AS
SELECT cart_id,
       collect_list( STRUCT(
               item_id := item_id,
               quantity := REDUCE ( actions, 0, (s, x) => case
           when x = 'ADD' then s + 1
           when x = 'REMOVE' then s - 1 end )))
           as basket
FROM item_cart_actions
GROUP BY cart_id
    EMIT CHANGES;

-- Inserting sample data into the tables.
INSERT INTO item_added (cart_id, item_id) VALUES (1234, 200);
INSERT INTO item_added (cart_id, item_id) VALUES (1234, 200);
INSERT INTO item_added (cart_id, item_id) VALUES (1234, 200);
INSERT INTO item_removed (cart_id, item_id) VALUES (1234, 200);
INSERT INTO item_added (cart_id, item_id) VALUES (1234, 400);

-- Query the shopping_cart and view the results
SELECT * FROM shopping_cart EMIT CHANGES;