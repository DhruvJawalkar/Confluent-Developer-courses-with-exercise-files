-- Create the brands table: (topic with brand information)
CREATE TABLE brands (
                        id BIGINT PRIMARY KEY,
                        name STRING
) WITH (
      KAFKA_TOPIC = 'brands',
      VALUE_FORMAT = 'AVRO',
      PARTITIONS = 6
      );

-- Create the tax_status table: (topic with tax_status information)
CREATE TABLE tax_status (
                            id BIGINT PRIMARY KEY,
                            state_tax DECIMAL(3, 2),
                            country_tax DECIMAL(3, 2)
) WITH (
      KAFKA_TOPIC = 'tax_status',
      VALUE_FORMAT = 'AVRO',
      PARTITIONS = 6
      );

-- Create an items_dim2 table: (topic with item information containing foreign key (normalized) reference to brand and tax_status)
CREATE TABLE items_dim2 (
                            id BIGINT PRIMARY KEY,
                            price DECIMAL(10, 2),
                            name STRING,
                            description STRING,
                            brand_id BIGINT,
                            tax_status_id BIGINT
) WITH (
      KAFKA_TOPIC = 'items_dim2',
      VALUE_FORMAT = 'AVRO',
      PARTITIONS = 6
      );

-- Create a new items_and_brands table by joining the items_dim2 table with the brands table. (topic with item information with denormalized information about brands)
CREATE TABLE items_and_brands AS
SELECT items_dim2.id AS id,
       items_dim2.price,
       items_dim2.name AS name,
       items_dim2.description,
       brands.name AS brand_name,
       items_dim2.tax_status_id
FROM items_dim2
         JOIN brands ON brand_id = brands.id
    EMIT CHANGES;

-- Create a new enriched_items table by joining the items_and_brands table with the tax_status table. (topic with item information with denormalized information from both brands and tax_status)
CREATE TABLE enriched_items AS
SELECT items_and_brands.id AS id,
       items_and_brands.price,
       items_and_brands.name,
       items_and_brands.description,
       items_and_brands.brand_name,
       tax_status.state_tax,
       tax_status.country_tax
FROM items_and_brands
         JOIN tax_status ON items_and_brands.tax_status_id = tax_status.id
    EMIT CHANGES;

-- Insert sample data in the brands and tax_status tables:
INSERT INTO brands (id, name) VALUES (400, 'ACME');
INSERT INTO tax_status (id, state_tax, country_tax) VALUES (777, 0.05, 0.10);

-- Insert a matching item into the items_dim2 table: (normalized item fact)
INSERT INTO items_dim2 (id, price, name, description, brand_id, tax_status_id)
VALUES (321, 29.99, 'Anvil', 'Sturdy Iron Anvil, 400 lbs', 400, 777);

-- You will now see an enriched join result in the enriched_items table:
SELECT * FROM enriched_items EMIT CHANGES;

-- Create three items of the ACME brand:
INSERT INTO items_dim2 (id, price, name, description, brand_id, tax_status_id)
VALUES (9000, 19.99, 'Ball', 'Rubber Ball', 400, 777);
INSERT INTO items_dim2 (id, price, name, description, brand_id, tax_status_id)
VALUES (9001, 39.99, 'Baseball Glove', 'Leather Baseball Glove', 400, 777);
INSERT INTO items_dim2 (id, price, name, description, brand_id, tax_status_id)
VALUES (9002, 2.99, 'Tennis Ball', 'Green Tennis Ball', 400, 777);

-- Change the brand from ACME to ACME Sports: (As a result of the join tables being a push query, changes to a row in brand table get propagated downstream to enriched_items table with updated info)
INSERT INTO brands (id, name) VALUES (400, 'ACME Sports');

-- Verify that all items were updated with the new brand:
SELECT * FROM enriched_items EMIT CHANGES;
