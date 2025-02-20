--makes no sense for iceberg tables for this to be 'true':
set hive.metastore.disallow.incompatible.col.type.changes=false;

--test against vectorized LLAP execution mode
set hive.llap.io.enabled=true;
set hive.vectorized.execution.enabled=true;

DROP TABLE IF EXISTS llap_orders PURGE;
DROP TABLE IF EXISTS llap_items PURGE;


CREATE EXTERNAL TABLE llap_items (itemid INT, price INT, category STRING, name STRING, description STRING) STORED BY ICEBERG STORED AS ORC;
INSERT INTO llap_items VALUES
(0, 35000,  'Sedan',     'Model 3', 'Standard range plus'),
(1, 45000,  'Sedan',     'Model 3', 'Long range'),
(2, 50000,  'Sedan',     'Model 3', 'Performance'),
(3, 48000,  'Crossover', 'Model Y', 'Long range'),
(4, 55000,  'Crossover', 'Model Y', 'Performance'),
(5, 83000,  'Sports',    'Model S', 'Long range'),
(6, 123000, 'Sports',   'Model S', 'Plaid');

CREATE EXTERNAL TABLE llap_orders (orderid INT, quantity INT, itemid INT, tradets TIMESTAMP) PARTITIONED BY (p1 STRING, p2 STRING) STORED BY ICEBERG STORED AS ORC;
INSERT INTO llap_orders VALUES
(0, 48, 5, timestamp('2000-06-04 19:55:46.129'), 'EU', 'DE'),
(1, 12, 6, timestamp('2007-06-24 19:23:22.829'), 'US', 'TX'),
(2, 76, 4, timestamp('2018-02-19 23:43:51.995'), 'EU', 'DE'),
(3, 91, 5, timestamp('2000-07-15 09:09:11.587'), 'US', 'NJ'),
(4, 18, 6, timestamp('2007-12-02 22:30:39.302'), 'EU', 'ES'),
(5, 71, 5, timestamp('2010-02-08 20:31:23.430'), 'EU', 'DE'),
(6, 78, 3, timestamp('2016-02-22 20:37:37.025'), 'EU', 'FR'),
(7, 88, 0, timestamp('2020-03-26 18:47:40.611'), 'EU', 'FR'),
(8, 87, 4, timestamp('2003-02-20 00:48:09.139'), 'EU', 'ES'),
(9, 60, 6, timestamp('2012-08-28 01:35:54.283'), 'EU', 'IT'),
(10, 24, 5, timestamp('2015-03-28 18:57:50.069'), 'US', 'NY'),
(11, 42, 2, timestamp('2012-06-27 01:13:32.350'), 'EU', 'UK'),
(12, 37, 4, timestamp('2020-08-09 01:18:50.153'), 'US', 'NY'),
(13, 52, 1, timestamp('2019-09-04 01:46:19.558'), 'EU', 'UK'),
(14, 96, 3, timestamp('2019-03-05 22:00:03.020'), 'US', 'NJ'),
(15, 18, 3, timestamp('2001-09-11 00:14:12.687'), 'EU', 'FR'),
(16, 46, 0, timestamp('2013-08-31 02:16:17.878'), 'EU', 'UK'),
(17, 26, 5, timestamp('2001-02-01 20:05:32.317'), 'EU', 'FR'),
(18, 68, 5, timestamp('2009-12-29 08:44:08.048'), 'EU', 'ES'),
(19, 54, 6, timestamp('2015-08-15 01:59:22.177'), 'EU', 'HU'),
(20, 10, 0, timestamp('2018-05-06 12:56:12.789'), 'US', 'CA');

--select query without any schema change yet
SELECT i.name, i.description, SUM(o.quantity) FROM llap_items i JOIN llap_orders o ON i.itemid = o.itemid  WHERE p1 = 'EU' and i.price >= 50000 GROUP BY i.name, i.description;


--schema evolution on unpartitioned table
--renames and reorders
ALTER TABLE llap_items CHANGE category cat string AFTER description;
ALTER TABLE llap_items CHANGE price cost int AFTER name;
SELECT i.name, i.description, SUM(o.quantity) FROM llap_items i JOIN llap_orders o ON i.itemid = o.itemid  WHERE p1 = 'EU' and i.cost >= 100000 GROUP BY i.name, i.description;

--adding a column
ALTER TABLE llap_items ADD COLUMNS (to60 float);
INSERT INTO llap_items VALUES
(7, 'Model X', 93000, 'SUV', 'Long range', 3.8),
(7, 'Model X', 113000, 'SUV', 'Plaid', 2.5);
SELECT cat, min(to60) from llap_items group by cat;

--removing a column
ALTER TABLE llap_items REPLACE COLUMNS (itemid int, name string, cost int COMMENT 'from deserializer', description string, to60 float);
INSERT INTO llap_items VALUES
(8, 'Cybertruck', 40000, 'Single Motor RWD', 6.5),
(9, 'Cybertruck', 50000, 'Dual Motor AWD', 4.5);
SELECT name, min(to60), max(cost) FROM llap_items WHERE itemid > 3 GROUP BY name;


--schema evolution on partitioned table (including partition changes)
--renames and reorders
ALTER TABLE llap_orders CHANGE tradets ordertime timestamp AFTER p2;
ALTER TABLE llap_orders CHANGE p1 region string;
INSERT INTO llap_orders VALUES
(21, 21, 8, 'EU', timestamp('2000-01-04 19:55:46.129'), 'HU');
SELECT region, min(ordertime), sum(quantity) FROM llap_orders WHERE itemid > 5 GROUP BY region;

ALTER TABLE llap_orders CHANGE p2 state string;
SELECT region, state, min(ordertime), sum(quantity) FROM llap_orders WHERE itemid > 5 GROUP BY region, state;

--adding new column
ALTER TABLE llap_orders ADD COLUMNS (city string);
INSERT INTO llap_orders VALUES
(22, 99, 9, 'EU', timestamp('2021-01-04 19:55:46.129'), 'DE', 'München');
SELECT state, max(city) from llap_orders WHERE region = 'EU' GROUP BY state;

--making it a partition column
ALTER TABLE llap_orders SET PARTITION SPEC (region, state, city);
INSERT INTO llap_orders VALUES
(23, 89, 6, 'EU', timestamp('2021-02-04 19:55:46.129'), 'IT', 'Venezia');
SELECT state, max(city), avg(itemid) from llap_orders WHERE region = 'EU' GROUP BY state;

--de-partitioning a column
ALTER TABLE llap_orders SET PARTITION SPEC (state, city);
INSERT INTO llap_orders VALUES
(24, 88, 5, 'EU', timestamp('2006-02-04 19:55:46.129'), 'UK', 'London');
SELECT state, max(city), avg(itemid) from llap_orders WHERE region = 'EU' GROUP BY state;

--removing a column from schema
ALTER TABLE llap_orders REPLACE COLUMNS (quantity int, itemid int, region string COMMENT 'from deserializer', ordertime timestamp COMMENT 'from deserializer', state string COMMENT 'from deserializer', city string);
INSERT INTO llap_orders VALUES
(88, 5, 'EU', timestamp('2006-02-04 19:55:46.129'), 'FR', 'Paris');
SELECT state, max(city), avg(itemid) from llap_orders WHERE region = 'EU' GROUP BY state;


--some more projections
SELECT o.city, i.name, min(i.cost), max(to60), sum(o.quantity) FROM llap_items i JOIN llap_orders o ON i.itemid = o.itemid  WHERE region = 'EU' and i.cost >= 50000 and ordertime > timestamp('2010-01-01') GROUP BY o.city, i.name;
SELECT i.name, i.description, SUM(o.quantity) FROM llap_items i JOIN llap_orders o ON i.itemid = o.itemid  WHERE region = 'EU' and i.cost >= 50000 GROUP BY i.name, i.description;