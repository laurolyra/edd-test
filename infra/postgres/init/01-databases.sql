-- Each microservice owns its own database. They share a Postgres server here
-- only to keep local setup light; in production they would be separate clusters.
CREATE DATABASE orders_db;
CREATE DATABASE stock_db;
CREATE DATABASE invoices_db;
CREATE DATABASE fiscal_db;
CREATE DATABASE recommendations_db;
