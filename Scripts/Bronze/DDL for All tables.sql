/*
==========================================================================================================================
Project        : SQL Data Warehouse Project
Layer          : Bronze Layer (Raw Ingestion)
Script Purpose : DDL scripts to create all base tables required in the Bronze layer.

Description    : 
- This script drops existing Bronze layer tables (if any) and recreates them.
- Tables include CRM product, sales, customer info, and ERP-related datasets.
- These tables serve as the raw ingestion layer in the Data Warehouse.

⚠️ WARNING:
- Running this script will DROP and RECREATE tables in the Bronze schema.
- All existing data in these tables will be permanently lost.
- Ensure backups or upstream data sources are available before execution.

Author         : Rahul Kumar
Database       : PostgreSQL
Last Updated   : 28-Sep-2025
==========================================================================================================================
*/

DROP TABLE IF EXISTS bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info(
prd_id int,
prd_key varchar(50),
prd_nm varchar(50),
prd_cost int,
prd_line varchar(50),
prd_start_dt timestamp,
prd_end_dt timestamp
);


DROP TABLE IF EXISTS bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details(
sls_ord_num VARCHAR(50),
sls_prd_key VARCHAR(50),
sls_cust_id INT,
sls_order_dt INT,
sls_ship_dt INT,
sls_due_dt INT,
sls_sales INT,
sls_quantity INT,
sls_price INT
);


DROP TABLE IF EXISTS bronze.crm_cust_info;
CREATE TABLE bronze.crm_cust_info(
cst_id int,
cst_key VARCHAR(20),
cst_firstname VARCHAR(30),
cst_lastname VARCHAR(30),
cst_marital_status VARCHAR(5),
cst_gndr VARCHAR(5),
cst_create_date DATE
);


DROP TABLE IF EXISTS bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12(
CID VARCHAR(50),
BDATE DATE,
GEN VARCHAR(50)
);


DROP TABLE IF EXISTS bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101(
CID VARCHAR(50),
CNTRY VARCHAR(50)
);

DROP TABLE IF EXISTS bronze.erp_px_cat_g1v2;

CREATE TABLE bronze.erp_px_cat_g1v2(
ID VARCHAR(50),
CAT VARCHAR(50),
SUBCAT VARCHAR(50),
MAINTENANCE VARCHAR(50)
);
