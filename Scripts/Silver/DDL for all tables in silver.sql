/*
==========================================================================================================================
Project        : SQL Data Warehouse Project
Layer          : Silver Layer (Cleansed / Standardized Data)
Script Purpose : DDL scripts to create all required tables in the Silver layer.

Description    :
- This script drops existing Silver layer tables (if any) and recreates them.
- Tables in the Silver layer are derived from the Bronze layer after cleansing,
  standardization, and applying basic transformations.
- These tables serve as the foundation for analytical queries and further modeling.

⚠️ WARNING:
- Running this script will DROP and RECREATE tables in the Silver schema.
- All existing data in these tables will be permanently lost.
- Ensure upstream Bronze layer data and ETL pipelines are in place before execution.

Author         : Rahul Kumar
Database       : PostgreSQL
Last Updated   : 28-Sep-2025
==========================================================================================================================
*/


DROP TABLE IF EXISTS silver.crm_prd_info;
CREATE TABLE silver.crm_prd_info(
prd_id int,
cat_id varchar(50),
prd_key varchar(50),
prd_nm varchar(50),
prd_cost int,
prd_line varchar(50),
prd_start_dt date,
prd_end_dt date
);


DROP TABLE IF EXISTS silver.crm_sales_details;
CREATE TABLE silver.crm_sales_details(
sls_ord_num VARCHAR(50),
sls_prd_key VARCHAR(50),
sls_cust_id INT,
sls_order_dt DATE,
sls_ship_dt DATE,
sls_due_dt DATE,
sls_sales INT,
sls_quantity INT,
sls_price INT
);


DROP TABLE IF EXISTS silver.crm_cust_info;
CREATE TABLE silver.crm_cust_info(
cst_id int,
cst_key VARCHAR(50),
cst_firstname VARCHAR(50),
cst_lastname VARCHAR(50),
cst_marital_status VARCHAR(50),
cst_gndr VARCHAR(50),
cst_create_date DATE
);


DROP TABLE IF EXISTS silver.erp_cust_az12;
CREATE TABLE silver.erp_cust_az12(
CID VARCHAR(50),
BDATE DATE,
GEN VARCHAR(50)
);


DROP TABLE IF EXISTS silver.erp_loc_a101;
CREATE TABLE silver.erp_loc_a101(
CID VARCHAR(50),
CNTRY VARCHAR(50)
);

DROP TABLE IF EXISTS silver.erp_px_cat_g1v2;

CREATE TABLE silver.erp_px_cat_g1v2(
ID VARCHAR(50),
CAT VARCHAR(50),
SUBCAT VARCHAR(50),
MAINTENANCE VARCHAR(50)
);
