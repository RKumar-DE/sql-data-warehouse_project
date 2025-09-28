
/*
==========================================================================================================================
Project        : SQL Data Warehouse Project
Layer          : Gold Layer (Business-Ready / Analytics)
Script Purpose : Create business-facing views for Customers, Products, and Fact Sales.

Description    :
- This script drops and recreates Gold Layer views.
- Views in the Gold layer combine cleansed Silver layer data into subject-oriented,
  analytics-ready datasets for reporting and BI consumption.
- Includes:
  1. CUSTOMER view  -> Unified customer details (CRM + ERP data)
  2. PRODUCT view   -> Product details with category and maintenance info
  3. FACT_SALES     -> Sales facts with order, product, and customer references

⚠️ WARNING:
- Running this script will DROP and RECREATE views in the Gold schema.
- Ensure that Silver layer tables are populated before execution.

Author         : Rahul Kumar
Database       : PostgreSQL
Last Updated   : 28-Sep-2025
==========================================================================================================================
*/

-- ======================================================================================================================
-- CUSTOMER VIEW (Enriched customer details with gender, country, and birthdate)
-- ======================================================================================================================
DROP VIEW IF EXISTS gold.dim_customer;

CREATE VIEW gold.dim_customer AS
SELECT 
    cci.cst_id              AS customer_id,
    cci.cst_key             AS customer_key,
    cci.cst_firstname       AS firstname,
    cci.cst_lastname        AS lastname,
    cci.cst_marital_status  AS marital_status,
    CASE 
        WHEN cci.cst_gndr = 'N/A' THEN eci.gen
        ELSE cci.cst_gndr
    END                     AS gender,
    el.cntry                AS country,
    eci.bdate               AS birth_date,
    cci.cst_create_date     AS create_date
FROM silver.crm_cust_info cci
LEFT JOIN silver.erp_cust_az12 eci
    ON cci.cst_key = eci.cid
LEFT JOIN silver.erp_loc_a101 el
    ON cci.cst_key = el.cid;


-- ======================================================================================================================
-- PRODUCT VIEW (Product details enriched with category, subcategory, and maintenance info)
-- ======================================================================================================================
DROP VIEW IF EXISTS gold.dim_product;

CREATE VIEW gold.dim_product AS
SELECT 
    cpi.prd_id          AS product_id,
    cpi.prd_key         AS product_key,
    cpi.prd_nm          AS product_name,
    cpi.prd_cost        AS product_cost,
    cpi.prd_line        AS product_line,
    cpi.cat_id          AS category_id,
    ep.cat              AS category,
    ep.subcat           AS subcategory,
    cpi.prd_start_dt    AS start_date,
    cpi.prd_end_dt      AS end_date,
    ep.maintenance      AS maintenance
FROM silver.crm_prd_info cpi
LEFT JOIN silver.erp_px_cat_g1v2 ep
    ON cpi.cat_id = ep.id;


-- ======================================================================================================================
-- FACT_SALES VIEW (Sales transactions fact table for analytics)
-- ======================================================================================================================
DROP VIEW IF EXISTS gold.fact_sales;

CREATE VIEW gold.fact_sales AS
SELECT 
    sls_ord_num     AS order_number,
    sls_prd_key     AS order_id,
    sls_cust_id     AS customer_id,
    sls_order_dt    AS order_date,
    sls_ship_dt     AS shipping_date,
    sls_due_dt      AS due_date,
    sls_sales       AS sales,
    sls_quantity    AS quantity,
    sls_price       AS price
FROM silver.crm_sales_details;
