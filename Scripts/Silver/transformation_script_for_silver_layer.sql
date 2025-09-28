/*
==========================================================================================================================
Project        : SQL Data Warehouse Project
Layer          : Silver Layer (Cleansed / Standardized Data)
Script Purpose : Transform raw Bronze layer tables and load into Silver layer tables.

Description    :
- This script performs transformations and data quality checks on Bronze tables before inserting into Silver layer.
- Transformations include:
    1. CRM customer info       -> trimming names, standardizing marital status and gender
    2. CRM product info        -> parsing product keys, standardizing product lines, calculating end dates
    3. CRM sales details       -> converting integer dates to proper DATE, recalculating sales and price if inconsistent
    4. ERP customer info       -> cleaning birthdates and standardizing gender
    5. ERP location info       -> cleaning CID, standardizing country names
    6. ERP product category    -> no transformation required, direct load
- All existing Silver layer tables are truncated before insert.

⚠️ WARNING:
- Running this script will **TRUNCATE Silver tables**, permanently deleting existing data.
- Ensure Bronze layer tables are populated and correct before execution.

Author         : Rahul Kumar
Database       : PostgreSQL
Last Updated   : 28-Sep-2025
==========================================================================================================================*/



--TRANSFORMATION OF CRM_CUST_INTO

TRUNCATE TABLE silver.crm_cust_info;

insert into silver.crm_cust_info
(select cst_id,cst_key,cst_firstname,cst_lastname,cst_marital_status,cst_gndr,cst_create_date
from 
(SELECT 
cst_id,
cst_key,
trim(cst_firstname) as cst_firstname,
trim(cst_lastname) as cst_lastname,
--cst_marital_status,
	CASE upper(trim(cst_marital_status))
		 WHEN 'M' then 'Married'
		 WHEN 'S' then 'Single'
		 else 'N/A'
	END AS cst_marital_status,
--cst_gndr,
	CASE upper(trim(cst_gndr))
		 WHEN 'M' then 'Male'
		 WHEN 'F' then 'Female'
		 else 'N/A'
	END AS cst_gndr, 
cst_create_date,
row_number() over(partition by cst_id order by cst_create_date desc) rn
from bronze.crm_cust_info 
) transformed_table
where rn=1)

select * from silver.crm_cust_info;


/*
=====================================================================================================
TRANSFORMATION OF CRM_PRD_INFO TABLE
=====================================================================================================*/
TRUNCATE TABLE silver.crm_prd_info;

insert into silver.crm_prd_info
select prd_id,cat_id,prd_key,prd_nm,prd_cost,prd_line,prd_start_dt,prd_end_dt
from (select 
prd_id,
--prd_key, 
replace(LEFT(TRIM(prd_key),5),'-','_') AS cat_id,
SUBSTRING(TRIM(prd_key),7, length(prd_key)) as prd_key,
prd_nm,
coalesce(prd_cost,0) as prd_cost,
--prd_line,
CASE UPPER(TRIM(prd_line))
	WHEN 'M' THEN 'Mountain'
	WHEN 'R' THEN 'Road'
	WHEN 'S' THEN 'Other Sales'
	WHEN 'T' THEN 'Touring'
	ELSE 'N/A'
END AS prd_line,
cast(prd_start_dt as date),
--prd_end_dt,
lead(cast(prd_start_dt as date),1) over(partition by prd_key order by prd_start_dt)-1 as prd_end_dt
from bronze.crm_prd_info) AS T

/*
==================================================================================================
TRANSFORMATION SCRIPT FOR CRM_SALES_DETAILS
==================================================================================================*/
--DQ Cheque 1.sls_order_dt,sls_due_dt,sls_ship_dt should be date, sales* quantiry = price this should follow
TRUNCATE TABLE silver.crm_sales_details;

insert into silver.crm_sales_details
SELECT 
    sls_ord_num,
    sls_prd_key,
    sls_cust_id, 
    case when sls_order_dt=0 OR LENGTH(sls_order_dt::TEXT) !=8 THEN NULL
		 ELSE TO_DATE(sls_order_dt::text, 'YYYYMMDD')
	END AS sls_order_dt,
	CASE WHEN sls_ship_dt=0 OR LENGTH(sls_ship_dt::TEXT) !=8 THEN NULL
		 ELSE TO_DATE(sls_ship_dt::TEXT,'YYYYMMDD')
	END AS sls_ship_dt,
	CASE WHEN sls_due_dt=0 OR LENGTH(sls_due_dt::TEXT) !=8 THEN NULL
		 ELSE TO_DATE(sls_due_dt::TEXT,'YYYYMMDD')
	END AS sls_due_dt,
 	
	case when sls_sales<=0 or sls_sales is null or sls_sales !=sls_quantity*abs(sls_price)  then sls_quantity*abs(sls_price)
		 else sls_sales 
	end as sls_sales,

	sls_quantity, 
	
	case when sls_price<=0 or sls_price is null then sls_sales/ nullif(sls_quantity,0) else sls_price end as sls_price
FROM BRONZE.CRM_SALES_DETAILS;


/*
==================================================================================================
TRANSFORMATION SCRIPT FOR ERP_
==================================================================================================*/
--DQ CHECK 1.remove "nas", 2. birthdate> current_date, 3. gender should be only Male , female or n/a
TRUNCATE TABLE silver.erp_cust_az12;

insert into silver.erp_cust_az12
SELECT 
SUBSTRING(cid,4,length(cid)) as cid,
case when bdate > current_date then null
	 else bdate
end as bdate,
case 
	when gen in ('Male','M') then 'Male'
	when gen in ('Female','F') then 'Female'
	else 'N/A'
END as gen
FROM bronze.erp_cust_az12;

/*
==================================================================================================
TRANSFORMATION SCRIPT FOR erp_loc_a101
==================================================================================================*/
--DQ check -> remove the hifen(-) and make sure country is proper
TRUNCATE TABLE silver.erp_loc_a101;

insert into silver.erp_loc_a101
select 
replace(cid,'-','') as cid,
CASE 
	WHEN cntry='DE' then 'Germany'
	WHEN cntry in ('USA', 'US') then 'United States'
	when cntry is null or cntry='' then 'N/A'
	else cntry
end as cntry
from bronze.erp_loc_a101

/*
==================================================================================================
TRANSFORMATION SCRIPT FOR erp_px_cat_g1v2
==================================================================================================*/

--No transformation required

TRUNCATE TABLE silver.erp_px_cat_g1v2;

insert into silver.erp_px_cat_g1v2
select * from erp_px_cat_g1v2;
