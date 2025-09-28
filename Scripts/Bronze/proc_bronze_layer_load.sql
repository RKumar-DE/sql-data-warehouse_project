--------------------------------------------------------------------------------------
--FULL LOAD IN BRONZE LAYER TABLES
--------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE load_bronze()
LANGUAGE PLPGSQL
AS 
$$
DECLARE
		start_time timestamp;--clock_timestamp()
		end_time timestamp;
		duration interval;
BEGIN
		start_time:= clock_timestamp();
		RAISE NOTICE 'data loading in table started at %',start_time;

		
		TRUNCATE TABLE bronze.crm_cust_info;
		COPY bronze.crm_cust_info
		FROM 'C:\Users\rjayk\Downloads\SQL related files\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		DELIMITER ','
		CSV HEADER;
		
		TRUNCATE TABLE bronze.crm_prd_info;
		COPY bronze.crm_prd_info
		FROM 'C:\Users\rjayk\Downloads\SQL related files\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		DELIMITER ','
		CSV HEADER;
		
		TRUNCATE TABLE bronze.crm_sales_details;
		COPY bronze.crm_sales_details
		FROM 'C:\Users\rjayk\Downloads\SQL related files\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		DELIMITER ','
		CSV HEADER;
		
		TRUNCATE TABLE bronze.erp_cust_az12;
		COPY bronze.erp_cust_az12
		FROM 'C:\Users\rjayk\Downloads\SQL related files\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		DELIMITER ','
		CSV HEADER;
		
		TRUNCATE TABLE bronze.erp_loc_a101;
		COPY bronze.erp_loc_a101
		FROM 'C:\Users\rjayk\Downloads\SQL related files\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		DELIMITER ','
		CSV HEADER;
		
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		COPY bronze.erp_px_cat_g1v2
		FROM 'C:\Users\rjayk\Downloads\SQL related files\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		DELIMITER ','
		CSV HEADER;

		end_time := clock_timestamp();
		raise notice 'data loading in table ended %',end_time;
		duration:= end_time-start_time;
		raise notice 'time taken to load data %', duration;

		
END;
$$;

call load_bronze();
