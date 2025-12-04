/* Creating STORED PROCEDURE 
ELT process from Bronz Layer to the Silver layer*/

CREATE OR ALTER PROCEDURE silver.load_silver AS 
	BEGIN
		DECLARE @start_time DATETIME, @end_time DATETIME,@start_whole DATETIME , @end_whole DATETIME;
		BEGIN TRY
		SET @start_whole=GETDATE();
			PRINT '====================================================================================================';
			PRINT 'LOADING SILVER LAYER';
			PRINT '====================================================================================================';

			PRINT '----------------------------------------------------------------------------------------------------';
			PRINT 'Loading CRM tables';
			PRINT '----------------------------------------------------------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>>Truncating Table silver.crm_cust_info'
		TRUNCATE TABLE silver.crm_cust_info
		PRINT 'INSERT INTO TABLE silver.crm_cust_info'

		INSERT INTO silver.crm_cust_info(
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_marital_status,
		cst_gndr,
		cst_created_date)

		SELECT
		cst_id,
		cst_key,
		TRIM(cst_firstname) AS cst_firstname,
		TRIM(cst_lastname) AS cst_lastname,
		CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
			 WHEN UPPER(TRIM(cst_marital_status)) =  'M' THEN 'Married'
			 ELSE 'n/a'
		END AS cst_marital_status, --Normalize Marital Status
		CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
			 WHEN UPPER(TRIM(cst_gndr)) =  'M' THEN 'Male'
			 ELSE 'n/a'
		END AS cst_gndr, -- Normalize Gender Statzs 
		cst_create_date
		FROM(
		SELECT
		*,
		ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
		FROM bronz.crm_cust_info
		WHERE cst_id IS NOT NULL
		)t WHERE flag_last =1 -- Select the most recent order per customer 
		SET @end_time = GETDATE();
		PRINT '>> Load Duration : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '---------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>>Truncating Table silver.crm_prd_info'
		TRUNCATE TABLE silver.crm_prd_info
		PRINT 'INSERT INTO TABLE silver.crm_prd_info'

		INSERT INTO silver.crm_prd_info(
		prd_int,
		cat_id,
		prd_key,
		prd_name,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
		)

		SELECT
		prd_int,
		REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id, --Extract category id
		SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key, --Extract product key
		TRIM(prd_nm) AS prd_name,
		ISNULL(prd_cost,0) AS prd_cost,
		CASE UPPER(TRIM(prd_line)) 
			 WHEN 'M' THEN 'Mountain'
			 WHEN 'R' THEN 'Road'
			 WHEN 'S' THEN 'Other Sales'
			 WHEN 'T' THEN 'Touring'
			 ELSE 'n/a'
		END AS prd_line, -- Map product line codes 
		CAST(prd_star_dt AS DATE) AS prd_start_dt,
		CAST(Lead(prd_star_dt)OVER (PARTITION BY prd_key ORDER BY prd_star_dt ) -1 AS DATE) AS prd_end_dt --Calculate emd date as one day before the start date 
		FROM bronz.crm_prd_info
		SET @end_time = GETDATE();
		PRINT '>> Load Duration : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '---------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>>Truncating Table silver.crm_sales_details'
		TRUNCATE TABLE silver.crm_sales_details
		PRINT 'INSERT INTO TABLE silver.crm_sales_details'

		INSERT INTO silver.crm_sales_details(
		sls_ord_num, 
		sls_prd_key, 
		sls_cust_id, 
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_sales, 
		sls_quantity,
		sls_price
		)

		SELECT
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
			 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
		END AS sls_order_dt,	
		CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
			 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
		END AS sls_ship_dt,
		CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
			 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
		END AS sls_due_dt,
		CASE WHEN sls_sales <=0  OR sls_sales IS NULL OR sls_sales != sls_quantity * ABS(sls_price) 
			 THEN sls_quantity * ABS(sls_price)
			 ELSE sls_sales
		END AS sls_sales,
		sls_quantity,
		CASE WHEN sls_price <=0 OR  sls_price IS NULL 
			 THEN sls_sales /NULLIF(sls_quantity,0)
			 ELSE sls_price
		END AS sls_price	 
		FROM bronz.crm_sales_details
		SET @end_time = GETDATE();
		PRINT '>> Load Duration : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '---------------------------------------';

		PRINT '----------------------------------------------------------------------------------------------------';
		PRINT 'Loading ERP tables';
		PRINT '----------------------------------------------------------------------------------------------------'; 

		SET @start_time = GETDATE();
		PRINT '>>Truncating Table silver.erp_cust_az12'
		TRUNCATE TABLE silver.erp_cust_az12
		PRINT 'INSERT INTO TABLE silver.erp_cust_az12'

		INSERT INTO silver.erp_cust_az12(cid,bdate,gen)

		SELECT
		CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
			 ELSE cid
		END AS cid, --Remove prefix NAS if present
		CASE WHEN bdate > GETDATE() THEN NULL
			 ELSE bdate -- SET futere dates to Null
		END AS bdate,

		CASE WHEN TRIM(UPPER(gen)) IN ('F','Female') THEN 'Female'
			 WHEN TRIM(UPPER(gen)) IN ('M','Male') THEN 'Male'
			 ELSE 'n/a'
		END AS gen -- Normalize gender values 
		FROM bronz.erp_cust_az12
		SET @end_time = GETDATE();
		PRINT '>> Load Duration : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '---------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>>Truncating Table silver.erp_loc_a101'
		TRUNCATE TABLE silver.erp_loc_a101
		PRINT 'INSERT INTO TABLE silver.erp_loc_a101'
		INSERT INTO silver.erp_loc_a101(cid,cntry)

		SELECT 
		REPLACE(cid,'-','') AS cid,
		CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
			 WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
			 WHEN TRIM(cntry) ='' OR cntry IS NULL THEN 'n/a'
			 ELSE TRIM(cntry)
		END AS cntry -- Normalize and handle missing or blank cntry values 
		FROM bronz.erp_loc_a101
		SET @end_time = GETDATE();
		PRINT '>> Load Duration : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '---------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>>Truncating Table silver.erp_px_cat_g1v2'
		TRUNCATE TABLE silver.erp_px_cat_g1v2
		PRINT 'INSERT INTO TABLE silver.erp_px_cat_g1v2'
		INSERT INTO silver.erp_px_cat_g1v2(id,cat,subcat,maintenance)
		SELECT
		id,
		cat,
		subcat,
		maintenance
		FROM bronz.erp_px_cat_g1v2
		SET @end_time = GETDATE();
		PRINT '>> Load Duration : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '---------------------------------------';
		PRINT '==========================================';
			PRINT ' Loading Table bronz is  Completed';
			SET @end_whole = GETDATE();
			PRINT '>> Total Load Duration : ' + CAST(DATEDIFF(second, @start_whole, @end_whole) AS NVARCHAR) + ' seconds';
			PRINT '---------------------------------------';
		END TRY
		BEGIN CATCH
			PRINT '--------------------------------------------------';
			PRINT 'Vyskytol sa problém pri načitaní Bronz Layer';
			PRINT 'Error Message' + ERROR_MESSAGE();
			PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
			PRINT 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
			PRINT '--------------------------------------------------';
		END CATCH
	END
