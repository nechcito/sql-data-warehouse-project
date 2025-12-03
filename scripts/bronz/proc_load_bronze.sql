

CREATE OR ALTER PROCEDURE bronz.load_bronz AS 
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @start_whole DATETIME , @end_whole DATETIME;
	BEGIN TRY
	SET @start_whole=GETDATE();
		PRINT '====================================================================================================';
		PRINT 'LOADING BRONZE LAYER';
		PRINT '====================================================================================================';

		PRINT '----------------------------------------------------------------------------------------------------';
		PRINT 'Loading CRM tables';
		PRINT '----------------------------------------------------------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table : bronz.crm_cust_info'
		TRUNCATE TABLE bronz.crm_cust_info;
		PRINT '>> Inserting data Into : bronz.crm_cust_info'
		BULK INSERT bronz.crm_cust_info
		FROM 'C:\Users\pocitac\Desktop\PeterSQL\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '---------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table : bronz.crm_prd_info'
		TRUNCATE TABLE bronz.crm_prd_info;
		PRINT '>> Inserting data Into : bronz.crm_prd_info'
		BULK INSERT bronz.crm_prd_info
		FROM 'C:\Users\pocitac\Desktop\PeterSQL\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '---------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table : bronz.crm_sales_details'
		TRUNCATE TABLE bronz.crm_sales_details;
		PRINT '>> Inserting data Into : bronz.crm_sales_details'
		BULK INSERT bronz.crm_sales_details
		FROM 'C:\Users\pocitac\Desktop\PeterSQL\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '---------------------------------------';

		PRINT '----------------------------------------------------------------------------------------------------';
		PRINT 'Loading ERP tables';
		PRINT '----------------------------------------------------------------------------------------------------'; 

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table : bronz.erp_cust_az12'
		TRUNCATE TABLE bronz.erp_cust_az12;
		PRINT '>> Inserting data Into : bronz.erp_cust_az12'
		BULK INSERT bronz.erp_cust_az12
		FROM 'C:\Users\pocitac\Desktop\PeterSQL\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		WITH(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '---------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table : bronz.erp_loc_a101'
		TRUNCATE TABLE bronz.erp_loc_a101;
		PRINT '>> Inserting data Into : bronz.erp_loc_a101'
		BULK INSERT bronz.erp_loc_a101
		FROM 'C:\Users\pocitac\Desktop\PeterSQL\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		WITH(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration : ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '---------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table : bronz.erp_px_cat_g1v2'
		TRUNCATE TABLE bronz.erp_px_cat_g1v2;
		PRINT '>> Inserting data Into : bronz.erp_px_cat_g1v2'
		BULK INSERT bronz.erp_px_cat_g1v2
		FROM 'C:\Users\pocitac\Desktop\PeterSQL\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH(
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK 
		);
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
