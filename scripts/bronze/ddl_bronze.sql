/*
===========================================================
DDL Script: Create Bronze Tables
===========================================================

Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables
    if they already exist.
    Run this script to re-define the DDL structure of 'bronze' Tables

===========================================================
*/


exec bronze.load_bronze

use DataWareHouse;
GO

create or alter procedure bronze.load_bronze as 
begin
	declare @start_time datetime, @end_time datetime, @batch_start_time datetime, @batch_end_time datetime;
	begin try
		set @batch_start_time = GETDATE()
		print '===========================================';
		print 'Loading Bronze Layer';
		print '===========================================';


		print '-------------------------------------------';
		print 'Loading CRM tables' ;
		print '-------------------------------------------';


		/*------------------crm_cust_info_import--------------------*/

		set @start_time = getdate();
		print '>> truncating table bronze.crm_cust_info';
		Print '>> inserting data into: bronze.crm_cust_info';
		truncate table bronze.crm_cust_info

		bulk insert bronze.crm_cust_info
		from 'C:\Users\abdoh\Desktop\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = getdate();
		print '>> load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds';
		print '--------------------------------'
		
		/*------------------crm_prd_info_import--------------------*/


		set @start_time = getdate()
		print '>> truncating table bronze.crm_prd_info';
		Print '>> inserting data into: bronze.crm_prd_info';

		truncate table bronze.crm_prd_info

		bulk insert bronze.crm_prd_info
		from 'C:\Users\abdoh\Desktop\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = getdate();
		print '>> load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds';
		print '--------------------------------'

		/*------------------crm_sales_details_import--------------------*/

		set @start_time = getdate()
		print '>> truncating table bronze.crm_sales_details';
		Print '>> inserting data into: bronze.crm_sales_details';
		truncate table bronze.crm_sales_details

		bulk insert bronze.crm_sales_details
		from 'C:\Users\abdoh\Desktop\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = getdate();
		print '>> load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds';
		print '--------------------------------'

		print '-------------------------------------------';
		print 'Loading ERP tables' ;
		print '-------------------------------------------';

		/*------------------erp_CUST_AZ12_import--------------------*/

		set @start_time = getdate()
		print '>> truncating table bronze.erp_CUST_AZ12';
		Print '>> inserting data into: bronze.erp_CUST_AZ12';
		truncate table bronze.erp_CUST_AZ12

		bulk insert bronze.erp_CUST_AZ12
		from 'C:\Users\abdoh\Desktop\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = getdate();
		print '>> load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds';
		print '--------------------------------'

		/*------------------erp_LOC_A101_import--------------------*/

		set @start_time = getdate()
		print '>> truncating table bronze.erp_LOC_A101';
		Print '>> inserting data into: bronze.erp_LOC_A101';

		truncate table bronze.erp_LOC_A101

		bulk insert bronze.erp_LOC_A101
		from 'C:\Users\abdoh\Desktop\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = getdate();
		print '>> load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds';
		print '--------------------------------'

		/*------------------erp_PX_CAT_G1V2_import--------------------*/

		set @start_time = getdate()
		print '>> truncating table bronze.erp_PX_CAT_G1V2';
		Print '>> inserting data into: bronze.erp_PX_CAT_G1V2';


		truncate table bronze.erp_PX_CAT_G1V2

		bulk insert bronze.erp_PX_CAT_G1V2
		from 'C:\Users\abdoh\Desktop\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		with (
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = getdate();
		print '>> load duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds';
		print '--------------------------------'

		set @batch_end_time = getdate();
		print 'Loading Bronze Layer is Completed'
		print '>> batch load duration: ' + cast(datediff(second, @batch_start_time, @batch_end_time) as nvarchar) + ' seconds';
		print '--------------------------------'
	end try
	begin catch 
		print '==========================='
		print 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'error message' + ERROR_MESSAGE();
		print 'error message' + cast (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'error message' + cast (error_state() as nvarchar);
		print '==========================='
	
	end catch

end 

