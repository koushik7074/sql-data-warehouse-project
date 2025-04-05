
create or alter procedure bronze.load_bronze 
as
begin
	declare @start_time datetime, @end_time datetime
	begin try
		print '=============================================================';
		print('Loading Bronze Layer');
		print '=============================================================';

		print '-------------------------------------------------------------';
		print('Loading CRM Tables');
		print '-------------------------------------------------------------';

		set @start_time = getdate();
		print '>>Truncating Table: bronze.crm_cust_info<<'
		truncate table bronze.crm_cust_info;
		print '>>Inserting Data Into: bronze.crm_cust_info<<'
		BULK INSERT bronze.crm_cust_info
		from 'C:\Users\Koushik\OneDrive\Desktop\datasets\source_crm\cust_info.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = getdate();
		print '>>Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
		print '>>--------------';

		set @start_time = getdate();
		print '>>Truncating Table: bronze.crm_prd_info<<'
		truncate table bronze.crm_prd_info;
		print '>>Inserting Data Into: bronze.crm_prd_info<<'
		BULK INSERT bronze.crm_prd_info
		from 'C:\Users\Koushik\OneDrive\Desktop\datasets\source_crm\prd_info.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = getdate();
		print '>>Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
		print '>>--------------';

		set @start_time = getdate();
		print '>>Truncating Table: bronze.crm_sales_details<<'
		truncate table bronze.crm_sales_details;
		print '>>Inserting Data Into: bronze.crm_sales_details<<'
		BULK INSERT bronze.crm_sales_details
		from 'C:\Users\Koushik\OneDrive\Desktop\datasets\source_crm\sales_details.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = getdate();
		print '>>Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
		print '>>--------------';

		print '-------------------------------------------------------------';
		print('Loading ERP Tables');
		print '-------------------------------------------------------------';

		set @start_time = getdate();
		print '>>Truncating Table: bronze.erp_CUST_AZ12<<'
		truncate table bronze.erp_CUST_AZ12;
		print '>>Inserting Data Into: bronze.erp_CUST_AZ12<<'
		BULK INSERT bronze.erp_CUST_AZ12
		from 'C:\Users\Koushik\OneDrive\Desktop\datasets\source_erp\CUST_AZ12.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = getdate();
		print '>>Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
		print '>>--------------';

		set @start_time = getdate();
		print '>>Truncating Table: bronze.erp_LOC_A101<<'
		truncate table bronze.erp_LOC_A101;
		print '>>Inserting Data Into: bronze.erp_LOC_A101<<'
		BULK INSERT bronze.erp_LOC_A101
		from 'C:\Users\Koushik\OneDrive\Desktop\datasets\source_erp\LOC_A101.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = getdate();
		print '>>Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
		print '>>--------------';

		set @start_time = getdate();
		print '>>Truncating Table: bronze.erp_PX_CAT_G1V2<<'
		truncate table bronze.erp_PX_CAT_G1V2;
		print '>>Inserting Data Into: bronze.erp_PX_CAT_G1V2<<'
		BULK INSERT bronze.erp_PX_CAT_G1V2
		from 'C:\Users\Koushik\OneDrive\Desktop\datasets\source_erp\PX_CAT_G1V2.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock
		);
		set @end_time = getdate();
		print '>>Load Duration: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + 'seconds';
		print '>>--------------';

		end try

		begin catch
		print '======================================================================';
		print 'ERROR OCCURED DURING LOADING BRONZE LAYER.';
		PRINT 'Error Message' + Error_Message();
		print 'Error Number' + cast(Error_Number() as nvarchar)
		print '======================================================================';
		end catch

end

exec bronze.load_bronze;

