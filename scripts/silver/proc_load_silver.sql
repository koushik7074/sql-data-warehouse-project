
create or alter procedure silver.load_silver
as
begin
	begin try

		print '>> Truncating table: silver.crm_cust_info'
		truncate table silver.crm_cust_info
		print '>> Inserting data into: silver.crm_cust_info'
		INSERT INTO silver.crm_cust_info (
			cst_id, 
			cst_key, 
			cst_firstname, 
			cst_lastname, 
			cst_marital_status, 
			cst_gndr,
			cst_create_date
				)
		SELECT
			cst_id,
			cst_key,
			TRIM(cst_firstname) AS cst_firstname,
			TRIM(cst_lastname) AS cst_lastname,
			CASE 
				WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
				WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
				ELSE 'n/a'
			END AS cst_marital_status, -- Normalize marital status values to readable format
			CASE 
				WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				ELSE 'n/a'
			END AS cst_gndr, -- Normalize gender values to readable format
			cst_create_date
		FROM (
			SELECT
				*,
				ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL
		) t
		WHERE flag_last = 1; -- Select the most recent record per customer

		--=========================================================================================================
		print '>> Truncating table: silver.crm_prd_info'
		truncate table silver.crm_prd_info
		print '>> Inserting data into: silver.crm_prd_info'
		INSERT INTO silver.crm_prd_info (
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
				)
		SELECT
			prd_id,
			REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, -- Extract category ID
			SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,        -- Extract product key
			prd_nm,
			ISNULL(prd_cost, 0) AS prd_cost,
			CASE 
				WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
				WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
				WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
				WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
				ELSE 'n/a'
			END AS prd_line, -- Map product line codes to descriptive values
			CAST(prd_start_dt AS DATE) AS prd_start_dt,
			CAST(
				DATEADD(DAY, -1, LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt))
				AS DATE
			) AS prd_end_dt -- Calculate end date as one day before the next start date
		FROM 
			bronze.crm_prd_info;

		--=========================================================================================================
		print '>> Truncating table: silver.crm_sales_details'
		truncate table silver.crm_sales_details
		print '>> Inserting data into: silver.crm_sales_details'
		insert into silver.crm_sales_details(
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

		select
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			case when sls_order_dt=0 or len(sls_order_dt) != 8 then null
				else cast(cast(sls_order_dt as varchar) as date)
			end as sls_order_dt,

			case when sls_ship_dt=0 or len(sls_ship_dt) != 8 then null
				else cast(cast(sls_ship_dt as varchar) as date)
			end as sls_ship_dt,

			case when sls_due_dt=0 or len(sls_due_dt) != 8 then null
				else cast(cast(sls_due_dt as varchar) as date)
			end as sls_due_dt,

			case when sls_sales is null or sls_sales <=0 or sls_sales!=sls_quantity*abs(sls_price) then sls_quantity*abs(sls_price)
				else sls_sales
			end as sls_sales,

			sls_quantity,

			case when sls_price is null or sls_price<=0 then sls_sales/ nullif(sls_quantity, 0)
				else sls_price
			end as sls_price

		from 
			bronze.crm_sales_details

		--=========================================================================================
		print '>> Truncating table: silver.erp_CUST_AZ12'
		truncate table silver.erp_CUST_AZ12
		print '>> Inserting data into: silver.erp_CUST_AZ12'
		insert into silver.erp_CUST_AZ12(
			CID,
			BDATE,
			GEN
		)

		select 
			case when cid like 'NAS%' then SUBSTRING(cid, 4, len(cid))
				else cid
			end as cid,

			case when bdate>getdate() then null
				else bdate
			end as bdate,

			case when upper(trim(gen)) in ('F', 'FEMALE') then 'Female'
				when upper(trim(gen)) in ('M', 'MALE') then 'Male'
				else 'n/a'
			end as gen

		from
			bronze.erp_CUST_AZ12

		--===================================================================================
		print '>> Truncating table: silver.erp_LOC_A101'
		truncate table silver.erp_LOC_A101
		print '>> Inserting data into: silver.erp_LOC_A101'
		insert into silver.erp_LOC_A101(
			CID,
			CNTRY
		)

		select 
			replace(CID, '-', '') cid,
			case when trim(cntry) = 'DE' then 'Germany'
				when trim(cntry) in ('US','USA') then 'United States'
				when trim(cntry) = '' or cntry is null then 'n/a'
				else trim(cntry)
			end cntry
		from
			bronze.erp_LOC_A101

		--====================================================================================
		print '>> Truncating table: silver.erp_PX_CAT_G1V2'
		truncate table silver.erp_PX_CAT_G1V2
		print '>> Inserting data into: silver.erp_PX_CAT_G1V2'
		insert into silver.erp_PX_CAT_G1V2(
			ID,
			CAT,
			SUBCAT,
			MAINTENANCE
		)

		select 
			ID,
			CAT,
			SUBCAT,
			MAINTENANCE
		from
			bronze.erp_PX_CAT_G1V2

		end try

		begin catch
			PRINT '=========================================='
			PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
			PRINT 'Error Message' + ERROR_MESSAGE();
			PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
			PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
			PRINT '=========================================='
		end catch
end
