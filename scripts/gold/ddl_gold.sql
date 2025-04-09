 if object_id('gold.dim_customers', 'V') is not null
	drop view gold.dim_customers;
go
 create view gold.dim_customers as 
 select
	row_number() over(order by cst_id) as customer_key,
	 ci.cst_id as customer_id,
	 ci.cst_key as customer_number,
	 ci.cst_firstname as first_name,
	 ci.cst_lastname as last_name,
	 la.CNTRY as country,
	 ci.cst_marital_status as marital_status,
	 case when ci.cst_gndr !='n/a' then ci.cst_gndr
		else coalesce(ca.gen, 'n/a')
	 end as gender,
	 ca.BDATE as birth_date,
	 ci.cst_create_date as create_date
 from
	 silver.crm_cust_info as ci
	 left join silver.erp_CUST_AZ12 as ca
	 on ci.cst_key = ca.CID
	 left join silver.erp_LOC_A101 la
	 on ci.cst_key = la.CID

--=============================================================================
if object_id('gold.dim_products', 'V') is not null
	drop view gold.dim_products;
go
create view gold.dim_products as
select
	row_number() over(order by pn.prd_start_dt, pn.prd_key) as product_key,
	pn.prd_id as product_id,
	pn.prd_key as product_number,
	pn.prd_nm as product_name,
	pn.cat_id as category_id,
	pc.cat as category,
	pc.SUBCAT as subcategory,
	pc.MAINTENANCE as maintenance,
	pn.prd_cost as cost,
	pn.prd_line as product_line,
	pn.prd_start_dt as start_date

from silver.crm_prd_info pn
	left join silver.erp_PX_CAT_G1V2 as pc
	on pn.cat_id = pc.ID
	where pn.prd_end_dt is null --filters out all historical data

--=========================================================================
 if object_id('gold.fact_sales', 'V') is not null
	drop view gold.fact_sales;
go
create view gold.fact_sales as 
select
	sd.sls_ord_num as order_number,
	pr.product_key,
	cu.customer_key,
	sd.sls_order_dt as order_date,
	sd.sls_ship_dt as shipping_date,
	sd.sls_due_dt as due_date,
	sd.sls_sales as sales_amount,
	sd.sls_quantity as quantity,
	sd.sls_price as price

from silver.crm_sales_details as sd
left join gold.dim_products as pr
on sd.sls_prd_key = pr.product_number
left join gold.dim_customers as cu
on sd.sls_cust_id = cu.customer_id

