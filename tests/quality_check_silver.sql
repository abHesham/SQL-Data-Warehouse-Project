/* -------------quality issues in crm_cust_info (checking)--------------*/
use DataWareHouse
go


-- (1)
-- check for nulls or duplicates in primary key
-- expectation: no result

select cst_id,
count(*)
from bronze.crm_cust_info
group by cst_id
having count(*) > 1 or cst_id is null


-- (2)
-- check for unwanted spaces
-- expectation: no result

select cst_firstname
from bronze.crm_cust_info
where cst_firstname != trim(cst_firstname)


-- (3)
--check distinct in cst_material_status
select distinct cst_material_status
from bronze.crm_cust_info


-- (4)
--check distinct in cst_gndr
select cst_gndr,
count(*)
from bronze.crm_cust_info 
group by cst_gndr 
having count(*) >=1





/* -------------quality issues in crm_prd_info (checking)--------------*/

use DataWareHouse
go


-- (1)
-- check for nulls or duplicates in primary key
-- expectation: no result
select prd_id,
count(*)
from silver.crm_prd_info
group by prd_id
having count(*) > 1


-- (2)
-- check for unwanted spaces
-- expectation: no result
select prd_nm
from silver.crm_prd_info
where prd_nm != trim(prd_nm)


-- (3)
-- check for nulls or negative numbers
-- expectation: no result
select prd_cost
from silver.crm_prd_info
where prd_cost < 0 or prd_cost is null


-- (4)
-- data normalization & consistency
select distinct prd_line
from silver.crm_prd_info


-- (5)
--check for invalid date orders
select * 
from silver.crm_prd_info
where prd_end_dt < prd_start_dt




/* -------------quality issues in sales_details (checking)--------------*/




-- (1)
-- check for nulls or duplicates in primary key
-- expectation: no result
select sls_ord_num
from bronze.crm_sales_details
where sls_ord_num != trim(sls_ord_num)


-- (2)
-- check date quality issues
select sls_due_dt
from bronze.crm_sales_details
where sls_due_dt <= 0 or sls_due_dt is null


-- (3)
--check date quality issues 2
select sls_due_dt
from bronze.crm_sales_details
where len(sls_due_dt) != 8


-- (4)
--check for invalid date orders
select *
from bronze.crm_sales_details
where sls_order_dt > sls_ship_date or sls_order_dt > sls_due_dt


-- (5)
-- check sales, prices, quantity issues
select distinct
sls_sales, 
sls_quantity,
sls_price
from silver.crm_sales_details
where sls_sales != sls_quantity * sls_price
or sls_sales is null or sls_quantity is null or sls_price is null
or sls_sales <= 0 or sls_quantity <= 0 or sls_price <= 0
order by sls_sales




/* -------------quality issues in erp_CUST_AZ12 (checking)--------------*/
use DataWareHouse
go


-- (1)
-- IDENTIFY OUT OF RANGE BDATES
select distinct 
bdate from bronze.erp_CUST_AZ12
where bdate > getdate()


-- (2)
-- check gender 
select distinct
gen 
from bronze.erp_CUST_AZ12





/* -------------quality issues in LOC_A101 (checking)--------------*/
use DataWareHouse
go


-- (1) check unwanted spaces 
select distinct 
cntry
from bronze.erp_LOC_A101






/* -------------quality issues in erp_PX_CAT_G1V2 (checking)--------------*/
use DataWareHouse
go

-- (1)
-- check unwanted spaces
select maintenance 
from bronze.erp_PX_CAT_G1V2 
where maintenance != trim(maintenance)


-- (2)
-- check cardinality
select distinct maintenance
from bronze.erp_PX_CAT_G1V2



