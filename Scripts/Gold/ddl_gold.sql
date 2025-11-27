/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/



USE DataWarehouse;
GO


-- =============================================================================
-- Create Dimension: gold.dim_customers
-- ============================================================================= 


IF OBJECT_ID ('gold.dim_customers' ,'V') IS NOT NULL 
DROP VIEW  gold.dim_customers;
GO

CREATE VIEW gold.dim_customers AS 
	(
		SELECT 
			ROW_NUMBER() OVER (ORDER BY customer_id ) AS customer_key,
 			customer_id,
			first_name,
			last_name,
			email,
			phone,
			gender,
			city,
			age,
			income_level,
			loyalty_score,
			segment,
			preferred_device,
			marital_status,created_at
		FROM silver.customers
		)

GO


-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================

IF OBJECT_ID ('gold.dim_product' ,'V') IS NOT NULL 
DROP VIEW  gold.dim_product;

GO

CREATE VIEW gold.dim_product AS 

(
	SELECT 
		ROW_NUMBER() OVER (ORDER BY product_id ) AS product_key,
		product_id,
		product_name,
		category,
		brand,
		price,
		rating,
		stock,
		weight_g,
		color,
		created_at

	FROM silver.products 
	
	)

GO


-- =============================================================================
-- Create Dimension: gold.dim_payments
-- =============================================================================

IF OBJECT_ID ('gold.dim_payments' ,'V') IS NOT NULL 
DROP VIEW  gold.dim_payments;

GO


CREATE VIEW  gold.dim_payments AS

		

		WITH payment AS

				(
				SELECT 
							ROW_NUMBER() OVER (ORDER BY p.payment_id ) AS payment_key,
							ROW_NUMBER() OVER (PARTITION BY p.order_id  ORDER BY p.order_id) AS rank_,
							p.payment_id,
							p.order_id,
							p.payment_method,
							p.payment_gateway,
							p.payment_status,
							p.currency,
							p.exchange_rate,
							p.created_at 
							
				FROM silver.payments p
				JOIN silver.orders o ON p.order_id = o.order_id
		
				 )


				SELECT      payment_key,
							payment_id,
							order_id,
							payment_method,
							payment_gateway,
							payment_status,
							currency,
							exchange_rate,
							created_at 
				FROM payment
				WHERE rank_ = 1
						
GO


-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================


IF OBJECT_ID ('gold.fact_order_sales' ,'V') IS NOT NULL 
DROP VIEW  gold.fact_order_sales;
GO

CREATE VIEW gold.fact_order_sales
AS
WITH sales AS (
    SELECT 
        o.order_id,
		c.customer_key,
        i.order_item_id,
		p.product_key,
        o.order_status,
        o.shipping_method,
        pay.payment_key,
        o.shipping_fee,
        o.payment_terms,
        i.fulfilled_by,
        i.quantity,
        i.unit_price,
        i.tax,
        i.discount_amount,
        (i.unit_price * i.quantity) 
            + (o.shipping_fee + i.tax) 
            - i.discount_amount AS order_amount
    FROM silver.orders o 
   LEFT JOIN silver.order_items i 
        ON o.order_id = i.order_id
		LEFT JOIN gold.dim_product p ON  i.product_id = p.product_id
		LEFT JOIN gold.dim_customers c ON o.customer_id =  c.customer_id
		LEFT JOIN gold.dim_payments pay ON pay.order_id =  o.order_id
)
SELECT 
		order_id,
		customer_key,
		payment_key,
        order_item_id,
		product_key,
        order_status,
        shipping_method,
        
        shipping_fee,
        payment_terms,
        fulfilled_by,
        quantity,
        unit_price,
        tax,
        order_amount
FROM sales;








