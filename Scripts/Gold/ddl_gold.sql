IF OBJECT_ID ('gold.dim_customers' ,'V') IS NOT NULL 
DROP VIEW  gold.dim_customers;

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


IF OBJECT_ID ('gold.dim_product' ,'V') IS NOT NULL 
DROP VIEW  gold.dim_product;

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


IF OBJECT_ID ('gold.dim_payments' ,'V') IS NOT NULL 
DROP VIEW  gold.dim_payments;

CREATE VIEW  gold.dim_payments AS

		

		WITH payment AS

				(
				SELECT 
							ROW_NUMBER() OVER (ORDER BY payment_id ) AS payment_key,
							ROW_NUMBER() OVER (PARTITION BY order_id  ORDER BY order_id) AS rank_,
							payment_id,
							order_id,
							payment_method,
							payment_gateway,
							payment_status,
							currency,
							exchange_rate,
							created_at 
							
				FROM silver.payments
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
						


IF OBJECT_ID ('gold.fact_order_sales' ,'V') IS NOT NULL 
DROP VIEW  gold.fact_order_sales;
GO

CREATE VIEW gold.fact_order_sales
AS
WITH sales AS (
    SELECT 
        ROW_NUMBER() OVER (
            PARTITION BY i.order_item_id 
            ORDER BY i.order_item_id
        ) AS rank_,
        o.order_id,
        o.customer_id,
        i.order_item_id,
		i.product_id,
        o.order_status,
        o.shipping_method,
        
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
    JOIN silver.order_items i 
        ON o.order_id = i.order_id
)
SELECT 
		order_id,
        customer_id,
        order_item_id,
		product_id,
        order_status,
        shipping_method,
        
        shipping_fee,
        payment_terms,
        fulfilled_by,
        quantity,
        unit_price,
        tax,
        order_amount
FROM sales
WHERE rank_ = 1;


SELECT * FROM gold.fact_order_sales

SELECT  * FROM gold.dim_payments
SELECT  * FROM silver.payments



	WITH payment AS

				(
				SELECT 
							ROW_NUMBER() OVER (ORDER BY payment_id ) AS payment_key,
							ROW_NUMBER() OVER (PARTITION order_id ) AS rank_,
							payment_id,
							order_id,
							payment_method,
							payment_gateway,
							payment_status,
							currency,
							exchange_rate,
							created_at 
							
				FROM silver.payments
				WHERE order_
				 )


				SELECT      payment_key,
							rank_,
							payment_id,
							order_id,
							payment_method,
							payment_gateway,
							payment_status,
							currency,
							exchange_rate,
							created_at 
				FROM payment



				SELECT order_id , count(*) FROM silver.payments
				GROUP BY order_id
				HAVING count(*) > 1
				