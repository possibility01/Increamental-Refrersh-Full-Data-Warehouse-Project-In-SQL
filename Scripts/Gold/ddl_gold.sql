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

	FROM bronze.products
	)

CREATE VIEW AS gold.dim_payments AS

		(

		SELECT 
			ROW_NUMBER() OVER (ORDER BY payment_id ) AS payment_id,
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