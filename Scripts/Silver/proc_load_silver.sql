
USE DataWarehouse;
GO

CREATE OR ALTER PROCEDURE  silver.silver_control_table AS
BEGIN
	IF OBJECT_ID('silver.control_table','U') IS NULL
	BEGIN
		CREATE TABLE silver.control_table (
											table_name NVARCHAR(50),
											last_ingestion_datetime DATETIME,
											last_batch_id NVARCHAR(50)
											)

		INSERT INTO  silver.control_table (table_name , last_ingestion_datetime,last_batch_id) 
		VALUES
			 ('customers' ,'2000-01-01',NULL),
			 ('products' ,'2000-01-01',NULL),
			 ('orders' ,'2000-01-01',NULL),
			 ('payments' ,'2000-01-01',NULL),
			 ('order_items' ,'2000-01-01',NULL);

	END
	ELSE 
		BEGIN
		 PRINT '>>> silver control table already exists. No changes made!!'
	END

END
GO



CREATE OR ALTER PROCEDURE  silver.inital_increamental_load AS
BEGIN
	DECLARE  @batch_id NVARCHAR(50)= FORMAT(GETDATE(), 'yyyyMMdd_HHmm'),@last_ingestion_datetime DATETIME;

	SELECT  @last_ingestion_datetime = last_ingestion_datetime 
	FROM silver.control_table
	WHERE table_name = 'customers';

	IF @last_ingestion_datetime <= '2000-01-01'
	
		BEGIN
	
			INSERT INTO silver.customers (
											   customer_id	,
											   first_name ,
											   last_name ,
											   email ,
											   phone ,	
											   gender	,
											   city	,                          
											   age,
											   income_level	,
											   loyalty_score ,
											   segment,
											   preferred_device,
											   marital_status ,
											   created_at,
											   updated_at ,
											   is_deleted,
											   batch_id
											   )



				SELECT 
					customer_id,
					REPLACE(LOWER(TRIM(SUBSTRING(first_name,1,5))),'1','i')  +
					REPLACE(LOWER(TRIM(SUBSTRING (first_name,6,LEN(first_name)))),'@','a') AS first_name ,
					REPLACE(LOWER(TRIM(last_name)),'@','a') AS last_name,
					CASE 
						WHEN email IS NULL THEN 'N/A'
					ELSE  LEFT(LOWER(email), CHARINDEX('@', email) - 1) -- username
						+ '@' +
						REPLACE(SUBSTRING(LOWER(email), CHARINDEX('@', email) + 1, LEN(email)), '@', '')  END AS email ,

					CASE 
						WHEN phone IS NULL THEN 'N/A'
					ELSE phone END phone ,
					CASE 
						WHEN gender IS NULL THEN 'N/A'
					ELSE REPLACE(TRIM(LOWER(gender)),'@','a') END gender,
					CASE 
						WHEN city IS NULL THEN 'N/A'
					ELSE REPLACE(TRIM(LOWER(city)),'@','a') END city ,
					CASE 
						WHEN age IS NULL THEN 'N/A'
					ELSE REPLACE(TRIM(LOWER(age)),'@','a') END age,

					CASE 
						WHEN income_level IS NULL THEN 'N/A'
					ELSE (TRIM(LOWER(income_level))) END income_level,
					CASE 
						WHEN loyalty_score IS NULL THEN 'N/A'
					ELSE (TRIM(LOWER(loyalty_score))) END loyalty_score ,


					CASE 
						WHEN segment IS NULL THEN 'N/A'
					ELSE REPLACE(TRIM(LOWER(segment)),'@','a') END segment,
					CASE 
						WHEN preferred_device IS NULL THEN 'N/A'
					ELSE REPLACE(TRIM(LOWER(preferred_device)),'@','a') END preferred_device,
					CASE 
						WHEN marital_status IS NULL THEN 'N/A'
					ELSE REPLACE(TRIM(LOWER(marital_status)),'@','a') END marital_status,
					created_at,
					updated_at,
					is_deleted,
					batch_id = @batch_id
	



				FROM bronze.customers
		END
		ELSE 
			BEGIN

				INSERT INTO silver.customers (
											   customer_id	,
											   first_name ,
											   last_name ,
											   email ,
											   phone ,	
											   gender	,
											   city	,                          
											   age,
											   income_level	,
											   loyalty_score ,
											   segment,
											   preferred_device,
											   marital_status ,
											   created_at,
											   updated_at ,
											   is_deleted,
											   batch_id
											   )



				SELECT 
					customer_id,
					REPLACE(LOWER(TRIM(SUBSTRING(first_name,1,5))),'1','i')  +
					REPLACE(LOWER(TRIM(SUBSTRING (first_name,6,LEN(first_name)))),'@','a') AS first_name ,
					REPLACE(LOWER(TRIM(last_name)),'@','a') AS last_name,
					CASE 
						WHEN email IS NULL THEN 'N/A'
					ELSE  LEFT(LOWER(email), CHARINDEX('@', email) - 1) -- username
						+ '@' +
						REPLACE(SUBSTRING(LOWER(email), CHARINDEX('@', email) + 1, LEN(email)), '@', '')  END AS email ,

					CASE 
						WHEN phone IS NULL THEN 'N/A'
					ELSE phone END phone ,
					CASE 
						WHEN gender IS NULL THEN 'N/A'
					ELSE REPLACE(TRIM(LOWER(gender)),'@','a') END gender,
					CASE 
						WHEN city IS NULL THEN 'N/A'
					ELSE REPLACE(TRIM(LOWER(city)),'@','a') END city ,
					CASE 
						WHEN age IS NULL THEN 'N/A'
					ELSE REPLACE(TRIM(LOWER(age)),'@','a') END age,

					CASE 
						WHEN income_level IS NULL THEN 'N/A'
					ELSE (TRIM(LOWER(income_level))) END income_level,
					CASE 
						WHEN loyalty_score IS NULL THEN 'N/A'
					ELSE (TRIM(LOWER(loyalty_score))) END loyalty_score ,


					CASE 
						WHEN segment IS NULL THEN 'N/A'
					ELSE REPLACE(TRIM(LOWER(segment)),'@','a') END segment,
					CASE 
						WHEN preferred_device IS NULL THEN 'N/A'
					ELSE REPLACE(TRIM(LOWER(preferred_device)),'@','a') END preferred_device,
					CASE 
						WHEN marital_status IS NULL THEN 'N/A'
					ELSE REPLACE(TRIM(LOWER(marital_status)),'@','a') END marital_status,
					created_at,
					updated_at,
					is_deleted,
					batch_id = @batch_id
	
				FROM bronze.customers
				WHERE TRY_CAST (updated_at AS DATETIME) > @last_ingestion_datetime ;
				
			END
			-- Update control table for this table
				UPDATE silver.control_table
				SET last_ingestion_datetime = GETDATE(),
					last_batch_id = @batch_id
				WHERE table_name = 'customers';



	SELECT  @last_ingestion_datetime = last_ingestion_datetime 
	FROM silver.control_table
	WHERE table_name = 'products';
	IF @last_ingestion_datetime <= '2000-01-01'
	
		BEGIN
	
			INSERT INTO silver.products(
										  product_id,
										  product_name,
										  category,
										  brand,
										  price,
										  discount,
										  rating,
										  stock,
										  weight_g ,
										  color,
										  created_at
										  ,updated_at,
										  is_deleted,
										  batch_id
											   )



				SELECT  
					product_id ,
					CASE WHEN
						product_name  IS NULL THEN 'N/A'
					ELSE REPLACE(LOWER(TRIM(product_name)),'@','a') END product_name ,
					CASE WHEN
						category  IS NULL THEN 'N/A'
					ELSE REPLACE(LOWER(TRIM(category)),'@','a') END category,
					CASE WHEN
						brand  IS NULL THEN 'N/A'
					ELSE REPLACE(LOWER(TRIM(brand)),'@','a') END brand,
					price,
					discount,
					rating,
					stock,
					weight_g,
					CASE WHEN
						color  IS NULL THEN 'N/A'
					ELSE REPLACE(LOWER(TRIM(color)),'@','a') END color,
					created_at,
					updated_at,
					is_deleted,
					batch_id = @batch_id


				FROM bronze.products
		END
		ELSE 
			BEGIN

				INSERT INTO silver.products(
										  product_id,
										  product_name,
										  category,
										  brand,
										  price,
										  discount,
										  rating,
										  stock,
										  weight_g ,
										  color,
										  created_at
										  ,updated_at,
										  is_deleted,
										  batch_id
											   )



				SELECT 

					product_id ,
					CASE WHEN
						product_name  IS NULL THEN 'N/A'
					ELSE REPLACE(LOWER(TRIM(product_name)),'@','a') END product_name ,
					CASE WHEN
						category  IS NULL THEN 'N/A'
					ELSE REPLACE(LOWER(TRIM(category)),'@','a') END category,
					CASE WHEN
						brand  IS NULL THEN 'N/A'
					ELSE REPLACE(LOWER(TRIM(brand)),'@','a') END brand,
					price,
					discount,
					rating,
					stock,
					weight_g,
					CASE WHEN
						color  IS NULL THEN 'N/A'
					ELSE REPLACE(LOWER(TRIM(color)),'@','a') END color,
					created_at,
					updated_at,
					is_deleted,
					batch_id = @batch_id


				FROM bronze.products
				WHERE TRY_CAST (updated_at AS DATETIME) > @last_ingestion_datetime ;
				
			END
			-- Update control table for this table
				UPDATE silver.control_table
				SET last_ingestion_datetime = GETDATE(),
					last_batch_id = @batch_id
				WHERE table_name = 'products';



	SELECT  @last_ingestion_datetime = last_ingestion_datetime 
	FROM silver.control_table
	WHERE table_name = 'payments';
	IF @last_ingestion_datetime <= '2000-01-01'
	
		BEGIN
	
			INSERT INTO silver.payments(
										payment_id,
										order_id,
										amount,
										payment_method,
										payment_gateway ,
										payment_status,
										currency,
										exchange_rate,
										created_at,
										updated_at,
										batch_id 
											   )



				SELECT 
					payment_id,
					order_id,
					amount,
					CASE WHEN
						payment_method  IS NULL THEN 'N/A'
					ELSE REPLACE(LOWER(TRIM(payment_method)),'@','a') END payment_method,
					CASE WHEN
						payment_gateway  IS NULL THEN 'N/A'
					ELSE REPLACE(LOWER(TRIM(payment_gateway)),'@','a') END payment_gateway,
					CASE WHEN
						payment_status  IS NULL THEN 'N/A'
					ELSE REPLACE(LOWER(TRIM(payment_status)),'@','a') END payment_status,
					CASE WHEN
						currency  IS NULL THEN 'N/A'
					ELSE REPLACE(UPPER(TRIM(currency)),'@','a') END currency,
					exchange_rate,
					created_at,
					updated_at,
					batch_id = @batch_id
				FROM bronze.payments


				
		END
		ELSE 
			BEGIN

				INSERT INTO silver.payments(
										payment_id,
										order_id,
										amount,
										payment_method,
										payment_gateway ,
										payment_status,
										currency,
										exchange_rate,
										created_at,
										updated_at,
										batch_id 
											   )



				SELECT 
					payment_id,
					order_id,
					amount,
					CASE WHEN
						payment_method  IS NULL THEN 'N/A'
					ELSE REPLACE(LOWER(TRIM(payment_method)),'@','a') END payment_method,
					CASE WHEN
						payment_gateway  IS NULL THEN 'N/A'
					ELSE REPLACE(LOWER(TRIM(payment_gateway)),'@','a') END payment_gateway,
					CASE WHEN
						payment_status  IS NULL THEN 'N/A'
					ELSE REPLACE(LOWER(TRIM(payment_status)),'@','a') END payment_status,
					CASE WHEN
						currency  IS NULL THEN 'N/A'
					ELSE REPLACE(UPPER(TRIM(currency)),'@','a') END currency,
					exchange_rate,
					created_at,
					updated_at,
					batch_id = @batch_id
				FROM bronze.payments
				WHERE TRY_CAST (updated_at AS DATETIME) > @last_ingestion_datetime ;
				
			END
			-- Update control table for this table
				UPDATE silver.control_table
				SET last_ingestion_datetime = GETDATE(),
					last_batch_id = @batch_id
				WHERE table_name = 'payments';


	SELECT  @last_ingestion_datetime = last_ingestion_datetime 
	FROM silver.control_table
	WHERE table_name = 'order_items';
	IF @last_ingestion_datetime <= '2000-01-01'
	
		BEGIN
	
			INSERT INTO silver.order_items(
										order_item_id ,
										order_id,
										product_id ,
										quantity,
										unit_price ,
										tax,
									    discount_amount ,
									    fulfilled_by,    
									    created_at ,
									    updated_at ,
									    batch_id 
											   )



				SELECT 
					order_item_id,
					order_id,
					product_id,
					quantity,
					unit_price,
					tax,
					discount_amount,
					CASE WHEN
							fulfilled_by  IS NULL THEN 'N/A'
					ELSE REPLACE(LOWER(TRIM(fulfilled_by)),'@','a') END fulfilled_by,
					created_at,
					updated_at,
					batch_id = @batch_id
				FROM bronze.order_items


				
		END
		ELSE 
			BEGIN

				INSERT INTO silver.order_items(
										order_item_id ,
										order_id,
										product_id ,
										quantity,
										unit_price ,
										tax,
									    discount_amount ,
									    fulfilled_by,    
									    created_at ,
									    updated_at ,
									    batch_id 
											   )



				SELECT 
					order_item_id,
					order_id,
					product_id,
					quantity,
					unit_price,
					tax,
					discount_amount,
					CASE WHEN
							fulfilled_by  IS NULL THEN 'N/A'
					ELSE REPLACE(LOWER(TRIM(fulfilled_by)),'@','a') END fulfilled_by,
					created_at,
					updated_at,
					batch_id = @batch_id
				FROM bronze.order_items
				WHERE TRY_CAST (updated_at AS DATETIME) > @last_ingestion_datetime ;
				
			END
			-- Update control table for this table
				UPDATE silver.control_table
				SET last_ingestion_datetime = GETDATE(),
					last_batch_id = @batch_id
				WHERE table_name = 'order_items';


END
GO

