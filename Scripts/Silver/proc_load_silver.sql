
/*
================================================================================
SILVER LAYER – INCREMENTAL LOAD STORED PROCEDURES
================================================================================

This script defines two stored procedures used to manage the Silver layer of a 
Data Warehouse/Lakehouse architecture. The Silver layer contains cleansed, 
standardized, and conformed data derived from the Bronze (raw) layer.

--------------------------------------------------------------------------------
1. Procedure: silver.silver_control_table
--------------------------------------------------------------------------------
Purpose:
    Creates a metadata control table (silver.control_table) used for tracking the
    last successful ingestion timestamp and batch ID for incremental processing.

Behavior:
    - If the control table DOES NOT exist:
        * Creates silver.control_table with columns:
              • table_name               : Table being tracked
              • last_ingestion_datetime : Last load timestamp
              • last_batch_id           : Identifier for last batch
        * Inserts default records for:
              customers, products, orders, payments, order_items
        * Sets:
              last_ingestion_datetime = '2000-01-01'
              last_batch_id = NULL
    - If the control table ALREADY exists:
        * Prints a message and makes no changes.

Role in ETL:
    This table ensures idempotency and allows incremental loads by maintaining a 
    timestamp of the last ingestion per table.

--------------------------------------------------------------------------------
2. Procedure: silver.initial_incremental_load
--------------------------------------------------------------------------------
Purpose:
    Performs **initial** or **incremental** loading of tables from the Bronze 
    schema into the Silver schema, based on metadata recorded in the control table.

High-Level Logic:
    a. Generate a unique batch ID using current timestamp (format: yyyyMMdd_HHmm).
    b. For each table (customers, products, orders, order_items, payments):
         1. Retrieve last_ingestion_datetime from silver.control_table.
         2. Determine load type:
              - If last_ingestion_datetime <= '2000-01-01' ? FULL INITIAL LOAD
              - Else ? INCREMENTAL LOAD (rows where updated_at > last_ingestion_datetime)
         3. Perform MERGE INTO silver.<table>:
              - UPDATE existing rows
              - INSERT new rows
         4. Apply data cleansing rules:
              • Lowercasing + trimming text
              • Removing unwanted characters (e.g., '@', '1')
              • Standardizing NULLs ? 'N/A'
              • Normalizing email structure
              • Enforcing valid defaults (e.g., quantity = 1 when null/zero)
         5. Assign batch_id to each processed row.
         6. Update silver.control_table with:
              • last_ingestion_datetime = GETDATE()
              • last_batch_id = batch_id

ETL Benefits:
    - Supports scalable incremental refresh of Silver tables.
    - Ensures auditability and traceability via batch IDs.
    - Eliminates duplicates and ensures consistent transformations.
    - Ideal for scheduled pipeline automation in SQL Server Agent or orchestration tools.

--------------------------------------------------------------------------------
Notes:
    - Bronze layer contains raw ingested data.
    - Silver layer holds cleaned, standardized, analytics-ready datasets.
    - This pattern follows modern Data Lakehouse incremental ingestion design.
================================================================================
*/

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


CREATE OR ALTER PROCEDURE  silver.inital_incremental_load AS
BEGIN
	DECLARE  @batch_id NVARCHAR(50)= FORMAT(GETDATE(), 'yyyyMMdd_HHmm'),@last_ingestion_datetime DATETIME;

	SELECT  @last_ingestion_datetime  =  last_ingestion_datetime 
	FROM silver.control_table
	WHERE table_name = 'customers';

	MERGE silver.customers AS tar
		USING (
			SELECT * FROM bronze.customers
			WHERE TRY_CAST(updated_at AS DATETIME) > @last_ingestion_datetime
           OR @last_ingestion_datetime <= '2000-01-01'  -- for initial load
				) AS src 
		ON tar.customer_id = src.customer_id

		WHEN MATCHED THEN 
			   UPDATE SET 
						tar.first_name = REPLACE(LOWER(TRIM(SUBSTRING(src.first_name,1,5))), '1', 'i') +
                                REPLACE(LOWER(TRIM(SUBSTRING(src.first_name,6,LEN(src.first_name)))), '@', 'a'),
            tar.last_name = REPLACE(LOWER(TRIM(src.last_name)), '@', 'a'),
            tar.email = CASE
                                WHEN src.email IS NULL THEN 'N/A'
                                ELSE LEFT(LOWER(src.email), CHARINDEX('@', src.email) - 1) 
                                     + '@' +
                                     REPLACE(SUBSTRING(LOWER(src.email), CHARINDEX('@', src.email) + 1, LEN(src.email)), '@', '')
                            END,
            tar.phone = CASE 
						WHEN src.phone IS NULL THEN 'N/A'
					ELSE src.phone END ,
            tar.gender = CASE 
						WHEN src.gender IS NULL THEN 'N/A'
					ELSE REPLACE(TRIM(LOWER(src.gender)),'@','a') END ,
            tar.city = CASE 
						WHEN src.city IS NULL THEN 'N/A'
					ELSE REPLACE(TRIM(LOWER(src.city)),'@','a') END,
            tar.age = CASE 
						WHEN src.age IS NULL THEN 0
					ELSE (src.age) END,
            tar.income_level = CASE 
						WHEN src.income_level IS NULL THEN 'N/A'
					ELSE (TRIM(LOWER(src.income_level))) END,
            tar.loyalty_score = CASE 
						WHEN src.loyalty_score IS NULL THEN 0
					ELSE (src.loyalty_score) END,
            tar.segment = CASE 
						WHEN src.segment IS NULL THEN 'N/A'
					ELSE REPLACE(TRIM(LOWER(src.segment)),'@','a') END,
            tar.preferred_device = CASE 
						WHEN src.segment IS NULL THEN 'N/A'
					ELSE REPLACE(TRIM(LOWER(src.segment)),'@','a') END,
            tar.marital_status = CASE 
						WHEN src.marital_status IS NULL THEN 'N/A'
					ELSE REPLACE(TRIM(LOWER(src.marital_status)),'@','a') END,
            tar.created_at = src.created_at,
            tar.updated_at = src.updated_at,
            tar.batch_id = @batch_id
	
	WHEN NOT MATCHED BY TARGET THEN
			INSERT (customer_id, first_name, last_name, email, phone, gender, city, age, income_level,
					loyalty_score, segment, preferred_device, marital_status, created_at, updated_at, batch_id)
			VALUES (
				src.customer_id,
				REPLACE(LOWER(TRIM(SUBSTRING(src.first_name,1,5))), '1', 'i') +
				REPLACE(LOWER(TRIM(SUBSTRING(src.first_name,6,LEN(src.first_name)))), '@', 'a'),
				REPLACE(LOWER(TRIM(src.last_name)), '@', 'a'),
				CASE
					WHEN src.email IS NULL THEN 'N/A'
					ELSE LEFT(LOWER(src.email), CHARINDEX('@', src.email) - 1) 
						 + '@' +
						 REPLACE(SUBSTRING(LOWER(src.email), CHARINDEX('@', src.email) + 1, LEN(src.email)), '@', '')
				END,
				CASE 
						WHEN src.phone IS NULL THEN 'N/A'
					ELSE src.phone END,
				CASE 
						WHEN src.gender IS NULL THEN 'N/A'
					ELSE REPLACE(TRIM(LOWER(src.gender)),'@','a') END,
				CASE 
						WHEN src.city IS NULL THEN 'N/A'
					ELSE REPLACE(TRIM(LOWER(src.city)),'@','a') END,
				src.age,
				CASE 
						WHEN src.income_level IS NULL THEN 'N/A'
					ELSE (TRIM(LOWER(src.income_level))) END,
				loyalty_score,
				CASE 
						WHEN src.segment IS NULL THEN 'N/A'
					ELSE REPLACE(TRIM(LOWER(src.segment)),'@','a') END,
				CASE 
						WHEN src.segment IS NULL THEN 'N/A'
					ELSE REPLACE(TRIM(LOWER(src.segment)),'@','a') END,
				CASE 
						WHEN src.marital_status IS NULL THEN 'N/A'
					ELSE REPLACE(TRIM(LOWER(src.marital_status)),'@','a') END,
				src.created_at,
				src.updated_at,
				@batch_id
			);

		-- Update control table
		UPDATE silver.control_table
		SET last_ingestion_datetime = GETDATE(),
			last_batch_id = @batch_id
		WHERE table_name = 'customers';	

	SELECT  @last_ingestion_datetime  =  last_ingestion_datetime 
	FROM silver.control_table
	WHERE table_name = 'products';

	MERGE silver.products AS tar
		USING (
			SELECT * FROM bronze.products
			WHERE TRY_CAST(updated_at AS DATETIME) > @last_ingestion_datetime
           OR @last_ingestion_datetime <= '2000-01-01'  -- for initial load
				) AS src 
		ON tar.product_id = src.product_id

		WHEN MATCHED THEN 
			   UPDATE SET 
						tar.product_id =src.product_id,
						tar.product_name= CASE WHEN
											src.product_name  IS NULL THEN 'N/A'
										ELSE REPLACE(LOWER(TRIM(src.product_name)),'@','a') END ,
						tar.category = CASE WHEN
											src.category  IS NULL THEN 'N/A'
										ELSE REPLACE(LOWER(TRIM(src.category)),'@','a') END ,
						tar.brand =	CASE WHEN
											src.brand  IS NULL THEN 'N/A'
									ELSE REPLACE(LOWER(TRIM(src.brand)),'@','a') END ,
					tar.price = src.price,
					tar.discount = src.discount,
					tar.rating = src.rating,
					tar.stock = src.stock,
					tar.weight_g = src.weight_g,
					tar.color = CASE WHEN
									src.color  IS NULL THEN 'N/A'
								ELSE REPLACE(LOWER(TRIM(src.color)),'@','a') END,
					tar.created_at = src.created_at,
					tar.updated_at= src.updated_at,
					
					tar.batch_id = @batch_id
	
	WHEN NOT MATCHED BY TARGET THEN
			INSERT (product_id,  product_name, category,  brand,  price,  discount, rating,  stock,
										  weight_g , color, created_at,updated_at, batch_id)
			VALUES (
					src.product_id ,
					CASE WHEN
						src.product_name  IS NULL THEN 'N/A'
					ELSE REPLACE(LOWER(TRIM(product_name)),'@','a') END  ,
					CASE WHEN
						src.category  IS NULL THEN 'N/A'
					ELSE REPLACE(LOWER(TRIM(src.category)),'@','a') END ,
					CASE WHEN
						src.brand  IS NULL THEN 'N/A'
					ELSE REPLACE(LOWER(TRIM(src.brand)),'@','a') END,
					src.price,
					src.discount,
					src.rating,
					src.stock,
					src.weight_g,
					CASE WHEN
						src.color  IS NULL THEN 'N/A'
					ELSE REPLACE(LOWER(TRIM(src.color)),'@','a') END ,
					src.created_at,
					src.updated_at,
					@batch_id
			);

		-- Update control table
		UPDATE silver.control_table
		SET last_ingestion_datetime = GETDATE(),
			last_batch_id = @batch_id
		WHERE table_name = 'products';

	SELECT  @last_ingestion_datetime  =  last_ingestion_datetime 
	FROM silver.control_table
	WHERE table_name = 'orders';

	MERGE silver.orders AS tar
		USING (
			SELECT * FROM bronze.orders
			WHERE TRY_CAST(updated_at AS DATETIME) > @last_ingestion_datetime
           OR @last_ingestion_datetime <= '2000-01-01'  -- for initial load
				) AS src 
		ON tar.order_id= src.order_id

		WHEN MATCHED THEN 
			   UPDATE SET 
						tar.order_id =  src.order_id,
						tar.customer_id = src.customer_id,
						tar.order_status = CASE WHEN
												src.order_status  IS NULL THEN 'N/A'
											ELSE REPLACE(LOWER(TRIM(src.order_status)),'@','a') END ,
						tar.shipping_method = CASE WHEN
												src.shipping_method  IS NULL THEN 'N/A'
						ELSE REPLACE(LOWER(TRIM(src.shipping_method)),'@','a') END,
						tar.payment_terms = CASE WHEN
												src.payment_terms  IS NULL THEN 'N/A'
											ELSE REPLACE(LOWER(TRIM(src.payment_terms)),'@','a') END,
						tar.shipping_fee =src.shipping_fee,
						tar.created_at = src.created_at,
						tar.updated_at =  src.updated_at,
						tar.batch_id = @batch_id
	
	WHEN NOT MATCHED BY TARGET THEN
			INSERT (order_id,customer_id,order_status, shipping_method,payment_terms,shipping_fee,
				created_at,updated_at , batch_id)
			VALUES (
						src.order_id,
						src.customer_id,
						CASE WHEN
								src.order_status  IS NULL THEN 'N/A'
						ELSE REPLACE(LOWER(TRIM(src.order_status)),'@','a') END ,
						CASE WHEN
								src.shipping_method  IS NULL THEN 'N/A'
						ELSE REPLACE(LOWER(TRIM(src.shipping_method)),'@','a') END ,
						CASE WHEN
								src.payment_terms  IS NULL THEN 'N/A'
						ELSE REPLACE(LOWER(TRIM(src.payment_terms)),'@','a') END,
						src.shipping_fee,
						src.created_at,
						src.updated_at,
						@batch_id
			);

		-- Update control table
		UPDATE silver.control_table
		SET last_ingestion_datetime = GETDATE(),
			last_batch_id = @batch_id
		WHERE table_name = 'orders';

	SELECT  @last_ingestion_datetime  =  last_ingestion_datetime 
	FROM silver.control_table
	WHERE table_name = 'order_items';

	MERGE silver.order_items AS tar
		USING (
			SELECT * FROM bronze.order_items
			WHERE TRY_CAST(updated_at AS DATETIME) > @last_ingestion_datetime
           OR @last_ingestion_datetime <= '2000-01-01'  -- for initial load
				) AS src 
		ON tar.order_item_id= src.order_item_id

		WHEN MATCHED THEN 
			   UPDATE SET 
						tar.order_item_id =  src.order_item_id,
						tar.order_id = src.order_id,
						tar.product_id  = src.product_id, 
						tar.quantity = CASE 
										WHEN src.quantity IS NULL OR src.quantity = 0 THEN 1
									ELSE src.quantity END ,
						tar.unit_price = src.unit_price,
						tar.tax =  src.tax,
						tar.discount_amount = src.discount_amount,
						tar.fulfilled_by =  CASE WHEN
												src.fulfilled_by  IS NULL THEN 'N/A'
											ELSE REPLACE(LOWER(TRIM(src.fulfilled_by)),'@','a') END ,
						tar.created_at = src.created_at,
						tar.updated_at = src.updated_at,
						tar.batch_id = @batch_id
	
	WHEN NOT MATCHED BY TARGET THEN
			INSERT (order_item_id ,order_id,product_id ,quantity,unit_price ,tax, discount_amount ,
					fulfilled_by, created_at , updated_at ,batch_id )
			VALUES (
					src.order_item_id,
					src.order_id,
					src.product_id,
					CASE 
						WHEN src.quantity IS NULL OR src.quantity = 0 THEN 1
					ELSE src.quantity END ,
					src.unit_price,
					src.tax,
					src.discount_amount,
					CASE WHEN
							src.fulfilled_by  IS NULL THEN 'N/A'
					ELSE REPLACE(LOWER(TRIM(src.fulfilled_by)),'@','a') END,
					src.created_at,
					src.updated_at,
					@batch_id
			);

		-- Update control table
		UPDATE silver.control_table
		SET last_ingestion_datetime = GETDATE(),
			last_batch_id = @batch_id
		WHERE table_name = 'order_items';
	
	SELECT  @last_ingestion_datetime  =  last_ingestion_datetime 
	FROM silver.control_table
	WHERE table_name = 'payments';

	MERGE silver.payments AS tar
		USING (
			SELECT * FROM bronze.payments
			WHERE TRY_CAST(updated_at AS DATETIME) > @last_ingestion_datetime
           OR @last_ingestion_datetime <= '2000-01-01'  -- for initial load
				) AS src 
		ON tar.payment_id= src.payment_id

		WHEN MATCHED THEN 
			   UPDATE SET 
						tar.payment_id =  src.payment_id,
						tar.order_id = src.order_id,
						tar.amount  = src.amount, 
						tar.payment_method = CASE WHEN
												src.payment_method  IS NULL THEN 'N/A'
											ELSE REPLACE(LOWER(TRIM(src.payment_method)),'@','a') END,
						tar.payment_gateway = CASE WHEN
												src.payment_gateway  IS NULL THEN 'N/A'
											ELSE REPLACE(LOWER(TRIM(src.payment_gateway)),'@','a') END,
						tar.payment_status = CASE WHEN
												src.payment_status  IS NULL THEN 'N/A'
											ELSE REPLACE(LOWER(TRIM(src.payment_status)),'@','a') END ,
						tar.currency = 		CASE WHEN
												src.currency  IS NULL THEN 'N/A'
											ELSE REPLACE(UPPER(TRIM(src.currency)),'@','a') END ,
						tar.exchange_rate = src.exchange_rate ,
						tar.created_at = src.created_at,
						tar.updated_at = src.updated_at,
						tar.batch_id = @batch_id
	
	WHEN NOT MATCHED BY TARGET THEN
			INSERT (payment_id,order_id,amount,payment_method,payment_gateway ,payment_status,
					currency,exchange_rate,created_at,updated_at,batch_id )
			VALUES (
					src.payment_id,
					src.order_id,
					src.amount,
					CASE WHEN
						src.payment_method  IS NULL THEN 'N/A'
					ELSE REPLACE(LOWER(TRIM(src.payment_method)),'@','a') END ,
					CASE WHEN
						src.payment_gateway  IS NULL THEN 'N/A'
					ELSE REPLACE(LOWER(TRIM(src.payment_gateway)),'@','a') END ,
					CASE WHEN
						src.payment_status  IS NULL THEN 'N/A'
					ELSE REPLACE(LOWER(TRIM(src.payment_status)),'@','a') END ,
					CASE WHEN
						src.currency  IS NULL THEN 'N/A'
					ELSE REPLACE(UPPER(TRIM(src.currency)),'@','a') END ,
					src.exchange_rate,
					src.created_at,
					src.updated_at,
					@batch_id
			);

		-- Update control table
		UPDATE silver.control_table
		SET last_ingestion_datetime = GETDATE(),
			last_batch_id = @batch_id
		WHERE table_name = 'payments';
		
END
