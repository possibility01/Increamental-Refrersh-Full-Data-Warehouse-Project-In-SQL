/*
==========================================================================
Stored Procedure: bronze.control_table
==========================================================================
Purpose:
    Ensures that the control table for the Bronze layer exists and is properly initialized.
    The control table is used to track incremental and full load ETL runs for all Bronze tables.

Actions Performed:
    - Checks if the control table 'bronze.bronze_control' exists.
    - Creates the control table if it does not exist.
    - Inserts initial rows for all relevant Bronze tables with a default timestamp ('2000-01-01').
    - Logs creation time and status messages to assist with monitoring.

Parameters:
    None
    This stored procedure does not accept or return any values.

Usage Example:
    EXEC bronze.control_table;

Control Table Schema:
    table_name              NVARCHAR(50)  -- Name of the table being tracked
    last_ingestion_datetime DATETIME      -- Timestamp of the last ETL run
    last_batch_id           NVARCHAR(50)  -- Batch ID of the last ETL run

Notes:
    - Default initialization timestamp is '2000-01-01'.
    - Ensure the procedure is executed before running incremental/full loads.
==========================================================================
==========================================================================
Stored Procedure: bronze.staging_tables
==========================================================================
Purpose:
    Loads raw data from external updating CSV files into the staging tables in the Bronze schema.

Actions Performed:
    - Truncates each staging table to ensure a fresh copy before loading.
    - Uses BULK INSERT to load data from external CSV files into staging tables.
    - Logs start and end times for each table load to track duration and performance.

Parameters:
    None
    This stored procedure does not accept or return any values.

Usage Example:
    EXEC bronze.staging_tables;

Tables Loaded:
    - bronze.staging_customers
    - bronze.staging_products
    - bronze.staging_orders
    - bronze.staging_order_items
    - bronze.staging_payments

Notes:
    - CSV files must exist in the specified file paths.
    - Ensure correct file permissions to allow BULK INSERT operations.
    - Recommended to run this procedure before performing initial/incremental Bronze loads.
==========================================================================

==========================================================================
Stored Procedure: bronze.initial_incremental_load
==========================================================================
Purpose:
    Performs the initial full load and subsequent incremental loads from staging tables
    into the main Bronze tables, while tracking ETL progress using the control table.

Actions Performed:
    - Checks the last ingestion timestamp from the control table for each Bronze table.
    - Performs full load if the table has never been loaded (default timestamp: '2000-01-01').
    - Performs incremental load for records updated after the last ingestion timestamp.
    - Generates a batch ID for each ETL run to track loads.
    - Updates the control table with the new ingestion timestamp and batch ID.
    - Logs start and end times for each table load for performance tracking.
    - Ensures data integrity using transactions; rolls back in case of errors.

Parameters:
    None
    This stored procedure does not accept or return any values.

Usage Example:
    EXEC bronze.initial_incremental_load;

Tables Loaded:
    - bronze.customers
    - bronze.products
    - bronze.orders
    - bronze.order_items
    - bronze.payments

Notes:
    - Make sure staging tables are loaded prior to executing this procedure.
    - Recommended to execute bronze.control_table first to ensure control table exists.
    - Batch ID format: 'yyyyMMdd_HHmm'.
    - Errors during ETL will trigger transaction rollback for all affected tables.
==========================================================================
*/



USE DataWarehouse;
GO

CREATE OR ALTER PROCEDURE bronze.control_table AS
BEGIN
    -- Variables to track start and end time for logging purposes 
    DECLARE @START_TIME_CONTROL_TABLE DATETIME,
            @END_TIME_CONTROL_TABLE DATETIME;

    PRINT '================================================================';
    PRINT 'Starting Control Table Creation for Incremental Refresh Logic';
    PRINT '--------------------------------------------------------------';
    SET @START_TIME_CONTROL_TABLE = GETDATE();

    -- Check if the control table already exists
    IF OBJECT_ID('bronze.bronze_control','U') IS NULL
    BEGIN
        -- Create the control table
        CREATE TABLE bronze.bronze_control (
            table_name NVARCHAR(50) PRIMARY KEY,       -- Name of the table being tracked
            last_ingestion_datetime DATETIME,          -- Timestamp of last ETL run
            last_batch_id NVARCHAR(50)                 -- Batch ID for last ETL run
        );

        -- Insert initial rows for all relevant bronze tables
        INSERT INTO bronze.bronze_control (table_name, last_ingestion_datetime, last_batch_id)
        VALUES 
            ('customers', '2000-01-01', NULL),
            ('products', '2000-01-01', NULL),
            ('orders', '2000-01-01', NULL),
            ('order_items', '2000-01-01', NULL),
            ('payments', '2000-01-01', NULL);

        SET @END_TIME_CONTROL_TABLE = GETDATE();

        PRINT '----------------------------------------------------------------';
        PRINT 'Control Table successfully created and initialized.';
        PRINT 'Total Creation Time: ' + CAST(DATEDIFF(SECOND,@START_TIME_CONTROL_TABLE,@END_TIME_CONTROL_TABLE) AS NVARCHAR) + ' seconds';
        PRINT '================================================================';
    END
    ELSE
    BEGIN
        -- Table already exists
        SET @END_TIME_CONTROL_TABLE = GETDATE();

        PRINT '----------------------------------------------------------------';
        PRINT 'Bronze Control Table already exists. No changes made.';
        PRINT 'Total Duration Checked: ' + CAST(DATEDIFF(SECOND,@START_TIME_CONTROL_TABLE,@END_TIME_CONTROL_TABLE) AS NVARCHAR) + ' seconds';
        PRINT '================================================================';
    END
END
GO


/*
===========================================================================================
2) STAGING TABLE LOAD PROCEDURE
Purpose:
    - Load CSV files into staging tables (bronze layer)
    - Uses TRUNCATE + BULK INSERT to refresh staging tables
    - Timing is captured for each table load
===========================================================================================
*/

CREATE OR ALTER PROCEDURE bronze.staging_tables AS
BEGIN
        -- Variables to capture start and end times for logging
        DECLARE @Start_time_staging_load DATETIME ,@END_time_staging_load DATETIME ,
                @Start_time_staging_load_customer DATETIME , @END_time_staging_load_customer DATETIME , 
                @Start_time_staging_load_order DATETIME , @End_time_staging_load_order DATETIME ,
                @Start_time_staging_load_product DATETIME,@End_time_staging_load_product DATETIME,
                @Start_time_staging_load_order_items DATETIME,@End_time_staging_load_order_items DATETIME ,
                @Start_time_staging_load_payments DATETIME,@End_time_staging_load_payments DATETIME;
    
        -- ===================================================================
        -- 2) Staging Load (Truncate + BULK INSERT)
        -- ===================================================================

         PRINT'================================================================'
         PRINT 'Loading the Staging Bronze Tables'
         PRINT'==============================================================='
         PRINT 'Loading the Staging Customer Table'
         PRINT'--------------------------------------------------------------'
         SET @Start_time_staging_load = GETDATE()
         SET @Start_time_staging_load_customer = GETDATE()

        -- Customers Table

        -- Clear old staging data
        TRUNCATE TABLE bronze.staging_customers;

        -- Load new data from CSV
        BULK INSERT bronze.staging_customers
        FROM 'C:\Users\eBay\source\repos\NewRepo\Datasets\customers.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);

        SET @End_time_staging_load_customer =  GETDATE()

        PRINT'---------------------------------------------------------------------------------------------------------------------------------------------'
        PRINT ' Customer Table Loading Completed,Total Creation Time:' + 
        CAST (DATEDIFF(SECOND,@Start_time_staging_load_customer ,@End_time_staging_load_customer) AS NVARCHAR) +'Seconds'
        PRINT'-------------------------------------------------------------------------------------------------------------------------------------------------------------'

        PRINT 'Loading the Staging Products Table'
        PRINT'--------------------------------------------------------------'

        SET @Start_time_staging_load_product = GETDATE()
        -- Products
        TRUNCATE TABLE bronze.staging_products;
        BULK INSERT bronze.staging_products
        FROM 'C:\Users\eBay\source\repos\NewRepo\Datasets\products.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);

        
        SET @End_time_staging_load_product =  GETDATE()

        PRINT'---------------------------------------------------------------------------------------------------------------------------------------------'
        PRINT ' Product Table Loading Completed,Total Creation Time:' +
        CAST (DATEDIFF(SECOND,@Start_time_staging_load_product ,@End_time_staging_load_product) AS NVARCHAR) +'Seconds'
        PRINT'-------------------------------------------------------------------------------------------------------------------------------------------------------------'

        PRINT 'Loading the Staging Order Table'
        PRINT'--------------------------------------------------------------'

        SET @Start_time_staging_load_order = GETDATE()
        -- Orders
        TRUNCATE TABLE bronze.staging_orders;
        BULK INSERT bronze.staging_orders
        FROM 'C:\Users\eBay\source\repos\NewRepo\Datasets\orders.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);

        SET @End_time_staging_load_order =  GETDATE()

        PRINT'---------------------------------------------------------------------------------------------------------------------------------------------'
        PRINT ' Order Table Loading Completed,Total Creation Time:' + 
        CAST (DATEDIFF(SECOND,@Start_time_staging_load_order ,@End_time_staging_load_order) AS NVARCHAR) +'Seconds'
        PRINT'-------------------------------------------------------------------------------------------------------------------------------------------------------------'

        PRINT 'Loading the Staging Order_Items Table'
        PRINT'--------------------------------------------------------------'

        SET @Start_time_staging_load_order_items = GETDATE()
        -- Order Items
        TRUNCATE TABLE bronze.staging_order_items;
        BULK INSERT bronze.staging_order_items
        FROM 'C:\Users\eBay\source\repos\NewRepo\Datasets\order_items.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);

        SET @End_time_staging_load_order_items =  GETDATE()

        PRINT'---------------------------------------------------------------------------------------------------------------------------------------------'
        PRINT ' Order_Items Table Loading Completed,Total Creation Time:' + 
        CAST (DATEDIFF(SECOND,@Start_time_staging_load_order_items ,@End_time_staging_load_order_items) AS NVARCHAR) +'Seconds'
        PRINT'-------------------------------------------------------------------------------------------------------------------------------------------------------------'

        PRINT 'Loading the Staging Payments Table'
        PRINT'--------------------------------------------------------------'

        SET @Start_time_staging_load_payments = GETDATE()
        -- Payments
        TRUNCATE TABLE bronze.staging_payments;
        BULK INSERT bronze.staging_payments
        FROM 'C:\Users\eBay\source\repos\NewRepo\Datasets\payments.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);

        SET @End_time_staging_load_payments =  GETDATE()

        PRINT'---------------------------------------------------------------------------------------------------------------------------------------------'
        PRINT ' Payment Table Loading Completed,Total Creation Time:' +
        CAST (DATEDIFF(SECOND,@Start_time_staging_load_payments ,@End_time_staging_load_payments) AS NVARCHAR) +'Seconds'
        PRINT'-------------------------------------------------------------------------------------------------------------------------------------------------------------'
        
        SET @END_time_staging_load = GETDATE()

        PRINT'======================================================================================================================================='
        PRINT ' Staging Tables Loading Completed,Total Creation Time:' + 
        CAST (DATEDIFF(SECOND,@Start_time_staging_load ,@End_time_staging_load) AS NVARCHAR) +'Seconds'
        PRINT'========================================================================================================================================================================='
END 
GO

/*
===========================================================================================
3) INITIAL / INCREMENTAL LOAD PROCEDURE
Purpose:
    - Loads data from staging into bronze tables
    - Supports full load (initial) and incremental load based on last ingestion
    - Updates control table after each table load
    - Uses TRY-CATCH with transaction to ensure ETL consistency
===========================================================================================
*/

CREATE OR ALTER PROCEDURE bronze.inital_increamental_load AS

        -- ===================================================================
        -- 3) Incremental / Full Load per Table
        -- ===================================================================
        BEGIN TRY
            BEGIN TRANSACTION;

            -- Batch ID for this ETL run

            DECLARE @batch_id NVARCHAR(50) = FORMAT(GETDATE(), 'yyyyMMdd_HHmm'),@last_ingestion DATETIME,
             -- Timing variables for logging (one pair per table)
                    @Start_time_initial_load_increamental DATETIME ,@END_time_initial_load_increamental DATETIME ,
                    @Start_time_intial_load_customer DATETIME , @END_time_intial_load_customer DATETIME ,
                    @Start_time_increamental_load_customer DATETIME , @END_time_increamental_load_customer DATETIME ,
                    @Start_time_intial_load_order DATETIME , @END_time_intial_load_order DATETIME ,
                    @Start_time_increamental_load_order DATETIME , @END_time_increamental_load_order DATETIME ,
                    @Start_time_intial_load_product DATETIME , @END_time_intial_load_product DATETIME ,
                    @Start_time_increamental_load_product DATETIME , @END_time_increamental_load_product DATETIME ,
                    @Start_time_intial_load_payment DATETIME , @END_time_intial_load_payment DATETIME ,
                    @Start_time_increamental_load_payment DATETIME , @END_time_increamental_load_payment DATETIME ,
                    @Start_time_intial_load_order_items DATETIME , @END_time_intial_load_order_items DATETIME ,
                    @Start_time_increamental_order_items DATETIME , @END_time_increamental_order_items DATETIME;

         PRINT'================================================================'
         PRINT 'Inital Loading of the Bronze Tables'
         PRINT'==============================================================='
         PRINT 'Initial Loading of the  Customer Table'
         PRINT'--------------------------------------------------------------'
            -- ------------------------------
            -- Customers
            -- ------------------------------
            SELECT @last_ingestion = last_ingestion_datetime
            FROM bronze.bronze_control
            WHERE table_name = 'customers';

            
            -- Full load if never ingested
            IF @last_ingestion <= '2000-01-01'

            

            BEGIN
                SET @Start_time_intial_load_customer = GETDATE()
                TRUNCATE TABLE bronze.customers;

                INSERT INTO bronze.customers (
                    customer_id, first_name, last_name, email, phone, gender, city, 
                    age, income_level, loyalty_score, segment, preferred_device, 
                    marital_status, created_at, updated_at, is_deleted, batch_id
                )
                SELECT 
                    customer_id, first_name, last_name, email, phone, gender, city, 
                    age, income_level, loyalty_score, segment, preferred_device, 
                    marital_status, created_at, updated_at, is_deleted, @batch_id
                FROM bronze.staging_customers;


                SET @END_time_intial_load_customer = GETDATE()
                PRINT'---------------------------------------------------------------------------------------------------------------------------------------------'
                PRINT ' Customer Table Inital Loading Completed,Total Creation Time:'
                + CAST (DATEDIFF(SECOND,@Start_time_intial_load_customer ,@END_time_intial_load_customer) AS NVARCHAR) +'Seconds'
                PRINT'-------------------------------------------------------------------------------------------------------------------------------------------------------------'

            END
            ELSE
            BEGIN
            -- Incremental load based on last ingestion datetime
                SET @Start_time_increamental_load_customer = GETDATE();

        MERGE bronze.customers AS tgt
        USING (
            SELECT 
                *,
                @batch_id AS batch_id
            FROM bronze.staging_customers
            WHERE TRY_CAST(updated_at AS DATETIME) > @last_ingestion
        ) AS src
        ON tgt.customer_id = src.customer_id
        

        WHEN MATCHED THEN
            UPDATE SET
                tgt.first_name       = src.first_name,
                tgt.last_name        = src.last_name,
                tgt.email            = src.email,
                tgt.phone            = src.phone,
                tgt.gender           = src.gender,
                tgt.city             = src.city,
                tgt.age              = src.age,
                tgt.income_level     = src.income_level,
                tgt.loyalty_score    = src.loyalty_score,
                tgt.segment          = src.segment,
                tgt.preferred_device = src.preferred_device,
                tgt.marital_status   = src.marital_status,
                tgt.created_at       = src.created_at,
                tgt.updated_at       = src.updated_at,
                tgt.is_deleted       = src.is_deleted,
                tgt.batch_id         = src.batch_id

        WHEN NOT MATCHED BY TARGET THEN
            INSERT (
                customer_id, first_name, last_name, email, phone, gender, city,
                age, income_level, loyalty_score, segment, preferred_device,
                marital_status, created_at, updated_at, is_deleted, batch_id
            )
            VALUES (
                src.customer_id, src.first_name, src.last_name, src.email, src.phone, src.gender, src.city,
                src.age, src.income_level, src.loyalty_score, src.segment, src.preferred_device,
                src.marital_status, src.created_at, src.updated_at, src.is_deleted, src.batch_id
            );
            SET @END_time_increamental_load_customer = GETDATE()

            PRINT'---------------------------------------------------------------------------------------------------------------------------------------------'
            PRINT ' Customer Table Increamental Loading Completed,Total Creation Time:' 
            + CAST (DATEDIFF(SECOND,@Start_time_increamental_load_customer ,@END_time_increamental_load_customer) AS NVARCHAR) +'Seconds'
            PRINT'-------------------------------------------------------------------------------------------------------------------------------------------------------------'

            END
            -- Update control table for this table
            UPDATE bronze.bronze_control
            SET last_ingestion_datetime = GETDATE(),
                last_batch_id = @batch_id
            WHERE table_name = 'customers';

            TRUNCATE TABLE bronze.staging_customers;
            
        /*
            Similarly repeat the pattern for products, orders, order_items, payments
            - Check last ingestion in control table
            - Full load if <= 2000-01-01
            - Otherwise incremental based on updated_at
            - Update control table after load
        */
            -- ------------------------------
            -- Products
            -- ------------------------------
            SELECT @last_ingestion = last_ingestion_datetime
            FROM bronze.bronze_control
            WHERE table_name = 'products';

            IF @last_ingestion <= '2000-01-01'
            BEGIN

                SET @Start_time_intial_load_product = GETDATE()
                TRUNCATE TABLE bronze.products;

                INSERT INTO bronze.products (
                    product_id, product_name, category, brand, price, discount, rating, stock,
                    weight_g, color, created_at, updated_at, is_deleted, batch_id
                )
                SELECT 
                    product_id, product_name, category, brand, price, discount, rating, stock,
                    weight_g, color, created_at, updated_at, is_deleted, @batch_id
                FROM bronze.staging_products;

                SET @END_time_intial_load_product = GETDATE()
                PRINT'---------------------------------------------------------------------------------------------------------------------------------------------'
                PRINT ' Product Table Inital Loading Completed,Total Creation Time:' 
                + CAST (DATEDIFF(SECOND,@Start_time_intial_load_product ,@END_time_intial_load_product) AS NVARCHAR) +'Seconds'
                PRINT'-------------------------------------------------------------------------------------------------------------------------------------------------------------'

            END
            ELSE
            BEGIN
                SET @Start_time_increamental_load_product = GETDATE()

                MERGE bronze.products  AS tgt
                USING ( 
                    SELECT *, @batch_id AS batch_id
                    FROM bronze.staging_products
                    WHERE TRY_CAST(updated_at AS DATETIME) > @last_ingestion ) AS src
                    ON tgt.product_id = src.product_id

                WHEN MATCHED THEN
                        UPDATE SET
                                tgt.product_name = src.product_name,
                                tgt.category = src.category,
                                tgt.brand = src.brand,
                                tgt.price = src.price,
                                tgt.discount =src.discount,
                                tgt.rating = src.rating,
                                tgt.stock = src.stock,
                                tgt.weight_g = src.weight_g, 
                                tgt.color = src.color, 
                                tgt.created_at = tgt.created_at, 
                                tgt.updated_at =  src.updated_at, 
                                tgt.is_deleted = src.is_deleted, 
                                tgt.batch_id = src.batch_id

                WHEN NOT MATCHED BY TARGET THEN
                            
                INSERT  (
                    product_id, product_name, category, brand, price, discount, rating, stock,
                    weight_g, color, created_at, updated_at, is_deleted, batch_id
                )
                VALUES (
                    src.product_id, src.product_name, src.category, src.brand, src.price, src.discount, src.rating, src.stock,
                    src.weight_g, src.color, src.created_at, src.updated_at, src.is_deleted, src.batch_id );

                SET @END_time_increamental_load_product = GETDATE()
                PRINT'---------------------------------------------------------------------------------------------------------------------------------------------'
                PRINT ' Product Table Increamental Loading Completed,Total Creation Time:' 
                + CAST (DATEDIFF(SECOND,@Start_time_increamental_load_product ,@END_time_increamental_load_product) AS NVARCHAR) +'Seconds'
                PRINT'-------------------------------------------------------------------------------------------------------------------------------------------------------------'

            END

            UPDATE bronze.bronze_control
            SET last_ingestion_datetime = GETDATE(),
                last_batch_id = @batch_id
            WHERE table_name = 'products';

            TRUNCATE TABLE bronze.staging_products;

            -- ------------------------------
            -- Orders
            -- ------------------------------
            SELECT @last_ingestion = last_ingestion_datetime
            FROM bronze.bronze_control
            WHERE table_name = 'orders';

            IF @last_ingestion <= '2000-01-01'
            BEGIN
                SET @Start_time_intial_load_order =GETDATE()
                TRUNCATE TABLE bronze.orders;

                INSERT INTO bronze.orders (
                    order_id, customer_id, order_status, shipping_method, payment_terms,
                    shipping_fee, created_at, updated_at, is_deleted, batch_id
                )
                SELECT 
                    order_id, customer_id, order_status, shipping_method, payment_terms,
                    shipping_fee, created_at, updated_at, is_deleted, @batch_id
                FROM bronze.staging_orders;

                SET @END_time_intial_load_order =GETDATE()

                PRINT'---------------------------------------------------------------------------------------------------------------------------------------------'
                PRINT ' Order Table Inital Loading Completed,Total Creation Time:'
                + CAST (DATEDIFF(SECOND,@Start_time_intial_load_order ,@END_time_intial_load_order) AS NVARCHAR) +'Seconds'
                PRINT'-------------------------------------------------------------------------------------------------------------------------------------------------------------'

            END
            ELSE
            BEGIN
                
                SET @Start_time_increamental_load_order = GETDATE()
                MERGE bronze.orders AS tgt
                        USING ( 
                            SELECT *, @batch_id  AS batch_id
                    FROM bronze.staging_orders
                    WHERE TRY_CAST(updated_at AS DATETIME) > @last_ingestion ) AS src

                    ON tgt.order_id  = src.order_id

                WHEN MATCHED THEN 
                       UPDATE SET 
                                tgt.customer_id = src.customer_id,
                                tgt.order_status = src.order_status,
                                tgt.shipping_method = src.shipping_method,
                                tgt.payment_terms = src.payment_terms,
                                tgt.shipping_fee =src.shipping_fee,
                                tgt.created_at = src.created_at,
                                tgt.updated_at = src.updated_at,
                                tgt.is_deleted = src.is_deleted, 
                                tgt.batch_id = src.batch_id

                WHEN NOT MATCHED BY TARGET THEN 
                INSERT  (
                    order_id, customer_id, order_status, shipping_method, payment_terms,
                    shipping_fee, created_at, updated_at, is_deleted, batch_id
                )
                VALUES ( 
                    src.order_id, src.customer_id, src.order_status, src.shipping_method, src.payment_terms,
                    src.shipping_fee, src.created_at, src.updated_at, src.is_deleted, src.batch_id );
                

                SET @END_time_increamental_load_order = GETDATE()
                PRINT'---------------------------------------------------------------------------------------------------------------------------------------------'
                PRINT ' Order Table Increamental Loading Completed,Total Creation Time:'
                + CAST (DATEDIFF(SECOND,@Start_time_increamental_load_order,@END_time_increamental_load_order) AS NVARCHAR) +'Seconds'
                PRINT'-------------------------------------------------------------------------------------------------------------------------------------------------------------'

            END

            UPDATE bronze.bronze_control
            SET last_ingestion_datetime = GETDATE(),
                last_batch_id = @batch_id
            WHERE table_name = 'orders';

            TRUNCATE TABLE bronze.staging_orders;

            -- ------------------------------
            -- Order Items
            -- ------------------------------
            SELECT @last_ingestion = last_ingestion_datetime
            FROM bronze.bronze_control
            WHERE table_name = 'order_items';

            IF @last_ingestion <= '2000-01-01'
            BEGIN

                SET @Start_time_intial_load_order_items = GETDATE()
                TRUNCATE TABLE bronze.order_items;

                INSERT INTO bronze.order_items (
                    order_item_id, order_id, product_id, quantity, unit_price, tax,
                    discount_amount, fulfilled_by, created_at, updated_at, batch_id
                )
                SELECT 
                    order_item_id, order_id, product_id, quantity, unit_price, tax,
                    discount_amount, fulfilled_by, created_at, updated_at, @batch_id
                FROM bronze.staging_order_items;

                SET @END_time_intial_load_order_items = GETDATE()
                PRINT'---------------------------------------------------------------------------------------------------------------------------------------------'
                PRINT ' Order_Items Table Inital Loading Completed,Total Creation Time:' 
                + CAST (DATEDIFF(SECOND,@Start_time_intial_load_order_items,@END_time_intial_load_order_items) AS NVARCHAR) +'Seconds'
                PRINT'-------------------------------------------------------------------------------------------------------------------------------------------------------------'

            END
            ELSE
            BEGIN
                SET @Start_time_increamental_order_items = GETDATE()
                MERGE bronze.order_items AS tgt
                    USING (
                            SELECT *, @batch_id AS batch_id
                    FROM bronze.staging_order_items
                    WHERE TRY_CAST(updated_at AS DATETIME) > @last_ingestion ) AS src

                    ON tgt.order_item_id  = src.order_item_id

                WHEN MATCHED THEN 
                       UPDATE SET 
                                tgt.order_id = src.order_id,
                                tgt.product_id = src.product_id,
                                tgt.quantity = src.quantity,
                                tgt.unit_price = src.unit_price,
                                tgt.tax =src.tax,
                                tgt.discount_amount = src.discount_amount,
                                tgt.fulfilled_by = src.fulfilled_by,
                                tgt.created_at = src.created_at,
                                tgt.updated_at = src.updated_at, 
                                tgt.batch_id = src.batch_id

            WHEN NOT MATCHED BY TARGET THEN 
                                INSERT  (
                                    order_item_id, order_id, product_id, quantity, unit_price, tax,
                                    discount_amount, fulfilled_by, created_at, updated_at, batch_id
                                )
                                VALUES (
                                    src.order_item_id, src.order_id, src.product_id, src.quantity, src.unit_price, src.tax,
                                    src.discount_amount, src.fulfilled_by, src.created_at, src.updated_at, src.batch_id );
                                

                SET @END_time_increamental_order_items = GETDATE()

                PRINT'---------------------------------------------------------------------------------------------------------------------------------------------'
                PRINT ' Order_Items Table Increamental Loading Completed,Total Creation Time:' 
                + CAST (DATEDIFF(SECOND,@Start_time_increamental_order_items,@END_time_increamental_order_items) AS NVARCHAR) +'Seconds'
                PRINT'-------------------------------------------------------------------------------------------------------------------------------------------------------------'

            END

            UPDATE bronze.bronze_control
            SET last_ingestion_datetime = GETDATE(),
                last_batch_id = @batch_id
            WHERE table_name = 'order_items';

            TRUNCATE TABLE bronze.staging_order_items;

            -- ------------------------------
            -- Payments
            -- ------------------------------
            SELECT @last_ingestion = last_ingestion_datetime
            FROM bronze.bronze_control
            WHERE table_name = 'payments';

            IF @last_ingestion <= '2000-01-01'
            BEGIN
                
                SET @Start_time_intial_load_payment = GETDATE()
                TRUNCATE TABLE bronze.payments;

                INSERT INTO bronze.payments (
                    payment_id, order_id, amount, payment_method, payment_gateway,
                    payment_status, currency, exchange_rate, created_at, updated_at, batch_id
                )
                SELECT 
                    payment_id, order_id, amount, payment_method, payment_gateway,
                    payment_status, currency, exchange_rate, created_at, updated_at, @batch_id
                FROM bronze.staging_payments;

                SET @END_time_intial_load_payment = GETDATE()
                PRINT'---------------------------------------------------------------------------------------------------------------------------------------------'
                PRINT ' Payment Table Initial Loading Completed,Total Creation Time:'
                + CAST (DATEDIFF(SECOND,@Start_time_intial_load_payment,@END_time_intial_load_payment) AS NVARCHAR) +'Seconds'
                PRINT'-------------------------------------------------------------------------------------------------------------------------------------------------------------'

            END
            ELSE
            BEGIN

                SET @Start_time_increamental_load_payment = GETDATE()
                MERGE bronze.payments AS tgt 
                        USING  (  
                                SELECT *, @batch_id AS batch_id 
                    FROM bronze.staging_payments
                    WHERE TRY_CAST(updated_at AS DATETIME) > @last_ingestion) AS src

                    ON tgt.payment_id = src.payment_id

                WHEN MATCHED THEN 
                       UPDATE SET 
                                tgt.payment_id = src.payment_id,
                                tgt.order_id = src.order_id,
                                tgt.amount = src.amount,
                                tgt.payment_method = src.payment_method,
                                tgt.payment_gateway =src.payment_gateway,
                                tgt.currency = src.currency,
                                tgt.payment_status = src.payment_status,
                                tgt.exchange_rate = src.exchange_rate,
                                tgt.created_at = src.created_at,
                                tgt.updated_at = src.updated_at, 
                                tgt.batch_id = src.batch_id

               WHEN NOT MATCHED BY TARGET THEN 


                                INSERT (
                                    payment_id, order_id, amount, payment_method, payment_gateway,
                                    payment_status, currency, exchange_rate, created_at, updated_at, batch_id
                                )
                                VALUES 
                                   ( payment_id, order_id, amount, payment_method, payment_gateway,
                                    payment_status, currency, exchange_rate, created_at, updated_at, batch_id);
                                


                SET @END_time_increamental_load_payment = GETDATE()

                PRINT'---------------------------------------------------------------------------------------------------------------------------------------------'
                PRINT ' Payment Table Increamental Loading Completed,Total Creation Time:' 
                + CAST (DATEDIFF(SECOND,@Start_time_increamental_load_payment,@END_time_increamental_load_payment) AS NVARCHAR) +'Seconds'
                PRINT'-------------------------------------------------------------------------------------------------------------------------------------------------------------'


            END

            UPDATE bronze.bronze_control
            SET last_ingestion_datetime = GETDATE(),
                last_batch_id = @batch_id
            WHERE table_name = 'payments';

            TRUNCATE TABLE bronze.staging_payments;

            COMMIT TRANSACTION;
       END TRY

    BEGIN CATCH
        ROLLBACK TRANSACTION;

        PRINT '*************** ERROR OCCURRED ***************';
        PRINT ERROR_MESSAGE();
        PRINT '**********************************************';
 END CATCH;

    

