
    USE DataWarehouse;
    GO
    USE DataWarehouse;
GO

/*
===========================================================================================
1) CONTROL TABLE PROCEDURE
Purpose:
    - Create a control table that keeps track of the last ingestion datetime and batch ID
    - This table is essential for incremental load logic
===========================================================================================
*/


    CREATE OR ALTER PROCEDURE bronze.control_table AS
    BEGIN
          
        DECLARE @START_TIME_CONTROL_TABLE DATETIME,  @END_TIME_CONTROL_TABLE DATETIME;

      -- ===================================================================
        -- 1) Control Table Setup (Run once)
        -- ===================================================================

         PRINT'================================================================'
         PRINT 'Creating Control Table for Incremental Refresh Logic'
         PRINT'--------------------------------------------------------------'
         SET @START_TIME_CONTROL_TABLE = GETDATE()

        IF OBJECT_ID('bronze.bronze_control','U') IS NULL
        BEGIN

        CREATE TABLE bronze.bronze_control (
            table_name NVARCHAR(50) PRIMARY KEY,
            last_ingestion_datetime DATETIME,
            last_batch_id NVARCHAR(50)
        );

        INSERT INTO bronze.bronze_control (table_name, last_ingestion_datetime, last_batch_id)
        VALUES 
            ('customers', '2000-01-01', NULL),
            ('products', '2000-01-01', NULL),
            ('orders', '2000-01-01', NULL),
            ('order_items', '2000-01-01', NULL),
            ('payments', '2000-01-01', NULL);
      SET @END_TIME_CONTROL_TABLE = GETDATE()
      

     PRINT'---------------------------------------------------------------------------------------------------------------------------------------------'
     PRINT ' Control Table for Incremental Refresh Logic Creation Completed,Total Creation Time:' + CAST (DATEDIFF(SECOND,@START_TIME_CONTROL_TABLE,@END_TIME_CONTROL_TABLE) AS NVARCHAR) +'Seconds'
     PRINT'================================================================================================================================'

    END 
    GO

    CREATE OR ALTER PROCEDURE bronze.staging_tables AS
    BEGIN

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
        TRUNCATE TABLE bronze.staging_customers;
        BULK INSERT bronze.staging_customers
        FROM 'C:\Users\eBay\source\repos\NewRepo\Datasets\customers.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);

        SET @End_time_staging_load_customer =  GETDATE()

        PRINT'---------------------------------------------------------------------------------------------------------------------------------------------'
        PRINT ' Customer Table Loading Completed,Total Creation Time:' + CAST (DATEDIFF(SECOND,@Start_time_staging_load_customer ,@End_time_staging_load_customer) AS NVARCHAR) +'Seconds'
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
        PRINT ' Product Table Loading Completed,Total Creation Time:' + CAST (DATEDIFF(SECOND,@Start_time_staging_load_product ,@End_time_staging_load_product) AS NVARCHAR) +'Seconds'
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
        PRINT ' Order Table Loading Completed,Total Creation Time:' + CAST (DATEDIFF(SECOND,@Start_time_staging_load_order ,@End_time_staging_load_order) AS NVARCHAR) +'Seconds'
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
        PRINT ' Order_Items Table Loading Completed,Total Creation Time:' + CAST (DATEDIFF(SECOND,@Start_time_staging_load_order_items ,@End_time_staging_load_order_items) AS NVARCHAR) +'Seconds'
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
        PRINT ' Payment Table Loading Completed,Total Creation Time:' + CAST (DATEDIFF(SECOND,@Start_time_staging_load_payments ,@End_time_staging_load_payments) AS NVARCHAR) +'Seconds'
        PRINT'-------------------------------------------------------------------------------------------------------------------------------------------------------------'
        
        SET @END_time_staging_load = GETDATE()

        PRINT'======================================================================================================================================='
        PRINT ' Staging Tables Loading Completed,Total Creation Time:' + CAST (DATEDIFF(SECOND,@Start_time_staging_load ,@End_time_staging_load) AS NVARCHAR) +'Seconds'
        PRINT'========================================================================================================================================================================='
    END 

    GO

    CREATE OR ALTER PROCEDURE bronze.inital_increamental_load AS
    BEGIN
        -- ===================================================================
        -- 3) Incremental / Full Load per Table
        -- ===================================================================
        BEGIN TRY
            BEGIN TRANSACTION;

            DECLARE @batch_id NVARCHAR(50) = FORMAT(GETDATE(), 'yyyyMMdd_HHmm'),@last_ingestion DATETIME,
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
                PRINT ' Customer Table Inital Loading Completed,Total Creation Time:' + CAST (DATEDIFF(SECOND,@Start_time_intial_load_customer ,@END_time_intial_load_customer) AS NVARCHAR) +'Seconds'
                PRINT'-------------------------------------------------------------------------------------------------------------------------------------------------------------'

            END
            ELSE
            BEGIN
                SET @Start_time_increamental_load_customer = GETDATE()
                INSERT INTO bronze.customers (
                    customer_id, first_name, last_name, email, phone, gender, city, 
                    age, income_level, loyalty_score, segment, preferred_device, 
                    marital_status, created_at, updated_at, is_deleted, batch_id
                )
                SELECT 
                    customer_id, first_name, last_name, email, phone, gender, city, 
                    age, income_level, loyalty_score, segment, preferred_device, 
                    marital_status, created_at, updated_at, is_deleted, @batch_id
                FROM bronze.staging_customers
                WHERE TRY_CAST(updated_at AS DATETIME) > @last_ingestion;
            SET @END_time_increamental_load_customer = GETDATE()

            PRINT'---------------------------------------------------------------------------------------------------------------------------------------------'
            PRINT ' Customer Table Increamental Loading Completed,Total Creation Time:' + CAST (DATEDIFF(SECOND,@Start_time_increamental_load_customer ,@END_time_increamental_load_customer) AS NVARCHAR) +'Seconds'
            PRINT'-------------------------------------------------------------------------------------------------------------------------------------------------------------'

            END

            UPDATE bronze.bronze_control
            SET last_ingestion_datetime = GETDATE(),
                last_batch_id = @batch_id
            WHERE table_name = 'customers';

            TRUNCATE TABLE bronze.staging_customers;

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
                PRINT ' Product Table Inital Loading Completed,Total Creation Time:' + CAST (DATEDIFF(SECOND,@Start_time_intial_load_product ,@END_time_intial_load_product) AS NVARCHAR) +'Seconds'
                PRINT'-------------------------------------------------------------------------------------------------------------------------------------------------------------'

            END
            ELSE
            BEGIN
                SET @Start_time_increamental_load_product = GETDATE()
                INSERT INTO bronze.products (
                    product_id, product_name, category, brand, price, discount, rating, stock,
                    weight_g, color, created_at, updated_at, is_deleted, batch_id
                )
                SELECT 
                    product_id, product_name, category, brand, price, discount, rating, stock,
                    weight_g, color, created_at, updated_at, is_deleted, @batch_id
                FROM bronze.staging_products
                WHERE TRY_CAST(updated_at AS DATETIME) > @last_ingestion;

                SET @END_time_increamental_load_product = GETDATE()
                PRINT'---------------------------------------------------------------------------------------------------------------------------------------------'
                PRINT ' Product Table Increamental Loading Completed,Total Creation Time:' + CAST (DATEDIFF(SECOND,@Start_time_increamental_load_product ,@END_time_increamental_load_product) AS NVARCHAR) +'Seconds'
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
                PRINT ' Order Table Inital Loading Completed,Total Creation Time:' + CAST (DATEDIFF(SECOND,@Start_time_intial_load_order ,@END_time_intial_load_order) AS NVARCHAR) +'Seconds'
                PRINT'-------------------------------------------------------------------------------------------------------------------------------------------------------------'

            END
            ELSE
            BEGIN
                
                SET @Start_time_increamental_load_order = GETDATE()
                INSERT INTO bronze.orders (
                    order_id, customer_id, order_status, shipping_method, payment_terms,
                    shipping_fee, created_at, updated_at, is_deleted, batch_id
                )
                SELECT 
                    order_id, customer_id, order_status, shipping_method, payment_terms,
                    shipping_fee, created_at, updated_at, is_deleted, @batch_id
                FROM bronze.staging_orders
                WHERE TRY_CAST(updated_at AS DATETIME) > @last_ingestion;

                SET @END_time_increamental_load_order = GETDATE()
                PRINT'---------------------------------------------------------------------------------------------------------------------------------------------'
                PRINT ' Order Table Increamental Loading Completed,Total Creation Time:' + CAST (DATEDIFF(SECOND,@Start_time_increamental_load_order,@END_time_increamental_load_order) AS NVARCHAR) +'Seconds'
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
                PRINT ' Order_Items Table Inital Loading Completed,Total Creation Time:' + CAST (DATEDIFF(SECOND,@Start_time_intial_load_order_items,@END_time_intial_load_order_items) AS NVARCHAR) +'Seconds'
                PRINT'-------------------------------------------------------------------------------------------------------------------------------------------------------------'

            END
            ELSE
            BEGIN
                SET @Start_time_increamental_order_items = GETDATE()
                INSERT INTO bronze.order_items (
                    order_item_id, order_id, product_id, quantity, unit_price, tax,
                    discount_amount, fulfilled_by, created_at, updated_at, batch_id
                )
                SELECT 
                    order_item_id, order_id, product_id, quantity, unit_price, tax,
                    discount_amount, fulfilled_by, created_at, updated_at, @batch_id
                FROM bronze.staging_order_items
                WHERE TRY_CAST(updated_at AS DATETIME) > @last_ingestion;

                SET @END_time_increamental_order_items = GETDATE()

                PRINT'---------------------------------------------------------------------------------------------------------------------------------------------'
                PRINT ' Order_Items Table Increamental Loading Completed,Total Creation Time:' + CAST (DATEDIFF(SECOND,@Start_time_increamental_order_items,@END_time_increamental_order_items) AS NVARCHAR) +'Seconds'
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
                PRINT ' Payment Table Initial Loading Completed,Total Creation Time:' + CAST (DATEDIFF(SECOND,@Start_time_intial_load_payment,@END_time_intial_load_payment) AS NVARCHAR) +'Seconds'
                PRINT'-------------------------------------------------------------------------------------------------------------------------------------------------------------'

            END
            ELSE
            BEGIN

                SET @Start_time_increamental_load_payment = GETDATE()
                INSERT INTO bronze.payments (
                    payment_id, order_id, amount, payment_method, payment_gateway,
                    payment_status, currency, exchange_rate, created_at, updated_at, batch_id
                )
                SELECT 
                    payment_id, order_id, amount, payment_method, payment_gateway,
                    payment_status, currency, exchange_rate, created_at, updated_at, @batch_id
                FROM bronze.staging_payments
                WHERE TRY_CAST(updated_at AS DATETIME) > @last_ingestion;   



                SET @END_time_increamental_load_payment = GETDATE()

                PRINT'---------------------------------------------------------------------------------------------------------------------------------------------'
                PRINT ' Payment Table Increamental Loading Completed,Total Creation Time:' + CAST (DATEDIFF(SECOND,@Start_time_increamental_load_payment,@END_time_increamental_load_payment) AS NVARCHAR) +'Seconds'
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
            IF XACT_STATE() <> 0
                ROLLBACK TRANSACTION;

            DECLARE @ErrMsg NVARCHAR(4000) = ERROR_MESSAGE();
            RAISERROR('Error during Bronze ETL: %s', 16, 1, @ErrMsg);
        END CATCH;
    

    END 

