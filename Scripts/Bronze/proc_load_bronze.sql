USE DataWarehouse;
GO

-- ===================================================================
-- 1) Control Table Setup (Run once)
-- ===================================================================
IF OBJECT_ID('bronze.bronze_control','U') IS NOT NULL
    DROP TABLE bronze.bronze_control;

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
GO

-- ===================================================================
-- 2) Staging Load (Truncate + BULK INSERT)
-- ===================================================================
-- Customers
TRUNCATE TABLE bronze.staging_customers;
BULK INSERT bronze.staging_customers
FROM 'C:\Users\eBay\source\repos\NewRepo\Datasets\customers.csv'
WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);

-- Products
TRUNCATE TABLE bronze.staging_products;
BULK INSERT bronze.staging_products
FROM 'C:\Users\eBay\source\repos\NewRepo\Datasets\products.csv'
WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);

-- Orders
TRUNCATE TABLE bronze.staging_orders;
BULK INSERT bronze.staging_orders
FROM 'C:\Users\eBay\source\repos\NewRepo\Datasets\orders.csv'
WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);

-- Order Items
TRUNCATE TABLE bronze.staging_order_items;
BULK INSERT bronze.staging_order_items
FROM 'C:\Users\eBay\source\repos\NewRepo\Datasets\order_items.csv'
WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);

-- Payments
TRUNCATE TABLE bronze.staging_payments;
BULK INSERT bronze.staging_payments
FROM 'C:\Users\eBay\source\repos\NewRepo\Datasets\payments.csv'
WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);

-- ===================================================================
-- 3) Incremental / Full Load per Table
-- ===================================================================
BEGIN TRY
    BEGIN TRANSACTION;

    DECLARE @batch_id NVARCHAR(50) = FORMAT(GETDATE(), 'yyyyMMdd_HHmm');
    DECLARE @last_ingestion DATETIME;

    -- ------------------------------
    -- Customers
    -- ------------------------------
    SELECT @last_ingestion = last_ingestion_datetime
    FROM bronze.bronze_control
    WHERE table_name = 'customers';

    IF @last_ingestion <= '2000-01-01'
    BEGIN
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
    END
    ELSE
    BEGIN
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
        TRUNCATE TABLE bronze.products;

        INSERT INTO bronze.products (
            product_id, product_name, category, brand, price, discount, rating, stock,
            weight_g, color, created_at, updated_at, is_deleted, batch_id
        )
        SELECT 
            product_id, product_name, category, brand, price, discount, rating, stock,
            weight_g, color, created_at, updated_at, is_deleted, @batch_id
        FROM bronze.staging_products;
    END
    ELSE
    BEGIN
        INSERT INTO bronze.products (
            product_id, product_name, category, brand, price, discount, rating, stock,
            weight_g, color, created_at, updated_at, is_deleted, batch_id
        )
        SELECT 
            product_id, product_name, category, brand, price, discount, rating, stock,
            weight_g, color, created_at, updated_at, is_deleted, @batch_id
        FROM bronze.staging_products
        WHERE TRY_CAST(updated_at AS DATETIME) > @last_ingestion;
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
        TRUNCATE TABLE bronze.orders;

        INSERT INTO bronze.orders (
            order_id, customer_id, order_status, shipping_method, payment_terms,
            shipping_fee, created_at, updated_at, is_deleted, batch_id
        )
        SELECT 
            order_id, customer_id, order_status, shipping_method, payment_terms,
            shipping_fee, created_at, updated_at, is_deleted, @batch_id
        FROM bronze.staging_orders;
    END
    ELSE
    BEGIN
        INSERT INTO bronze.orders (
            order_id, customer_id, order_status, shipping_method, payment_terms,
            shipping_fee, created_at, updated_at, is_deleted, batch_id
        )
        SELECT 
            order_id, customer_id, order_status, shipping_method, payment_terms,
            shipping_fee, created_at, updated_at, is_deleted, @batch_id
        FROM bronze.staging_orders
        WHERE TRY_CAST(updated_at AS DATETIME) > @last_ingestion;
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
        TRUNCATE TABLE bronze.order_items;

        INSERT INTO bronze.order_items (
            order_item_id, order_id, product_id, quantity, unit_price, tax,
            discount_amount, fulfilled_by, created_at, updated_at, batch_id
        )
        SELECT 
            order_item_id, order_id, product_id, quantity, unit_price, tax,
            discount_amount, fulfilled_by, created_at, updated_at, @batch_id
        FROM bronze.staging_order_items;
    END
    ELSE
    BEGIN
        INSERT INTO bronze.order_items (
            order_item_id, order_id, product_id, quantity, unit_price, tax,
            discount_amount, fulfilled_by, created_at, updated_at, batch_id
        )
        SELECT 
            order_item_id, order_id, product_id, quantity, unit_price, tax,
            discount_amount, fulfilled_by, created_at, updated_at, @batch_id
        FROM bronze.staging_order_items
        WHERE TRY_CAST(updated_at AS DATETIME) > @last_ingestion;
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
        TRUNCATE TABLE bronze.payments;

        INSERT INTO bronze.payments (
            payment_id, order_id, amount, payment_method, payment_gateway,
            payment_status, currency, exchange_rate, created_at, updated_at, batch_id
        )
        SELECT 
            payment_id, order_id, amount, payment_method, payment_gateway,
            payment_status, currency, exchange_rate, created_at, updated_at, @batch_id
        FROM bronze.staging_payments;
    END
    ELSE
    BEGIN
        INSERT INTO bronze.payments (
            payment_id, order_id, amount, payment_method, payment_gateway,
            payment_status, currency, exchange_rate, created_at, updated_at, batch_id
        )
        SELECT 
            payment_id, order_id, amount, payment_method, payment_gateway,
            payment_status, currency, exchange_rate, created_at, updated_at, @batch_id
        FROM bronze.staging_payments
        WHERE TRY_CAST(updated_at AS DATETIME) > @last_ingestion;
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
GO
