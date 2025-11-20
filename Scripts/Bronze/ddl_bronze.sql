/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
=============================================================================
*/


IF OBJECT_ID ('bronze.products' , 'U' ) IS NOT NULL
    DROP TABLE bronze.products;

GO

CREATE TABLE bronze.products (

                      product_id INT ,
                      product_name NVARCHAR (50),
                      category NVARCHAR (50),
                      brand NVARCHAR (50),
                      price FLOAT,
                      discount INT,
                      rating INT,
                      stock INT,
                      weight_g INT,
                      color NVARCHAR (50),
                      created_at DATETIME
                      ,updated_at DATETIME,
                      is_deleted INT
                      );
GO


IF OBJECT_ID ('bronze.payments' , 'U' ) IS NOT NULL
    DROP TABLE bronze.payments;

CREATE TABLE bronze.payments (
                            payment_id INT,
                            order_id INT,
                            amount FLOAT,
                            payment_method NVARCHAR(50),
                            payment_gateway NVARCHAR(50),
                            payment_status NVARCHAR(50),
                            currency NVARCHAR(50),
                            exchange_rate FLOAT,
                            created_at DATETIME,
                            updated_at DATETIME
                      );
GO

IF OBJECT_ID ('bronze.orders' , 'U' ) IS NOT NULL
    DROP TABLE bronze.orders;

CREATE TABLE bronze.orders (
                            order_id INT,
                            customer_id INT,
                            order_status NVARCHAR(50),
                            shipping_method	NVARCHAR(50),
                            payment_terms	NVARCHAR(50),
                            shipping_fee FLOAT,
                            delivery_city NVARCHAR(50),
                            created_at DATETIME,
                            updated_at DATETIME,
                            is_deleted INT
                      );
GO

IF OBJECT_ID ('bronze.order_items' , 'U' ) IS NOT NULL
    DROP TABLE bronze.order_items;

CREATE TABLE bronze.order_items (
                               order_item_id INT,
                               order_id	INT,
                               product_id INT,
                               quantity	INT,
                               unit_price FLOAT,
                               tax	FLOAT,
                               discount_amount FLOAT,
                               fulfilled_by	NVARCHAR (50),    
                               created_at DATETIME,
                               updated_at DATETIME,
                               
                      );
GO

IF OBJECT_ID ('bronze.customers' , 'U' ) IS NOT NULL
    DROP TABLE bronze.customers;

CREATE TABLE bronze.customers (
                               customer_id	INT,
                               first_name NVARCHAR (50),
                               last_name NVARCHAR(50),
                               email NVARCHAR(50),
                               phone INT,	
                               gender	NVARCHAR(50),
                               city	NVARCHAR(50),
                               country NVARCHAR(50),
                               age INT,
                               income_level	NVARCHAR(50),
                               loyalty_score INT,
                               segment	NVARCHAR(50),
                               preferred_device NVARCHAR(50),
                               marital_status NVARCHAR(50),
                               created_at DATETIME,
                               updated_at DATETIME,
                               is_deleted INT
                               
                      );
GO