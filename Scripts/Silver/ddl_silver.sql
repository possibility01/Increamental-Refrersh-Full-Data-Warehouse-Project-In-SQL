/*
===============================================================================
DDL Script: Create Silver Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'silver' schema, dropping existing tables 
    if they already exist.
	  Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/
USE DataWarehouse;
GO

IF OBJECT_ID ('silver.customers' ,'U') IS NOT NULL
DROP TABLE silver.customers;
 
CREATE TABLE silver.customers (
                               customer_id	NVARCHAR (50),
                               first_name NVARCHAR (50),
                               last_name NVARCHAR(50),
                               email NVARCHAR(50),
                               phone NVARCHAR(50),	
                               gender	NVARCHAR(50),
                               city	NVARCHAR(50),                          
                               age NVARCHAR(50),
                               income_level	NVARCHAR(50),
                               loyalty_score NVARCHAR(50),
                               segment	NVARCHAR(50),
                               preferred_device NVARCHAR(50),
                               marital_status NVARCHAR(50),
                               created_at DATETIME,
                               updated_at DATETIME,
                               is_deleted INT,
                               batch_id NVARCHAR(50),
                               dwh_created_date DATETIME DEFAULT GETDATE()
                               
                      );
GO

IF OBJECT_ID ('silver.products' ,'U') IS NOT NULL
DROP TABLE silver.products;
CREATE TABLE silver.products (
                      product_id NVARCHAR(50) ,
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
                      is_deleted INT,
                      batch_id NVARCHAR(50),
                      dwh_created_date DATETIME DEFAULT GETDATE()
                      );
GO
            
IF OBJECT_ID ('silver.orders' ,'U') IS NOT NULL
DROP TABLE silver.orders;
CREATE TABLE silver.orders ( 
                            order_id NVARCHAR(50),
                            customer_id NVARCHAR(50),
                            order_status NVARCHAR(50),
                            shipping_method	NVARCHAR(50),
                            payment_terms	NVARCHAR(50),
                            shipping_fee FLOAT,
                            created_at DATETIME,
                            updated_at DATETIME,
                            is_deleted INT ,
                            batch_id NVARCHAR(50),
                            dwh_created_date DATETIME DEFAULT GETDATE()
                            );
GO

IF OBJECT_ID ('silver.order_items' ,'U') IS NOT NULL
DROP TABLE silver.order_items;
CREATE TABLE silver.order_items (
                               order_item_id NVARCHAR (50),
                               order_id	NVARCHAR (50),
                               product_id NVARCHAR (50),
                               quantity	INT,
                               unit_price FLOAT,
                               tax	FLOAT,
                               discount_amount FLOAT,
                               fulfilled_by	NVARCHAR (50),    
                               created_at DATETIME,
                               updated_at DATETIME,
                               batch_id NVARCHAR(50),
                               dwh_created_date DATETIME DEFAULT GETDATE()
                               );

GO

IF OBJECT_ID ('silver.payments' ,'U') IS NOT NULL
DROP TABLE silver.payments;
CREATE TABLE silver.payments (
                            payment_id NVARCHAR(50),
                            order_id NVARCHAR(50),
                            amount FLOAT,
                            payment_method NVARCHAR(50),
                            payment_gateway NVARCHAR(50),
                            payment_status NVARCHAR(50),
                            currency NVARCHAR(50),
                            exchange_rate FLOAT,
                            created_at DATETIME,
                            updated_at DATETIME,
                            batch_id NVARCHAR(50),
                            dwh_created_date DATETIME DEFAULT GETDATE()
                            );

GO