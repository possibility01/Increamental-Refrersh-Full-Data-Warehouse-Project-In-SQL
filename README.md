
# Data Warehouse with Incremental Load - SQL Server Implementation

Welcome to the **Data Warehouse** repository! 🚀  
This project demonstrates a comprehensive data warehousing Designed as a portfolio project, it highlights industry best practices in data engineering.

---

## 🎯 Project Overview

This project implements a **production-ready data warehouse** using SQL Server with a **medallion architecture** (Bronze-Silver-Gold) and **incremental loading** capabilities. The system is designed to efficiently process and transform e-commerce transactional data into analytics-ready dimensional models.

### Key Features

✅ **Incremental ETL Processing** - Only loads changed data after initial load  
✅ **Medallion Architecture** - Three-layer data refinement (Bronze → Silver → Gold)  
✅ **Star Schema Design** - Optimized dimensional model for BI tools  
✅ **Data Quality & Cleansing** - Automated validation and standardization  
✅ **Batch Tracking** - Complete data lineage and audit trail  
✅ **Transaction Safety** - ACID compliance with error handling  
✅ **Performance Optimized** - Efficient BULK INSERT and MERGE operations

### Business Use Cases

- **Sales Analytics**: Revenue trends, product performance, customer segmentation
- **Operational Reporting**:  inventory tracking, payment processing
- **Customer Intelligence**: Lifetime value, loyalty analysis, demographic insights
- **Financial Reporting**: Tax collection, discount analysis, multi-currency support

---


## 🏗️ Data Architecture

The data architecture for this project follows Medallion Architecture **Bronze**, **Silver**, and **Gold** layers: ![Data Architecture](https://github.com/possibility01/Increamental-Refrersh-Full-Data-Warehouse-Project-In-SQL/blob/master/Docs/Data%20Architecture.jpg)

The project implements a **three-tier medallion architecture** for progressive data refinement:
```
┌─────────────────────────────────────────────────────────────────┐
│                         DATA SOURCES                            │
│                    CSV Files (Flat Files)                       │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ↓
┌─────────────────────────────────────────────────────────────────┐
│                      BRONZE LAYER (Raw)                         │
│  • Staging tables (temporary landing zone)                     │
│  • Persistent tables (historical raw data)                     │
│  • Batch tracking with batch_id                                │
│  • No transformations - data "as-is"                           │
│  • Change Data Capture via updated_at timestamps              │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ↓
┌─────────────────────────────────────────────────────────────────┐
│                    SILVER LAYER (Cleansed)                      │
│  • Data quality rules applied                                  │
│  • Standardization (lowercase, trim, character replacement)   │
│  • NULL handling with explicit defaults                        │
│  • Business rule validation                                    │
│  • Single version of truth                                     │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ↓
┌─────────────────────────────────────────────────────────────────┐
│                  GOLD LAYER (Analytics-Ready)                   │
│  • Star schema dimensional model                               │
│  • Dimension tables (Customers, Products, Payments,date)           │
│  • Fact table (Order Sales)                                    │
│  • Surrogate keys for performance                              │
│  • Denormalized for query optimization                         │
│  • BI tool integration ready                                   │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ↓
┌─────────────────────────────────────────────────────────────────┐
│              ANALYTICS & REPORTING LAYER                        │
│  Power BI • Tableau • Excel • SSRS • Custom Dashboards         │
└─────────────────────────────────────────────────────────────────┘

### Layer Responsibilities

| Layer | Purpose | Data Quality | Structure | Users |
|-------|---------|-------------|-----------|-------|
| **Bronze** | Landing & Audit | None (raw) | Normalized | Data Engineers |
| **Silver** | Cleansing & Standardization | High | Normalized | Data Engineers |
| **Gold** | Business Analytics | Highest | Denormalized | Business Analysts |

# 📊 Database Structure

### Schema Organization

DataWarehouse
```text
├── bronze (Raw Data Layer)
│   ├── Tables
│   │   ├── staging_customers
│   │   ├── customers
│   │   ├── staging_products
│   │   ├── products
│   │   ├── staging_orders
│   │   ├── orders
│   │   ├── staging_order_items
│   │   ├── order_items
│   │   ├── staging_payments
│   │   ├── payments
│   │   └── bronze_control (metadata)
│   └── Stored Procedures
│       ├── control_table
│       ├── staging_tables
│       └── inital_increamental_load
│
├── silver (Cleansed Data Layer)
│   ├── Tables
│   │   ├── customers
│   │   ├── products
│   │   ├── orders
│   │   ├── order_items
│   │   ├── payments
│   │   └── control_table (metadata)
│   └── Stored Procedures
│       ├── silver_control_table
│       └── inital_incremental_load
│
└── gold (Analytics Layer)
    └── Views
        ├── dim_customers
        ├── dim_product
        ├── dim_payments
        ├── dim_date
        └── fact_order_sales
```

### Data Model

#### Entity Relationship Diagram
```
CUSTOMERS(1) ────< ORDERS (1)────< ORDER_ITEMS (n)>──── (1) PRODUCTS
                      │
                      │
                      └────< PAYMENTS (1)
```
## 🔄 ETL Pipeline

### Pipeline Architecture
```
┌─────────────────────────────────────────────────────────────────┐
│                    PHASE 1: INITIALIZATION                      │
├─────────────────────────────────────────────────────────────────┤
│  1. bronze.control_table()        → Create Bronze metadata      │
│  2. silver.silver_control_table() → Create Silver metadata      │
└─────────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────────┐
│                 PHASE 2: BRONZE LAYER LOADING                   │
├─────────────────────────────────────────────────────────────────┤
│  3. bronze.staging_tables()                                     │
│     • TRUNCATE staging tables                                   │
│     • BULK INSERT from CSV files                                │
│     • Load 5 tables: customers, products, orders,               │
│       order_items, payments                                     │
│                                                                 │
│  4. bronze.inital_increamental_load()                           │
│     • Check last_ingestion_datetime in control table            │
│     • IF first run (≤2000-01-01): Full load all data            │
│     • ELSE: Incremental load (WHERE updated_at > last_load)     │
│     • MERGE staging → bronze persistent tables                  │
│     • Assign batch_id to all records                            │
│     • UPDATE control table with new timestamp                   │
└─────────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────────┐
│                 PHASE 3: SILVER LAYER TRANSFORMATION            │
├─────────────────────────────────────────────────────────────────┤
│  5. silver.inital_incremental_load()                            │
│     • Read from bronze persistent tables                        │
│     • Apply data quality rules:                                 │
│       - Standardize text (lowercase, trim)                      │
│       - Replace special characters (@→a, 1→i)                   │
│       - Handle NULLs (text→'N/A', numeric→0)                    │
│       - Validate email formats                                  │
│       - Apply business rules (quantity≥1)                       │
│     • MERGE into silver tables                                  │
│     • UPDATE control table                                      │
└─────────────────────────────────────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────────┐
│                 PHASE 4: GOLD LAYER (AUTOMATIC)                 │
├─────────────────────────────────────────────────────────────────┤
│  6. Gold views automatically reflect latest silver data         │
│     • dim_customers,dim_product,dim_payments,dim_date           │
│     • fact_order_sales                                          │
│     • No ETL needed - real-time views                           │
└─────────────────────────────────────────────────────────────────┘
```
### Incremental Loading Strategy

#### Control Table Pattern

Each layer maintains a control table tracking:
```sql
CREATE TABLE bronze.bronze_control (
    table_name NVARCHAR(50) PRIMARY KEY,
    last_ingestion_datetime DATETIME,      -- High-water mark
    last_batch_id NVARCHAR(50)             -- Batch identifier
);

```

## 🌟 Dimensional Model

### Star Schema Design

![Star Schema Design](https://github.com/possibility01/Increamental-Refrersh-Full-Data-Warehouse-Project-In-SQL/blob/master/Docs/Data%20Model.jpg)

### Dimension Tables

#### dim_customers

**Purpose:** Customer demographic and behavioral attributes  
**Grain:** One row per unique customer  
**Type:** Type 1 SCD (current state only)

**Key Attributes:**
- **customer_key** (INT): Surrogate key for fast joins
- **customer_id** (NVARCHAR): Natural key from source system
- **Demographics**: gender, age, city, marital_status
- **Behavioral**: loyalty_score, segment, preferred_device
- **Contact**: email, phone

#### dim_product

**Purpose:** Product catalog and inventory information  
**Grain:** One row per unique product  
**Type:** Type 1 SCD (current state only)

**Key Attributes:**
- **product_key** (INT): Surrogate key
- **product_id** (NVARCHAR): Natural key (SKU)
- **Hierarchy**: brand → category → product_name
- **Pricing**: price, rating
- **Physical**: weight_g, color, stock

#### dim_payments

**Purpose:** Payment transaction metadata  
**Grain:** One row per order (deduplicated)  
**Type:** Type 1 SCD

**Special Logic:** Deduplication per order
```sql
-- Handles multiple payment attempts per order
ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_id) AS rank_
WHERE rank_ = 1  -- Keep only first payment per order
```

**Key Attributes:**
- **payment_key** (INT): Surrogate key
- **payment_id** (NVARCHAR): Transaction identifier
- **payment_method**: credit_card, paypal, bank_transfer
- **payment_gateway**: stripe, square, braintree
- **payment_status**: completed, pending, failed, refunded
- **currency**, **exchange_rate**: Multi-currency support

### Fact Table

#### fact_order_sales

**Purpose:** Order line item sales transactions  
**Grain:** One row per order line item (most granular level)  
**Type:** Transaction fact table




**Foreign Keys:**
- **customer_key** → dim_customers
- **product_key** → dim_product
- **payment_key** → dim_payments

**Degenerate Dimensions** (stored in fact):
- order_id, order_item_id, order_status

**Measures (Metrics):**

| Measure | Type | Formula | Additive? |
|---------|------|---------|-----------|
| `quantity` | Base | Direct from source | Fully additive |
| `unit_price` | Base | Direct from source | Non-additive (use AVG) |
| `tax` | Base | Direct from source | Fully additive |
| `discount_amount` | Base | Direct from source | Fully additive |
| `shipping_fee` | Base | Order-level cost | Semi-additive* |
| `order_amount` | Calculated | `(unit_price × quantity) + tax - discount` | Fully additive |



## 🛡️ License

This project is licensed under the [MIT License](LICENSE). You are free to use, modify, and share this project with proper attribution.