
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
```
DataWarehouse
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
        └── fact_order_sales
```

### Data Model

#### Entity Relationship Diagram
```
CUSTOMERS (1) ────< ORDERS (1) ────< ORDER_ITEMS (n) >──── (1) PRODUCTS
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
│  1. bronze.control_table()        → Create Bronze metadata     │
│  2. silver.silver_control_table() → Create Silver metadata     │
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
│     • dim_customers, dim_product, dim_payments                  │
│     • fact_order_sales                                          │
│     • No ETL needed - real-time views                           │
└─────────────────────────────────────────────────────────────────┘

1. **Bronze Layer**: Stores raw data as-is from the source systems. Data is ingested from CSV Files into SQL Server Database. Bronze layer has a staging tables for each csv -bronze.staging_(name of the csv), which pupolate the real table that feeds the silver layer, which work of the staging table table to get the old data and new data from the source and use that to upsert into the real table for new record and update any changed record from 
2. **Silver Layer**: This layer includes data cleansing, standardization, and normalization processes to prepare data for analysis.
3. **Gold Layer**: Houses business-ready data modeled into a star schema required for reporting and analytics.

---
## 📖 Project Overview

This project involves:

1. **Data Architecture**: Designing a Modern Data Warehouse Using Medallion Architecture **Bronze**, **Silver**, and **Gold** layers.
2. **ETL Pipelines**: Extracting, transforming, and loading data from source systems into the warehouse.
3. **Data Modeling**: Developing fact and dimension tables optimized for analytical queries.
4. **Analytics & Reporting**: Creating SQL-based reports and dashboards for actionable insights.

🎯 This repository is an excellent resource for professionals and students looking to showcase expertise in:
- SQL Development
- Data Architect
- Data Engineering  
- ETL Pipeline Developer  
- Data Modeling  
- Data Analytics  

---

## 🛠️ Important Links & Tools:

Everything is for Free!
- **[Datasets](datasets/):** Access to the project dataset (csv files).
- **[SQL Server Express](https://www.microsoft.com/en-us/sql-server/sql-server-downloads):** Lightweight server for hosting your SQL database.
- **[SQL Server Management Studio (SSMS)](https://learn.microsoft.com/en-us/sql/ssms/download-sql-server-management-studio-ssms?view=sql-server-ver16):** GUI for managing and interacting with databases.
- **[Git Repository](https://github.com/):** Set up a GitHub account and repository to manage, version, and collaborate on your code efficiently.
- **[DrawIO](https://www.drawio.com/):** Design data architecture, models, flows, and diagrams.
- **[Notion](https://www.notion.com/):** All-in-one tool for project management and organization.
- **[Notion Project Steps](https://thankful-pangolin-2ca.notion.site/SQL-Data-Warehouse-Project-16ed041640ef80489667cfe2f380b269?pvs=4):** Access to All Project Phases and Tasks.

---

## 🚀 Project Requirements

### Building the Data Warehouse (Data Engineering)

#### Objective
Develop a modern data warehouse using SQL Server to consolidate sales data, enabling analytical reporting and informed decision-making.

#### Specifications
- **Data Sources**: Import data from two source systems (ERP and CRM) provided as CSV files.
- **Data Quality**: Cleanse and resolve data quality issues prior to analysis.
- **Integration**: Combine both sources into a single, user-friendly data model designed for analytical queries.
- **Scope**: Focus on the latest dataset only; historization of data is not required.
- **Documentation**: Provide clear documentation of the data model to support both business stakeholders and analytics teams.

---

### BI: Analytics & Reporting (Data Analysis)

#### Objective
Develop SQL-based analytics to deliver detailed insights into:
- **Customer Behavior**
- **Product Performance**
- **Sales Trends**

These insights empower stakeholders with key business metrics, enabling strategic decision-making.  

For more details, refer to [docs/requirements.md](docs/requirements.md).

## 📂 Repository Structure
```
data-warehouse-project/
│
├── datasets/                           # Raw datasets used for the project (ERP and CRM data)
│
├── docs/                               # Project documentation and architecture details
│   ├── etl.drawio                      # Draw.io file shows all different techniquies and methods of ETL
│   ├── data_architecture.drawio        # Draw.io file shows the project's architecture
│   ├── data_catalog.md                 # Catalog of datasets, including field descriptions and metadata
│   ├── data_flow.drawio                # Draw.io file for the data flow diagram
│   ├── data_models.drawio              # Draw.io file for data models (star schema)
│   ├── naming-conventions.md           # Consistent naming guidelines for tables, columns, and files
│
├── scripts/                            # SQL scripts for ETL and transformations
│   ├── bronze/                         # Scripts for extracting and loading raw data
│   ├── silver/                         # Scripts for cleaning and transforming data
│   ├── gold/                           # Scripts for creating analytical models
│
├── tests/                              # Test scripts and quality files
│
├── README.md                           # Project overview and instructions
├── LICENSE                             # License information for the repository
├── .gitignore                          # Files and directories to be ignored by Git
└── requirements.txt                    # Dependencies and requirements for the project
```
---


## 🛡️ License

This project is licensed under the [MIT License](LICENSE). You are free to use, modify, and share this project with proper attribution.

## 🌟 About Me

Hi there! I'm **Baraa Khatib Salkini**, also known as **Data With Baraa**. I’m an IT professional and passionate YouTuber on a mission to share knowledge and make working with data enjoyable and engaging!

Let's stay in touch! Feel free to connect with me on the following platforms:

[![YouTube](https://img.shields.io/badge/YouTube-red?style=for-the-badge&logo=youtube&logoColor=white)](http://bit.ly/3GiCVUE)
[![LinkedIn](https://img.shields.io/badge/LinkedIn-0077B5?style=for-the-badge&logo=linkedin&logoColor=white)](https://linkedin.com/in/baraa-khatib-salkini)
[![Website](https://img.shields.io/badge/Website-000000?style=for-the-badge&logo=google-chrome&logoColor=white)](https://www.datawithbaraa.com)
[![Newsletter](https://img.shields.io/badge/Newsletter-FF5722?style=for-the-badge&logo=substack&logoColor=white)](https://bit.ly/BaraaNewsletter)
[![PayPal](https://img.shields.io/badge/PayPal-00457C?style=for-the-badge&logo=paypal&logoColor=white)](https://paypal.me/baraasalkini)
[![Join](https://img.shields.io/badge/Join-FF0000?style=for-the-badge&logo=youtube&logoColor=white)](https://www.youtube.com/@datawithbaraa)
