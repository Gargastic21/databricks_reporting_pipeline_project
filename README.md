# Automated Retail Reporting Pipeline using Databricks

## Project Overview

This project demonstrates an end-to-end Data Engineering pipeline built on Databricks using Unity Catalog, Volumes, Delta Tables, PySpark, and Databricks Workflows.

The solution automates data ingestion, transformation, and reporting processes to improve reporting efficiency and reduce manual operational effort.

The project follows the Medallion Architecture pattern:

Raw Data → Bronze → Silver → Gold

---

## Business Problem

Retail teams receive daily sales, customer, and product files in CSV format.

Challenges:

- Manual data processing
- Data quality issues
- Delayed reporting
- Lack of automation

This project solves these challenges by building an automated data pipeline that ingests, cleans, transforms, and aggregates retail data for reporting.

---

## Architecture

```text
CSV Files
   │
   ▼
Unity Catalog Volume
   │
   ▼
Bronze Layer (Raw Delta Tables)
   │
   ▼
Silver Layer (Cleaned Delta Tables)
   │
   ▼
Gold Layer (Business Reporting Tables)
   │
   ▼
SQL Analytics & Dashboards
```

---

## Technology Stack

| Component | Technology |
|------------|------------|
| Platform | Databricks Free Edition |
| Storage | Unity Catalog Volumes |
| Processing | PySpark |
| Tables | Delta Lake |
| Catalog | Unity Catalog |
| Reporting | Databricks SQL |
| Scheduling | Databricks Workflows |
| Language | Python, SQL |

---

## Project Structure

```text
retail-reporting-pipeline/

│
├── data/
│   ├── sales.csv
│   ├── customers.csv
│   └── products.csv
│
├── notebooks/
│   ├── 01_bronze_ingestion
│   ├── 02_silver_transformations
│   └── 03_gold_reporting
│
├── screenshots/
│
└── README.md
```

---

# Step 1: Create Catalog

```sql
CREATE CATALOG IF NOT EXISTS retail_catalog;
```

---

# Step 2: Create Schema

```sql
CREATE SCHEMA IF NOT EXISTS retail_catalog.retail_schema;
```

---

# Step 3: Create Volumes

Raw Volume:

```sql
CREATE VOLUME IF NOT EXISTS retail_catalog.retail_schema.raw_volume;
```

Processed Volume:

```sql
CREATE VOLUME IF NOT EXISTS retail_catalog.retail_schema.processed_volume;
```

---

# Step 4: Upload Source Files

Upload the following files to:

```text
Catalog
→ retail_catalog
→ retail_schema
→ Volumes
→ raw_volume
```

Files:

- sales.csv
- customers.csv
- products.csv

---

# Sample Data

## sales.csv

```csv
sale_id,customer_id,product_id,sale_date,quantity,amount
1,101,1001,2026-05-01,2,1200
2,102,1002,2026-05-01,1,800
3,103,1003,2026-05-02,3,1500
...
```

## customers.csv

```csv
customer_id,customer_name,city,state
101,Amit Sharma,Delhi,Delhi
102,Priya Verma,Gurugram,Haryana
...
```

## products.csv

```csv
product_id,product_name,category,price
1001,iPhone 15,Mobile,600
1002,Samsung TV,Electronics,800
...
```

---

# Step 5: Bronze Layer

Notebook:

```text
01_bronze_ingestion
```

Read Sales File:

```python
sales_path = "/Volumes/retail_catalog/retail_schema/raw_volume/sales.csv"

sales_df = (
    spark.read
    .format("csv")
    .option("header", "true")
    .option("nullValue", "null")
    .schema("""
        sale_id INT,
        customer_id INT,
        product_id INT,
        sale_date DATE,
        quantity INT,
        amount DOUBLE
    """)
    .load(sales_path)
)
```

Save Bronze Table:

```python
sales_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("retail_catalog.retail_schema.bronze_sales")
```

Similarly create:

- bronze_customers
- bronze_products

---

# Bronze Layer Responsibilities

- Store raw data
- Preserve source records
- Minimal transformations
- Support auditing

---

# Step 6: Silver Layer

Notebook:

```text
02_silver_transformations
```

Read Bronze Tables:

```python
sales_df = spark.table(
    "retail_catalog.retail_schema.bronze_sales"
)
```

Data Cleaning:

```python
from pyspark.sql.functions import *

clean_sales_df = (
    sales_df
    .dropDuplicates()
    .filter(col("quantity") > 0)
    .filter(col("amount") > 0)
    .withColumn("sale_date", to_date(col("sale_date")))
)
```

Write Silver Table:

```python
clean_sales_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("retail_catalog.retail_schema.silver_sales")
```

---

# Silver Layer Responsibilities

- Data cleansing
- Deduplication
- Data quality checks
- Standardized schema

---

# Step 7: Gold Layer

Notebook:

```text
03_gold_reporting
```

Read Silver Tables:

```python
sales_df = spark.table(
    "retail_catalog.retail_schema.silver_sales"
)

customer_df = spark.table(
    "retail_catalog.retail_schema.silver_customers"
)

product_df = spark.table(
    "retail_catalog.retail_schema.silver_products"
)
```

Business Aggregation:

```python
from pyspark.sql.functions import *

gold_df = (
    sales_df
    .join(customer_df, "customer_id")
    .join(product_df, "product_id")
    .groupBy("category")
    .agg(
        sum("amount").alias("total_sales"),
        count("*").alias("total_orders")
    )
)
```

Write Gold Table:

```python
gold_df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "retail_catalog.retail_schema.gold_sales_report"
    )
```

---

# Gold Layer Responsibilities

- Business KPIs
- Reporting tables
- Dashboard consumption
- Analytics-ready datasets

---

# Step 8: Data Quality Checks

Invalid Records:

```python
invalid_sales = (
    sales_df
    .filter(
        (col("quantity") <= 0) |
        (col("amount") <= 0)
    )
)

display(invalid_sales)
```

Duplicate Detection:

```python
sales_df.groupBy(
    sales_df.columns
).count().filter("count > 1")
```

---

# Step 9: Audit Columns

```python
from pyspark.sql.functions import current_timestamp

clean_sales_df = clean_sales_df.withColumn(
    "ingestion_timestamp",
    current_timestamp()
)
```

Benefits:

- Data lineage
- Auditing
- Troubleshooting

---

# Step 10: Incremental Processing

Instead of overwrite:

```python
clean_sales_df.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(
        "retail_catalog.retail_schema.silver_sales"
    )
```

Benefits:

- Faster processing
- Reduced compute usage
- Production-ready design

---

# Step 11: Databricks Workflow

Create a Workflow:

Task 1:

```text
01_bronze_ingestion
```

↓

Task 2:

```text
02_silver_transformations
```

↓

Task 3:

```text
03_gold_reporting
```

Schedule:

```text
Daily 08:00 AM
```

This automates the complete reporting pipeline.

---

# Step 12: Reporting Queries

## Category Sales

```sql
SELECT *
FROM retail_catalog.retail_schema.gold_sales_report
ORDER BY total_sales DESC;
```

## Daily Sales Trend

```sql
SELECT
    sale_date,
    SUM(amount) AS daily_sales
FROM retail_catalog.retail_schema.silver_sales
GROUP BY sale_date
ORDER BY sale_date;
```

## Customer Revenue

```sql
SELECT
    customer_id,
    SUM(amount) AS customer_revenue
FROM retail_catalog.retail_schema.silver_sales
GROUP BY customer_id
ORDER BY customer_revenue DESC;
```

---

# Expected Outcomes

- Automated data ingestion
- Reduced manual reporting effort
- Improved data quality
- Faster business reporting
- Reusable ETL framework

---

# Key Learnings

- Unity Catalog
- Databricks Volumes
- Delta Tables
- PySpark Transformations
- Medallion Architecture
- Data Quality Validation
- Databricks Workflows
- Reporting Layer Design

---

# Resume Bullet Points

- Built an end-to-end retail reporting pipeline using Databricks, PySpark, Delta Lake, Unity Catalog, and Databricks Workflows.

- Automated ingestion and transformation workflows using Medallion Architecture, reducing manual reporting effort and improving reporting efficiency.

- Developed Bronze, Silver, and Gold data layers with data quality validations, audit tracking, and business KPI reporting tables.

- Implemented Delta Lake storage and PySpark-based transformations to support scalable and reliable analytics workloads.

---

# Future Enhancements

- Auto Loader
- Change Data Capture (CDC)
- Slowly Changing Dimensions (SCD Type 2)
- Delta Live Tables
- Streaming Pipelines
- CI/CD Integration
- GitHub Actions Deployment
- Data Quality Framework
