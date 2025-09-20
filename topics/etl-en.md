## [Main title](../README.md)
# ETL

## Table of Contents
- [1. What is ETL?](#1-what-is-etl)
- [2. What is ELT?](#2-what-is-elt)
- [3. What are the key differences between ETL and ELT? When to use ETL and ELT?](#3-what-are-the-key-differences-between-etl-and-elt-when-to-use-etl-and-elt)
- [4. What is an ETL pipeline?](#4-what-is-an-etl-pipeline)
- [5. What are the triggers for ETL/ELT pipelines?](#5-what-are-the-triggers-for-etlelt-pipelines)
- [6. Explain the three-layer architecture of the ETL cycle](#6-explain-the-three-layer-architecture-of-the-etl-cycle)
- [7. What is a Data Warehouse (DWH)?](#7-what-is-a-data-warehouse-dwh)
- [8. What systems make up a Data Warehouse?](#8-what-systems-make-up-a-data-warehouse)
- [9. What are facts and dimensions in a Data Warehouse?](#9-what-are-facts-and-dimensions-in-a-data-warehouse)
- [10. What is a Snowflake Schema?](#10-what-is-a-snowflake-schema)
- [11. What is a Star Schema?](#11-what-is-a-star-schema)
- [12. What is a Galaxy Schema?](#12-what-is-a-galaxy-schema)
- [13. What is an OLAP system?](#13-what-is-an-olap-system)
- [14. What is an OLTP system?](#14-what-is-an-oltp-system)
- [15. What is the difference between OLAP and OLTP systems?](#15-what-is-the-difference-between-olap-and-oltp-systems)
- [16. What is the difference between Data Warehouse and Operational Database?](#16-what-is-the-difference-between-data-warehouse-and-operational-database)
- [17. What is an OLAP cube?](#17-what-is-an-olap-cube)
- [18. How are OLAP cubes structured?](#18-how-are-olap-cubes-structured)
- [19. What is idempotency and how to implement it in ETL?](#19-what-is-idempotency-and-how-to-implement-it-in-etl)
- [20. What is dbt (Data Build Tool)?](#20-what-is-dbt-data-build-tool)
- [21. What is orchestration in the context of ETL and data pipelines?](#21-what-is-orchestration-in-the-context-of-etl-and-data-pipelines)
- [22. What problems does Apache Airflow solve?](#22-what-problems-does-apache-airflow-solve)
- [23. What is a DAG and what does it mean in the context of ETL?](#23-what-is-a-dag-and-what-does-it-mean-in-the-context-of-etl)
- [24. How to solve the problem of concurrent data access in ETL?](#24-how-to-solve-the-problem-of-concurrent-data-access-in-etl)
- [25. What ETL Architecture Patterns do you know?](#25-what-etl-architecture-patterns-do-you-know)
- [26. What are Data Lake and Lakehouse?](#26-what-are-data-lake-and-lakehouse)
- [27. What approaches exist for error handling during pipeline execution?](#27-what-approaches-exist-for-error-handling-during-pipeline-execution)
- [28. What are Slowly Changing Dimensions (SCD)?](#28-what-are-slowly-changing-dimensions-scd)
- [29. What are Long Updating Dimensions (LUD)?](#29-what-are-long-updating-dimensions-lud)
- [30. Practical task](#30-practical-task)


## 1. What is ETL?

**ETL (Extract, Transform, Load)** is a process in data engineering and analytics used to extract data from different sources, transform it into a convenient format, and load it into a centralized storage (usually a data warehouse).

### ETL Stages
1. **Extract (extraction):**
   - Data is collected from various sources:
      - databases,
      - APIs,
      - file systems,
      - third-party platforms.
   - Main goal: collect all necessary data.

2. **Transform (transformation):**
   - Data is cleaned, standardized, enriched, combined, or aggregated.
   - Examples of transformations:
      - removing duplicates,
      - normalizing formats (e.g., dates),
      - combining reference data,
      - calculating metrics.
   - Main goal: bring data to unified business rules and analytical requirements.

3. **Load (loading):**
   - Transformed data is loaded into the target system:
      - Data Warehouse (Snowflake, BigQuery, Redshift),
      - Data Lake,
      - analytical platform.
   - Main goal: prepare data for building reports, dashboards, and BI.

- Ensures **accuracy and consistency** of data.
- Makes data **structured and suitable for analysis**.
- Is the foundation for building **data pipelines** and **analytics platforms**.
- Accelerates analysts' access to information.

### Summary
ETL is a fundamental data processing workflow that ensures collection, processing, and delivery of information to analytical systems in a convenient and reliable form.

## 2. What is ELT?

**ELT (Extract, Load, Transform)** is a modern approach to data integration, most commonly used in cloud architectures.  
The main difference from ETL: data is first **extracted and loaded into storage**, and transformations are performed **inside the data warehouse itself** (e.g., Snowflake, BigQuery, Redshift).

### ELT Stages

1. **Extract (extraction):**
   - Data is taken from sources (databases, APIs, files).
   - Goal: quickly collect data in "raw" form.

2. **Load (loading):**
   - Extracted data is immediately loaded into **centralized storage** or **data lake** in unchanged form.

3. **Transform (transformation):**
   - All transformations are performed **inside the warehouse** using its native capabilities:
      - SQL queries,
      - specialized tools (e.g., **dbt**),
      - built-in parallel processing mechanisms.

### Key Features of ELT

1. **Uses the power of cloud DWH**  
   Modern warehouses (Snowflake, BigQuery, Redshift) are optimized for large-scale parallel processing.

2. **Faster loading (faster ingestion)**  
   Data is loaded without waiting for transformations → faster entry into the warehouse.

3. **Flexibility**  
   Raw data is available for different scenarios (not only analytics, but also data science, machine learning).

4. **Modularity and version control**  
   Works excellently with tools like **dbt**, allowing building modular, documented, and testable pipelines.

### Summary
ELT is an approach where the "raw" data layer is immediately saved to storage, and all transformations are performed on the DWH side.  
It's better suited for cloud solutions where you can scale computations and flexibly manage transformations.

## 3. What are the key differences between ETL and ELT? When to use ETL and ELT?

### Main Difference
- **ETL (Extract, Transform, Load):**
   - Data is first extracted (**Extract**),
   - then transformed outside the warehouse (by a separate ETL server or tool) (**Transform**),
   - and only then loaded into the warehouse in ready form (**Load**).

- **ELT (Extract, Load, Transform):**
   - Data is extracted (**Extract**),
   - immediately loaded into the warehouse in "raw" form (**Load**),
   - transformations are performed inside the warehouse itself (**Transform**) using its computational power (e.g., SQL).

### When to use **ETL**
- You work with **on-premise systems** or traditional DWH with limited computational resources (Teradata, Oracle).
- **Security and compliance** requirements require processing before loading.
- Complex transformations are more convenient to do **outside the warehouse** (in Python, Java).

### When to use **ELT**
- You work with **cloud-scalable DWH** (Snowflake, BigQuery, Redshift).
- Need to store "raw" data for **reprocessing or audit**.
- Want to use the advantages of built-in DWH capabilities for SQL transformations (and tools like **dbt**).
- **Flexibility, modularity, and CI/CD-friendly pipelines** are important.

### Comparison Table

| Characteristic              | **ETL**                              | **ELT**                                |
|-----------------------------|--------------------------------------|----------------------------------------|
| Operation order            | Extract → Transform → Load           | Extract → Load → Transform             |
| Where Transform is executed | External server / ETL tool           | Inside DWH (SQL, dbt, etc.)           |
| Suitable for                | On-premise systems, legacy DWH       | Cloud, scalable DWH                    |
| Loading speed               | Slower (waiting for transformations) | Faster (data loads immediately)       |
| Raw data storage            | Usually not saved                    | Saved for reprocessing/audit           |
| Flexibility and DevOps      | Limited                              | High (suitable for CI/CD)              |

### Summary
- **ETL** — classic approach, optimal for old systems and strict security requirements.
- **ELT** — modern cloud standard, allowing scaling processing and storing "raw" data for different scenarios.

## 4. What is an ETL pipeline?

**ETL pipeline** is an automated process (workflow) that moves data through three main stages:  
**Extract → Transform → Load**.

It's responsible for ensuring that data is sequentially extracted, transformed, and loaded into storage or analytical system.

### ETL Tools
1. **Classic ETL platforms**:
   - *Informatica, Talend, SSIS*.

2. **Custom code**:
   - in *Python, SQL, Apache Spark*.

3. **Modern orchestrators**:
   - *Apache Airflow, Prefect*.

### Types of Data Pipelines
1. **Batch pipeline**
   - Runs on schedule (e.g., every hour, once a day).
   - Suitable for regular reports and planned data loading.

2. **Real-time / Streaming pipeline**
   - Processes data "on the fly" as it arrives.
   - Uses technologies like *Kafka, Flink*.
   - Applied for real-time analytics and monitoring.

3. **Incremental pipeline**
   - Loads only new or changed data.
   - Very efficient approach, reduces system load.

### Summary
ETL pipeline is a "conveyor belt" for data. It can work in batch, real-time, or incrementally and is built using ETL tools, custom code, or modern orchestrators.

## 5. What are the triggers for ETL/ELT pipelines?

ETL/ELT pipelines can be triggered differently depending on the architecture and orchestration tools.  
A trigger determines the **condition for starting pipeline execution**.

### Main Types of Triggers

1. **Scheduled Trigger (by schedule, Cron-based)**
   - Pipeline runs at fixed intervals (hourly, daily).
   - Used in: *Airflow, dbt Cloud, GitHub Actions*.

2. **Upstream Dependency (dependency on other tasks, DAG-based)**
   - Pipeline starts after successful completion of another task or pipeline.
   - Often used in: *Airflow, Prefect*.

3. **Event-based Trigger**  
   Triggered by external events, for example:
   - appearance of new file (S3, FTP),
   - completion of synchronization (e.g., Fivetran),
   - message from *Kafka* or webhook.

4. **Manual Trigger**
   - Launch via CLI, UI, or API.
   - Applied for ad hoc tasks or debugging.
   - Example: button in *Airflow UI*, `dbt run` command.

5. **Git-based CI/CD Trigger**
   - Triggered by changes in version control system (Git).
   - Example: push to main branch triggers `dbt run` via GitHub Actions.

### Summary
Triggers allow flexible management of ETL/ELT pipeline launches:
- from simple schedule (cron),
- to event-based and CI/CD integrations.  
  This makes it easy to integrate pipelines into the overall data processing and delivery infrastructure.


## 6. Explain the three-layer architecture of the ETL cycle

**Three-layer architecture** in ETL is a design pattern where data processing is divided into **three sequential layers**.  
Goal: improve **clarity, manageability, and performance** of pipelines.

### Architecture Layers

1. **Staging Layer (loading/buffering layer)**
   - Here data is **extracted** from source systems and loaded **without transformations** or with minimal formatting.
   - Contains "raw", unfiltered data (*e.g.: stg_orders, stg_customers*).
   - Used for:
      - data validation,
      - backups,
      - reproducibility of loads.

2. **Core / Integration Layer (core/integration layer)**
   - At this level, data is **standardized and integrated** from different sources.
   - Performed: table joins, data enrichment (*e.g.: core_orders, core_customers*).
   - Business rules are applied:
      - date normalization,
      - duplicate removal.
   - **Single source of truth** is formed for facts and dimensions.

3. **Data Marts / Presentation Layer (data marts/presentation layer)**
   - Final layer where data is **aggregated and structured** for specific analytics and BI tasks.
   - Examples of ready tables: *sales_summary, customer_lifetime*.
   - Often organized in schemas:
      - **star schema**,
      - **snowflake schema**.

### Summary
Three-layer architecture allows:
- storing **raw data** for verification and recovery,
- building **standardized core** with business rules,
- providing **ready data marts** for analytics and BI.

## 7. What is a Data Warehouse (DWH)?

**Data Warehouse (data warehouse)** is a centralized system designed for **storing, integrating, and analyzing data** from different sources.  
DWH is used to support:
- reporting,
- business intelligence (BI),
- decision-making.

### Key Characteristics
1. **Storage of historical and structured data**
   - usually for many years,
   - data is immutable (append-only).

2. **Optimization for analytical queries (read-heavy)**
   - not for transactional processing (like OLTP),
   - but for complex aggregations and analytics.

3. **Support for complex operations**
   - aggregates,
   - joins,
   - time series analysis.

4. **Structuring by schemas**
   - *star schema*,
   - *snowflake schema*,
   - fact and dimension tables.

### Examples of Data Warehouses
- **Snowflake**
- **Azure Synapse Analytics**
- **Google BigQuery**
- **Amazon Redshift**

### Summary
DWH is the foundation of analytical infrastructure.  
It combines data from different systems, makes it structured and accessible for building BI reports, dashboards, and advanced analytics.

## 8. What systems make up a Data Warehouse?

### Main DWH Components

1. **Source Systems (data sources)**
   - Transactional databases (OLTP: PostgreSQL, MySQL, Oracle).
   - CRM, ERP, financial systems.
   - APIs, log files, IoT, external data.

2. **ETL/ELT Layer (data loading process)**
   - Used for **extracting, transforming, and loading** data.
   - Tools: *Informatica, Talend, SSIS, dbt, Apache Airflow, Fivetran, Spark*.
   - Forms staging and core layers.

3. **Staging Area (loading buffer/raw layer)**
   - Stores "raw" data. (Snowflake)
   - Used for validation and reproducibility.

4. **Integration / Core Layer (warehouse core)**
   - Here data is cleaned, normalized, combined.
   - **Single source of truth** is formed.

5. **Data Marts (data marts)**
   - Optimized for specific analytical tasks.
   - Often built as **star schema** or **snowflake schema**.
   - Examples: *sales_mart, customer_mart*.

6. **Metadata & Master Data Management**
   - Metadata describes sources, transformations, lineage.
   - MDM is responsible for unified reference data (e.g., customer list).

7. **BI & Analytics Tools (consumption tools)**
   - Reporting and visualization: *Power BI, Tableau, Looker, Qlik*.
   - Data Science and ML: Python, R, Databricks.

### Summary
DWH is not just a database, but a **whole ecosystem** that includes:
- data sources,
- loading layer (ETL/ELT),
- staging, core, and data marts,
- metadata management,
- BI tools.

## 9. What are facts and dimensions in a Data Warehouse?

**Facts** are numerical, measurable data that describe business events or transactions.  
They are stored in **Fact Tables** and are usually subject to aggregation (summation, averaging, counting).

📌 Examples of facts:
- sale amount (`sales_amount`),
- quantity of goods sold (`quantity`),
- profit (`profit`),
- number of clicks (`clicks`),
- call duration (`duration`).

**Dimensions** are descriptive attributes that provide context for facts.  
They are stored in **Dimension Tables** and are used for grouping, filtering, and analytics.

📌 Examples of dimensions:
- **Time:** date, month, year, quarter,
- **Product:** name, category, brand,
- **Customer:** name, age, region, segment,
- **Store:** city, country, store type.

### Example:

**Fact Table: Sales**
```sql
    DateKey | ProductKey | CustomerKey | StoreKey | SalesAmount | Quantity
```
**Dimension: Product**
```sql
    ProductKey | ProductName | Category | Brand
```
**Dimension: Time**
```sql
    DateKey | Date | Month | Year | Quarter
```
**Dimension: Customer**
```sql
    CustomerKey | Name | Age | Region | Segment
```

### Summary:
- **Facts = what happened (numerical metrics).**
- **Dimensions = in what context it happened (time, customer, product, location).**

📊 Example analytical question:  
*"How many sales (fact) were made by customers from Moscow (dimension) in 2023 (dimension)?"*

## 10. What is a Snowflake Schema?

**Snowflake Schema** is a type of database schema in Data Warehouse where the fact table is connected to **normalized dimension tables**.  
The name "snowflake" comes from the fact that the diagram has a tree-like structure with branches resembling a snowflake.

### Structure
- **Fact Table**
   - contains numerical metrics (e.g., `sales_amount`, `quantity`, `profit`),
   - keys referencing dimension tables.

- **Dimension Tables**
   - describe context of facts (e.g., `customer`, `product`, `time`, `region`).
   - unlike Star Schema, dimensions here are **normalized**:
      - `product` table can be split into `product → category → department`,
      - `location` table can be split into `city → state → country`.

### Example
![SNOWFLAKE_SCHEMA](../images/snowflake-schema.webp)

### Advantages
- Memory savings (no duplication in dimensions).
- Improved data structure.
- Suitable for cases where **normalization** and avoiding redundancy are important.

### Disadvantages
- More complex queries (more JOINs needed).
- Slower compared to **Star Schema**.
- More difficult for business users to understand.

### Summary
- **Snowflake Schema** = normalized dimension model (multi-level structure).
- Used in Data Warehouse when you need to optimize storage and maintain strict data structure.
- Contrasts with **Star Schema**, which is simpler and faster, but stores data with redundancy.


## 11. What is a Star Schema?

**Star Schema** is a type of data organization in Data Warehouse where:
- in the center is a **Fact Table** with numerical metrics,
- around it are **Dimension Tables** directly connected to the fact table.

Since the connections form a structure resembling a star ⭐ — the schema is called **Star Schema**.

### Structure

### Example
![STAR_SCHEMA](../images/star-schema.png)

### Advantages of Star Schema
- Simplicity of understanding and use (closer to business logic).
- High query execution speed (fewer JOINs).
- Excellent for BI tools (Power BI, Tableau).
- Convenient for building **OLAP cubes**.

### Disadvantages
- Data redundancy (e.g., product category name can be duplicated).
- Requires more storage space than Snowflake Schema.

### Difference from Snowflake Schema
- In **Star Schema** dimension tables are **denormalized** (all attributes stored in one table).
- In **Snowflake Schema** dimensions are **normalized** and can be split into several related tables.

### Summary
- **Star Schema** = simple and fast structure for analytics.
- Used when **performance and BI convenience** are important, not minimizing redundancy.

## 12. What is a Galaxy Schema?

**Galaxy Schema** (or **Fact Constellation Schema**) is a data organization model in Data Warehouse where **multiple fact tables** are used, which can **share common dimension tables**.

It's called **"galaxy schema"** or **"fact constellation"** because it combines several "stars" (Star Schemas) into a unified system.

### Example
![GALAXY_SCHEMA](../images/galaxy-schema.png)

### Advantages:
- Ability to analyze **multiple business processes** in one model.
- Space savings through shared dimension tables.
- Support for more complex analytics (e.g., comparing sales and returns).

### Disadvantages:
- More complex structure than Star or Snowflake Schema.
- Queries can be heavier (more JOINs).
- Requires careful design to avoid confusion.

### Difference from other schemas:
- **Star Schema** → one fact table + simple dimensions.
- **Snowflake Schema** → one fact table + normalized dimensions.
- **Galaxy Schema** → multiple fact tables + shared dimensions.

### Summary:
**Galaxy Schema** is used in large data warehouses where you need to model and analyze **multiple business processes simultaneously**: sales, returns, logistics, finance, etc.

## 13. What is an OLAP system?

**OLAP (Online Analytical Processing)** is a type of system designed to support:
- **complex data analysis**,
- **reporting**,
- **decision-making**,

usually on large volumes of **historical data**.

### Main OLAP Characteristics:
1. **Historical data storage** — aggregated and structured data from different sources.
2. **Optimization for analytical queries** (read-heavy workload), not transactional processing.
3. **Support for multidimensional analysis**: dimensions (time, product, region) and facts (sales, profit).
4. **High performance for aggregations and slices** (drill-down, roll-up, slice & dice).
5. Often implemented based on **star schema** or **snowflake schema**.

### Examples of OLAP Systems:
- **Microsoft Analysis Services (SSAS)**,
- **Oracle OLAP**,
- **SAP BW**,
- **Snowflake, BigQuery, Redshift** (in BI and analytics context),
- **Cognos, MicroStrategy, Tableau** (as OLAP cube consumers).

### Summary:
OLAP system is a tool for building analytics on large historical data.  
It gives business the ability to quickly answer questions like:  
*"What was the revenue (fact) for 'Electronics' category (dimension) by regions over the last 3 years (dimension)?"*

## 14. What is an OLTP system?

**OLTP (Online Transaction Processing)** is a type of database management system designed for processing **operational transactions** in real time.  
Such systems are used for **daily business operations**.

### Main OLTP Characteristics:
1. **Operational transaction processing** (insert, update, delete).
2. **Support for large number of concurrent users**.
3. **Optimization for writing and fast access to individual rows**.
4. **Data stored in normalized form** to avoid redundancy.
5. High speed of simple queries (e.g.: find order by ID).

### Examples of OLTP Systems:
- E-commerce sites (creating and paying orders).
- Banking systems (transactions, payments, transfers).
- CRM and ERP systems (customer management, inventory, logistics).
- Databases: **PostgreSQL, MySQL, Oracle, SQL Server** (in OLTP mode).

### Summary:
- **OLTP** = system for **real-time operations** (create order, process payment).
- **OLAP** = system for **data analysis** (reports, analytics, forecasts).

## 15. What is the difference between OLAP and OLTP systems?

**OLTP (Online Transaction Processing)** and **OLAP (Online Analytical Processing)** solve different tasks in data work.  
OLTP is used for **operational transactions in real time**, while OLAP is for **analysis and reporting on large volumes of historical data**.

### OLTP vs OLAP Comparison

| Characteristic        | OLTP (operational systems) | OLAP (analytical systems) |
|------------------------|-----------------------------|-------------------------------|
| Purpose           | Daily transactions (order creation, payments, inventory) | Data analysis, reporting, decision-making |
| Data                | Current, detailed   | Historical, aggregated |
| Workload type          | Many write and update operations | Many read and aggregation operations |
| Storage model       | Normalized (3NF)       | Denormalized (Star, Snowflake Schema) |
| Number of users  | Thousands (operators, cashiers, managers) | Hundreds (analysts, BI, management) |
| Queries               | Simple and short (search by ID) | Complex, with aggregations and groupings |
| Performance    | Optimized for transactions | Optimized for analytics |
| System examples        | PostgreSQL, MySQL, Oracle (OLTP mode) | Snowflake, BigQuery, Redshift, SSAS |

### Summary:
- **OLTP** = fast transactions and real-time operations.
- **OLAP** = data analysis, building reports and business analytics.

📌 In practice, data from OLTP systems **enters OLAP through ETL/ELT processes**:  
👉 *OLTP (operational data) → ETL → Data Warehouse (OLAP for analysis)*.


## 16. What is the difference between Data Warehouse and Operational Database?

**Data Warehouse (DWH)** is a system designed for storing and analyzing **historical data**.  
It supports **analytical processing (OLAP)**, used for building reports and decision-making.

**Operational Database (OLTP)** is a system for managing **operational data** in real time.  
It supports **transactional processing** and is used for daily business operations.

### Data Warehouse vs Operational Database Comparison

| Characteristic          | Data Warehouse (DWH)         | Operational Database (OLTP) |
|--------------------------|------------------------------|------------------------------|
| Main purpose            | Analytics and reporting       | Real-time operational transactions |
| Data type               | Historical, aggregated | Current, actual |
| Workload type             | **Reading (read-heavy)**      | **Writing and updates (write-heavy)** |
| Queries                  | Complex, with aggregations       | Simple, fast transactions |
| Number of users     | Analysts, BI, management   | Managers, cashiers, customers |
| Storage model          | Denormalized (star, snowflake) | Normalized (3NF) |
| Data volume             | Very large (TB–PB)        | Comparatively smaller |
| System examples           | Snowflake, BigQuery, Redshift | PostgreSQL, MySQL, Oracle, SQL Server |

### Summary:
- **Data Warehouse (OLAP)** = storage for **analytics, reports, strategic decisions**.
- **Operational Database (OLTP)** = database for **real-time operations** (payments, orders, CRM).

📌 In practice:  
👉 Data from **OLTP** systems is transferred to **DWH** using **ETL/ELT processes**.

## 17. What is an OLAP cube?

**OLAP Cube** is a multidimensional data structure used in **OLAP systems** for fast analysis of large volumes of information.  
It's called a "cube" because data is organized not in flat tables, but in **multidimensional dimensions**, allowing analysis from different angles.

### Main OLAP Cube Elements:
1. **Facts** — numerical indicators (sales, profit, number of orders).
2. **Dimensions** — context for facts:
   - Time (year, month, day),
   - Geography (country, city, region),
   - Product (category, brand, model),
   - Customer (age, segment).
3. **Measures** — fact aggregates (SUM, AVG, COUNT).

### Example:
**Analyst's question:**  
*"What is the total sales amount (fact) by products (dimension) in regions (dimension) by months (dimension)?"*

In OLAP cube:
- X-axis = Time (Year → Month → Day),
- Y-axis = Products (Category → Brand → SKU),
- Z-axis = Geography (Region → City).

At the intersection of these dimensions, the metric is stored: **SalesAmount**.

### OLAP Cube Operations:
- **Slice** — selecting one slice (e.g., sales only for 2024).
- **Dice** — selecting subset by multiple dimensions (e.g., phone sales in Europe in 2023).
- **Drill-down** — detailing (year → month → day).
- **Roll-up** — aggregation (day → month → year).
- **Pivot (Rotate)** — rotating cube to change analysis perspective.

### OLAP Cube Advantages:
- Very fast analytical query execution.
- Convenience of multidimensional analysis.
- Support for business users (BI, reports, dashboards).

### Summary:
**OLAP Cube** is a way to store and analyze data in multidimensional structure that allows performing complex analytical queries (aggregations, groupings) quickly and conveniently.

## 18. How are OLAP cubes structured?

**OLAP Cube (Online Analytical Processing Cube)** is a multidimensional data structure used for analyzing large volumes of information.  
It stores **facts (measures)** and connects them with **dimensions**, forming multidimensional space for analytics.

### Main Cube Elements
- **Facts** — numerical indicators that are analyzed (e.g.: sales, number of orders, profit).
- **Dimensions** — attributes that provide context for facts (e.g.: time, product, region, customer).
- **Measures** — fact aggregates (SUM, AVG, COUNT).
- **Cube cells** — intersection of dimensions with specific fact values.

### Logical Structure
OLAP cube looks like an **n-dimensional array**:
- 3 dimensions → cube (3D),
- 4 dimensions → hypercube (4D),
- in reality, cube can have dozens of dimensions.

### Physical OLAP Cube Implementation
There are three approaches:

1. **MOLAP (Multidimensional OLAP)**
   - Data is stored in multidimensional arrays.
   - Very fast access (O(1)), as data is indexed by coordinates.
   - Problem: many empty cells (sparse data) → compression needed.

2. **ROLAP (Relational OLAP)**
   - Data is stored in relational tables (star, snowflake).
   - Cube is implemented as Fact Table + Dimension Tables.
   - Queries executed via SQL (JOIN + GROUP BY).
   - Slower, but scales better.

3. **HOLAP (Hybrid OLAP)**
   - Hybrid approach: aggregates stored in multidimensional structure (MOLAP),
   - detailed data — in relational DB (ROLAP).
   - Balance of speed and flexibility.

### Data Storage and Search
- Cube data is usually **sparse**, so only non-empty cells are stored.
- For faster search, the following are used:
   - **Bitmap indexing** (masks for categorical attributes),
   - **Pre-aggregation** (materialized views),
   - **Star-join optimization** (SQL query optimization).

### OLAP Cube Operations
- **Slice** — selecting one dimension (e.g., sales only for 2024).
- **Dice** — selecting subset by multiple dimensions (e.g., phone sales in Europe).
- **Drill-down** — detailing (year → month → day).
- **Roll-up** — aggregation (day → month → year).
- **Pivot** — rotating cube to change analysis angle.

### Summary:
OLAP cubes are **multidimensional data structures** where:
- facts (numbers) are connected with dimensions (context),
- data can be stored in multidimensional arrays (MOLAP), relational tables (ROLAP), or hybrid (HOLAP),
- support fast multidimensional analysis through slice, dice, drill-down, and roll-up operations.

## 19. What is idempotency and how to implement it in ETL?

**Idempotency** is a property of an operation where repeated execution gives the same result as single execution.

In the context of **ETL**, this means that if the same data loading or transformation process is executed multiple times, the target system state won't change incorrectly (no duplicates, repeated aggregation, etc.).

### Why Idempotency is Needed in ETL
- **Reliability:** in case of failures or job crashes, you can safely restart the loading.
- **Predictability:** repeated runs always lead to the same result.
- **Production safety:** especially critical when working with big data and incremental updates.

### Examples of Idempotency in ETL

1. **Loading data into staging table with full cleanup**
```sql
   TRUNCATE TABLE staging_orders;
    INSERT INTO staging_orders SELECT * FROM source_orders;
```
- Repeated run always gives the same staging state.

2. **Using UPSERT (MERGE) instead of INSERT**
```sql
   MERGE INTO target_orders AS t
   USING staging_orders AS s
   ON t.order_id = s.order_id
   WHEN MATCHED THEN UPDATE SET t.amount = s.amount
   WHEN NOT MATCHED THEN INSERT (order_id, amount) VALUES (s.order_id, s.amount);
```
- Repeated run won't create duplicates.

3. **Unique keys and constraints**
```sql
   CREATE UNIQUE INDEX idx_orders_id ON target_orders(order_id);
```
- Even with erroneous repeated insertion, duplicate won't appear.

4. **Idempotency Key in streaming loads**
- For each data batch, store unique loading identifier (batch_id).
- Before loading, check if this batch_id was already processed.
```sql
   IF NOT EXISTS (SELECT 1 FROM etl_log WHERE batch_id = '2024-08-26-001')
   THEN insert data
   ELSE skip
```

### Approaches to Implementing Idempotency in ETL
1. **Full rewrite (full refresh)** — clean and load again.
2. **Incremental loading with upsert** — update only new/changed records.
3. **Hash comparison** — compare row hashes to determine changes.
4. **Batch_id logging** — exclude repeated processing of the same data batch.

### Summary
In ETL, idempotency means that the process can be safely restarted without risk of duplication or data corruption.  
Implemented through:
- `TRUNCATE + INSERT`,
- `UPSERT (MERGE)`,
- unique keys,
- batch_id control,
- row hashing.

## 20. What is dbt (Data Build Tool)?

**dbt (Data Build Tool)** is an open-source framework for **transforming data inside a warehouse (data warehouse)** using SQL, while using best practices from software development: modularity, testing, version control.

### Main dbt Capabilities
1. **SQL as primary language**
   - All transformations are written in SQL, without complex ETL code.
   - dbt manages dependencies and execution order.

2. **Building models**
   - SQL scripts become **tables or views** inside the warehouse.
   - Example model:
     ```
     SELECT
         customer_id,
         SUM(amount) AS total_sales
     FROM raw.sales
     GROUP BY customer_id
     ```

3. **Dependency management**
   - Uses `ref()` macro for references to other models.
   - dbt builds DAG (dependency graph) and runs models in correct order.

4. **Incremental loading and materialization**
   - Support for **incremental refresh** (updating only new data).
   - Different materialization strategies: `view`, `table`, `incremental`, `ephemeral`.

5. **Version control and CI/CD**
   - Full Git integration.
   - Support for automated pipelines (GitHub Actions, GitLab CI).

6. **Testing and documentation**
   - Ability to set data quality tests (e.g., `unique`, `not_null`).
   - Automatic documentation and lineage generation (dependency chains).

7. **Support for modern cloud DWH**
   - Works with **Snowflake, BigQuery, Redshift, Databricks, Postgres**.

### Summary
**dbt** is a tool that allows:
- managing transformations **directly in the warehouse**,
- writing everything in SQL, but using engineering practices (Git, CI/CD, testing),
- easily building repeatable and transparent data pipelines.

It has become the de facto standard for **Modern Data Stack**.


## 21. What is orchestration in the context of ETL and data pipelines?

**Orchestration** is automated coordination, planning, and monitoring of task execution within a data pipeline.  
It ensures task launches:
- **in correct order**,
- **at correct time**,
- **with correct parameters**,
- and guarantees proper error handling.

### Main Orchestration Tasks
1. **Dependency management**
   - Task B starts only after successful completion of task A.

2. **Scheduling**
   - Tasks execute on schedule (e.g., every day at 2:00 AM).

3. **Error handling and retries**
   - Automatic retry on failure.
   - Sending notifications (e.g., to Slack or email).

4. **Parallelization and resource optimization**
   - Multiple tasks can execute simultaneously if no dependencies.
   - Flexible load distribution.

5. **Environment and parameter control**
   - Separation of execution in dev/staging/prod.
   - Parameter passing to pipelines.

### Examples of Orchestration Tools
- **Apache Airflow** — most popular tool (DAGs, Python + UI).
- **Prefect** — modern analog, focus on simplicity and cloud integration.
- **Luigi** — used for building pipelines.
- **Dagster** — data orchestrator with advanced testing capabilities.
- **Azure Data Factory / AWS Step Functions / GCP Composer** — cloud orchestration solutions.

### Example Scenario
ETL pipeline for loading sales data:
1. **Extract** — load CSV from S3.
2. **Transform** — clean data (remove duplicates, normalize dates).
3. **Load** — load data into DWH (e.g., Snowflake).
4. **Report** — build aggregated tables for BI.

Orchestrator guarantees that steps execute sequentially and will restart in case of error.

### Summary
Orchestration is the "conductor" in the ETL world: it manages dependencies, launch timing, retries, and pipeline monitoring. Without it, complex ETL/Data Engineering processes become chaotic and unreliable.

## 22. What problems does Apache Airflow solve?

**Apache Airflow** is an open-source platform for managing and scheduling workflows (ETL, data pipelines, analytics).  
It's used for automating complex data processing processes built from multiple dependent tasks.

### Main Problems Airflow Solves

1. **Pipeline management**
   - Allows **describing, planning, and monitoring** pipelines as DAG (Directed Acyclic Graph).
   - All dependencies between tasks are explicitly defined.

2. **Centralized logging**
   - All task execution logs are available in one place.
   - Convenient for monitoring and debugging.

3. **Error handling**
   - Ability to configure **callbacks** on failure (e.g., notifications to Slack/Discord/email).
   - Support for retry policies (repeated execution attempts).

4. **User interface (UI)**
   - Web interface for visualizing DAGs and monitoring progress.
   - Ability to manually restart failed tasks.

5. **Tool integration**
   - Support for working with databases, clouds (AWS, GCP, Azure), Spark, dbt, Kubernetes, etc.
   - Uses operators and sensors for integration.

6. **Flexibility and extensibility**
   - Pipelines are described in **Python**, allowing use of loops, conditions, parameters.
   - Can create custom operators.

7. **Open-source and community**
   - Free and actively supported by community.
   - Many plugins and ready integrations.

### Example Usage
Airflow is often used for:
- ETL/ELT processes (extract → transform → load into DWH),
- ML pipeline runs (model training on schedule),
- analytical report automation,
- managing complex workflows in production.

### Summary
Apache Airflow solves the problem of **orchestration and data pipeline management**:  
it provides transparency, control, integration with various systems, and reliable error handling, making it the de facto standard for modern data engineering teams.

## 23. What is a DAG and what does it mean in the context of ETL?

**DAG (Directed Acyclic Graph)** is a **directed acyclic graph**, a structure where:
- **nodes** — are tasks (e.g., extract, transform, load),
- **edges** — dependencies between tasks,
- **no cycles** — a task cannot depend on itself directly or indirectly.

### DAG in ETL Processes
In ETL context, DAG describes the sequence of step execution:
1. **Extract** — loading data from sources,
2. **Transform** — cleaning, aggregation, enrichment,
3. **Load** — loading into warehouse or data mart.

Each task starts **only after completion of previous ones**, guaranteeing **logical and predictable order** of data processing.

### Example DAG for Sales Data Processing
```
   [ extract_crm_data ]
   ↓
   [ transform_customers ]
   ↓
   [ join_sales_and_customers ]
   ↓
   [ aggregate_sales_by_region ]
   ↓
   [ export_to_dashboard ]
```
- Each task starts only after completion of the previous one.
- Executes once (or with parameters, but without looping).

### Where DAGs are Used?
1. **Apache Airflow**
   - Each pipeline in Airflow is a DAG.
   - DAG is described in Python.

   Example:
   ```python
   with DAG('sales_pipeline') as dag:
       t1 = PythonOperator(task_id='extract', python_callable=extract_func)
       t2 = PythonOperator(task_id='transform', python_callable=transform_func)
       t3 = PythonOperator(task_id='load', python_callable=load_func)

       t1 >> t2 >> t3   # execution order
   ```
2. Prefect / Luigi / Dagster — alternative orchestrators where pipelines are also represented as DAGs.

### Why DAGs are Needed?
- Explicitly show dependencies between tasks.
- Guarantee correct execution order.
- Ensure repeatability and reliability of pipelines.
- Simplify monitoring and maintenance of ETL processes.

### Summary
DAG is the foundation of ETL orchestration.
It turns a complex process of multiple tasks into a clear dependency scheme, allowing systems like Airflow to manage pipelines transparently and predictably.

## 24. How to solve the problem of concurrent data access in ETL?

### Main Goals
- Avoid duplication (double data processing)
- Prevent write conflicts
- Guarantee consistency for reading processes
- Make repeated runs safe (idempotency)

### 1. Idempotent Records
- Use **upsert/merge** instead of simple inserts.
- Create **unique indexes** on business keys.
- Track **batch_id** and skip already loaded batches.

### 2. Managing Concurrent Writes
### a) Single-writer (one writer)
Use **locks or leases**
### b) At orchestrator level
- In Airflow use **pools** or concurrency limits.
- In distributed systems — **Redis/ZooKeeper locks**.

### c) Partition separation
- Each process writes only to its **date/region/shard** to avoid intersections.

### 3. Isolating Readers from Writers
### a) Snapshots (MVCC / snapshot isolation)
- Readers get consistent data view.
- In warehouses — use copy-on-write (Delta Lake, Iceberg, Hudi).

### b) Staging + atomic replacement
- Load data into temporary table, then change reference.

### 4. Exactly-once and Deduplication
- Store **offset / watermark** (e.g., max id or timestamp).
- Add **dedupe key** (business key hash + time).

### 5. Working with Files (Data Lake)
- First write to temporary folder: `/incoming/_tmp/run123/*`
- Then **atomically rename** to working directory.
- For ACID operations use Delta/Iceberg/Hudi.

### 6. SCD (Slowly Changing Dimensions)
- For Type-2 control non-overlapping date ranges.

### 7. Error Handling and Retries
- All steps should be **idempotent**.
- Use retries with delay (**jitter**).

### 8. Observability
- Track: **row counts, checksums, list of files/partitions**.
- Check data quality after batch completion.

### Summary
For reliable ETL pipeline:
- Use **idempotency + unique keys**,
- Do loading through **staging and atomic swap**,
- Provide **snapshot isolation** for readers,
- Separate responsibility (partitioning / sharding),
- Store **offsets and batch_id** for control.

This approach eliminates most concurrent access problems and makes the process robust.

## 25. What ETL Architecture Patterns do you know?

### 1. Single-tier Architecture
**Description:**
- Data is extracted from sources and immediately loaded into analytical system without intermediate layers.

**Example:** Excel or Access, where data is loaded directly from CSV or database.

**Pros:**
- Simple implementation.
- Minimal delays (fewer steps).

**Cons:**
- No data cleaning before loading.
- No flexibility, difficult to scale.

### 2. Two-tier Architecture
**Description:**
- Sources → data warehouse.
- Transformations are performed on-the-fly during loading.

**Example:** Old DWH projects where ETL server (e.g., DataStage, SSIS) writes directly to Warehouse.

**Pros:**
- Fast building.
- Fewer systems to maintain.

**Cons:**
- Difficult to add new sources.
- No staging layer for debugging.

### 3. Three-tier Architecture (classic)
**Description:**
- Includes:
   1. **Staging Layer** → stores "raw" data.
   2. **Integration/Core Layer** → cleans, combines, and normalizes.
   3. **Presentation Layer / Data Marts** → aggregated data for analytics.

**Example:** Classic enterprise warehouses (Teradata, Oracle DWH).

**Pros:**
- Scalability and flexibility.
- Support for historicity (SCD).
- Data validation at each layer.

**Cons:**
- High storage cost.
- Delays due to batch loads.

### 4. Lambda Architecture
**Description:**
- Combines batch + streaming.
- Two paths:
   - Batch layer → loading large volumes (e.g., daily).
   - Speed/Stream layer → real-time event stream processing.

**Example:** IoT, click analytics in advertising.

**Pros:**
- Support for real-time and historical data.
- Balance between speed and completeness.

**Cons:**
- Logic duplication.
- Architecture complexity.

### 5. Kappa Architecture
**Description:**
- All data is processed in streaming, no batch.
- Historical data can be "replayed" through the same stream.

**Example:** Event-driven systems (Kafka + Flink / ksqlDB).

**Pros:**
- No batch/stream duplication.
- Minimal delay.

**Cons:**
- Requires powerful infrastructure.
- Difficult to implement complex transformations.

### 6. ELT (Extract → Load → Transform)
**Description:**
- Data is first loaded "as is" into DWH, then transformations are performed inside.
- Used in **cloud data warehouses**.

**Example:** BigQuery + dbt, Snowflake.

**Pros:**
- Scalability (DWH power).
- Simple maintenance (SQL).
- Suitable for agile development.

**Cons:**
- High DWH load.
- Limitations for complex business rules.

### 7. Data Lake + ETL (or Data Lakehouse)
**Description:**
- Data is saved to **Data Lake** (HDFS, S3, GCS).
- Then cleaned and transformed into DWH or Lakehouse.

**Example:** Raw JSON → S3 → Spark/Databricks → Delta Lake.

**Pros:**
- Storage of any data (structured and unstructured).
- Universality (Lake + Warehouse).

**Cons:**
- More complex maintenance.
- Requires data governance.

### 8. Micro-batch / Streaming ETL
**Description:**
- Intermediate option between batch and streaming.
- Data is processed in mini-batches (every 1–5 minutes).

**Example:** Spark Structured Streaming in micro-batch mode.

**Pros:**
- Almost real-time analytics.
- Simpler than full streaming.

**Cons:**
- Has delay (> 1 min).
- Not always suitable for critical real-time systems.

### ✨ Summary
- Small systems → **Single-tier / Two-tier**.
- Enterprise DWH → **Three-tier**.
- Real-time + Big Data → **Lambda or Kappa**.
- Cloud-native → **ELT**.
- Universal lake systems → **Lakehouse**.


## 26. What are Data Lake and Lakehouse?

### Data Lake
**Definition:**  
Data Lake is a centralized data storage that allows storing **huge volumes of data in raw form**, regardless of their format.  
Data can be structured, semi-structured, and unstructured.

**Features:**
- Storage of any data types: CSV, JSON, Parquet, logs, images, videos.
- Data is usually stored in **cloud file systems** (Amazon S3, Azure Data Lake, HDFS).
- Supports "schema-on-read" (schema is applied only during reading).

**Pros:**
- Flexibility — can store absolutely everything.
- Scalability and low storage cost.
- Suitable for Data Science, ML, AI.

**Cons:**
- "Data swamp" — without strict management and cataloging, data becomes unmanageable.
- Complexity of integration with BI tools (needs ETL/ELT).

### Data Lakehouse
**Definition:**  
Data Lakehouse is a hybrid architecture that combines advantages of **Data Lake (storage flexibility)** and **Data Warehouse (structure, ACID, query optimization)**.  
Essentially, it's **Data Lake + management layer + SQL analysis**.

**Key Technologies:**
- Delta Lake (Databricks),
- Apache Iceberg,
- Apache Hudi.

**Features:**
- Support for **ACID transactions** in storage.
- Support for **schema enforcement** and schema evolution.
- Ability to execute **SQL queries directly** on Lake data.
- Suitable for both analytics (BI) and ML/AI.

**Pros:**
- Universality: one layer for Data Engineering, BI, and Data Science.
- High performance (optimized formats: Parquet + metadata).
- Flexibility and scalability like Data Lake.

**Cons:**
- Relatively new technology (still developing).
- Requires implementation of metadata management systems.

### 💡 Summary
- **Data Lake** — is simply a "lake" for storing any data.
- **Lakehouse** — is a "lake with infrastructure" where data can be immediately used for both BI and ML, while maintaining control and transactional properties.

## 27. What approaches exist for error handling during pipeline execution?

### Main Approaches to Error Handling in ETL/ELT Pipelines

1. **Fail-fast + Alerting**
   - Execution stops at first critical error.
   - Immediately notify responsible parties (Slack, email, monitoring system).

2. **Dead-letter Queue (DLQ)**
   - Erroneous records (or tasks) **don't block main processing**.
   - They are redirected to separate storage for further analysis.

3. **Quarantine Layer / Error Table**
   - Erroneous rows are saved to special table  
     *(e.g.: `error_orders`, `quarantine_customers`)*.
   - Used when data schema or business rules are violated.

4. **Retry with exponential backoff**
   - Automatic task retry with exponentially increasing intervals.
   - Example: retry after 1 sec → 2 sec → 4 sec → 8 sec, etc.

5. **Split-by-task Isolation**
   - Dividing pipeline into independent tasks with separate tracking.
   - Example: order loading shouldn't fail due to customer data problems.

Thus, in real projects, several strategies are usually combined:
- **Fail-fast** for critical system errors.
- **DLQ + Error Table** for saving and subsequent debugging of data.
- **Retry** for temporary failures.

## 28. What are Slowly Changing Dimensions (SCD)?

**Slowly Changing Dimensions (SCD)** are dimensions in data warehouse (Data Warehouse) whose values  
**change over time**, but not frequently (e.g., customer surname, address, position).  
Such changes need to be stored in a special way to **preserve history** while maintaining current data.

### SCD Types

1. **SCD Type 0 — Static**
   - Data is not updated.
   - Example: customer birth date.

2. **SCD Type 1 — Overwrite**
   - Old value is replaced with new, history is not preserved.
   - Example: correcting a typo in name.
   - Transaction:
     ```
     UPDATE customers SET address = 'New York' WHERE customer_id = 123;
     ```

3. **SCD Type 2 — History Tracking**
   - New record is created when value changes.
   - Full history is preserved (often with `start_date`, `end_date`, `is_active` fields).
   - Example: customer moved → old record is closed, new one is added.

4. **SCD Type 3 — Limited History**
   - Store only **one previous value** in separate column.
   - Example: `current_address`, `previous_address`.

5. **SCD Type 4 — History Table**
   - Main table stores only current values.
   - Separate table contains full change history.

6. **SCD Type 6 — Hybrid (combination of Type 1 + Type 2 + Type 3)**
   - Used for flexibility.
   - Store both current data and history, and separate fields for previous value.

### Example (SCD Type 2)

`customer_dimension` table:

| customer_id | name     | city      | start_date | end_date   | is_active |
|-------------|----------|-----------|------------|------------|-----------|
| 123         | John Doe | New York  | 2018-01-01 | 2021-05-01 | 0         |
| 123         | John Doe | Chicago   | 2021-05-01 | NULL       | 1         |

👉 Here we can see customer history:  
first he lived in **New York**, then moved to **Chicago**.

### Why is this needed?

- Support for **historical analytics** (e.g., how many orders were made by customers living in New York in 2019).
- Correct analysis when data changes over time.
- Ability to build OLAP cubes considering temporal context.

## 29. What are Long Updating Dimensions (LUD)?

**Long Updating Dimensions (LUD)** are a type of dimensions in data warehouse,  
where **attributes change very rarely**, but the change takes  
**a long time** and can happen **gradually**.

### Difference from Slowly Changing Dimensions (SCD)
- **SCD (Slowly Changing Dimensions)** — captures changes in attribute values over time  
  (e.g., customer surname or address change).
- **LUD (Long Updating Dimensions)** — characterized by **long and gradual data updates**,  
  which can affect large volumes and last hours or even days.

### LUD Examples
1. **Product Reference (Product Dimension)**
   - New product line is added gradually (prices, characteristics, categories).
   - Complete attribute information may be entered only after several days.

2. **Organizational Structure**
   - Changes in departments, managers, and employees.
   - Updates take long due to approvals and migration stages.

### LUD Problems
- Not all analytical queries can work correctly during updates.
- Risk that part of data will be **incomplete** or **inconsistent**.
- Long system load during mass updates.

### Approaches to Working with LUD
1. **Staging area** — changes are first loaded into temporary area,  
   then atomically replace old data.

2. **Partition update** — updates are done in parts (e.g., by dates or regions).

3. **Versioning (like SCD Type 2)** — can store old and new record versions,  
   but close old one only after all attributes are updated.

### Summary:
**Long Updating Dimensions** are dimensions where changes occur rarely,  
but the updates themselves take a long time and happen gradually.

## 30. Practical task

### Build analytics for user events in mobile application

#### a. Context
- Company releases mobile application where users perform actions: opening app, purchases, clicks, page transitions, etc.
- Each action is an **event** in JSON format, sent to backend in real time.
- Product team wants to:
   1. See daily and weekly user activity.
   2. Track registration and payment funnel.
   3. Analyze behavior by user segments.

**Task:** design end-to-end data pipeline architecture.

#### b. Data Collection (Mobile App → Kafka)
1. Each action is sent to **Kafka** (JSON).
2. Kafka buffers events and guarantees delivery to storage.

#### c. Raw Layer (Raw Storage)
- All events go to **raw layer** (e.g., Snowflake, S3).
- Use batch or micro-batch ingestion for loading into staging.

1. **Batch ingestion**
   - Data accumulates in source (logs, tables).
   - Task (cron, Airflow, dbt Cloud) periodically extracts and saves to staging/raw.

2. **Micro-batch ingestion**
   - Automatic loading every N seconds/minutes.
   - Each "portion" contains new/changed data.
   - Example: export last minute of events from Kafka.

💡 **Note:**  
In the project, we used Kafka → raw (S3) every 60 seconds.  
Airflow launched Spark ingestion (micro-batch).  
dbt recalculated data marts every 5 minutes.  
Analytics delay ≤ 10 minutes without streaming.

#### d. Staging Layer (BigQuery / Snowflake)
1. Business entities are formed: `core_users`, `core_sessions`, `core_events`.
2. Joins and business transformations are done.
3. JSON is converted to relational tables (SQL parsing of JSON keys).
4. Data can be filtered, aggregated, and joined with other tables.

#### e. Marts Layer
- Final data marts: `user_activity_summary`, `registration_funnel`, `retention_metrics`.
- Aggregation by days, weeks, channels.

#### f. BI Visualization (Tableau / Looker)
- Connection to marts layer.
- Dashboards: activity, segmentation, funnels.

#### g. Orchestration (Airflow / Prefect)
- Pipeline management, schedules, dependencies.
- Ability for **retries** and error logging.

#### h. Data Quality Control
1. dbt tests: `not_null`, `unique`, `accepted_values`.
2. Alerts when event volume drops or tables are empty.
3. Schema checking (schema drift detection).
   - Comparing incoming data with expected schema.
4. SLA monitoring: if `sales_summary` mart wasn't updated for 30 min → alert to Slack.

#### i. Implementation Questions

**1. Why is data separated into raw / staging / core / marts?**
- ✅ Best practice.
- Traceability: clear where data came from.
- Security: raw — only storage.
- Flexibility: staging handles semi-structured data.
- Reproducibility: any layer can be recalculated.

**2. How is data quality control implemented?**
- Tests `not_null`, `unique`, `relationships` in dbt.
- Logging row counts, empty values.
- Schema checking (`schema drift`).
- Alerts to Slack/email when metrics drop.

**3. What to do if event volume ↑ 10x?**
- Horizontal scaling of Kafka and DWH.
- dbt runs with larger number of `threads`.
- DAG parallelization (independent branches).

**4. What does "incremental model" mean?**
- On first run creates table.
- On subsequent runs — processes only new/changed rows.

Example (dbt):

```sql
{{ config(materialized='incremental', unique_key='order_id') }}
SELECT *
FROM staging.orders
WHERE updated_at > (SELECT MAX(updated_at) FROM {{ this }})
```
**5. What does "partitioned model" mean?**
- Data is split into partitions (e.g., by order_date).
- Fast selection and rewriting of only needed fragment.

**6. Why not use one big table?**
- Impossible to control quality at stages.
- Difficult to debug.
- No model reuse.
- Layers provide flexibility, transparency, and reproducibility.

**7. How is idempotency ensured on repeated runs?**
- In staging/core/marts:
- DELETE + INSERT by date or MERGE by key.
- is_incremental() in dbt.
- audit_log with information about last successful load.