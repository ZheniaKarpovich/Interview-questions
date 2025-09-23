## [Main title](../README.md)
# DB/SQL

## Оглавление

- [1. Kimball vs Inmon — what they are, how to model, and when to choose](#1-kimball-vs-inmon--what-they-are-how-to-model-and-when-to-choose)
- [2. Difference between SQL and NoSQL databases](#2-difference-between-sql-and-nosql-databases)
- [3. What operations can be performed with SQL, what are they used for, and their execution order?](#3-what-operations-can-be-performed-with-sql-what-are-they-used-for-and-their-execution-order)
- [4. What types of JOINs exist and what are their differences?](#4-what-types-of-joins-exist-and-what-are-their-differences)
- [5. What are join algorithms in SQL and what types exist?](#5-what-are-join-algorithms-in-sql-and-what-types-exist)
- [6. What is Hash Join?](#6-what-is-hash-join)
- [7. What is a Primary Key in a database?](#7-what-is-a-primary-key-in-a-database)
- [8. What is a Foreign Key and how is it used?](#8-what-is-a-foreign-key-and-how-is-it-used)
- [9. What is the difference between PRIMARY KEY and UNIQUE key?](#9-what-is-the-difference-between-primary-key-and-unique-key)
- [10. What types of relationships exist between tables and how are they implemented?](#10-what-types-of-relationships-exist-between-tables-and-how-are-they-implemented)
- [11. What is normalization? Explain with examples](#11-what-is-normalization-explain-with-examples)
- [12. What is denormalization and when is it used?](#12-what-is-denormalization-and-when-is-it-used)
- [13. What are indexes and how do they improve query performance?](#13-what-are-indexes-and-how-do-they-improve-query-performance)
- [14. What is PL/SQL?](#14-what-is-plsql)
- [15. What is the difference between procedural and declarative approach in SQL?](#15-what-is-the-difference-between-procedural-and-declarative-approach-in-sql)
- [16. Explain ACID properties in the context of databases](#16-explain-acid-properties-in-the-context-of-databases)
- [17. What is transaction isolation level? What types exist and what problems do they solve?](#17-what-is-transaction-isolation-level-what-types-exist-and-what-problems-do-they-solve)
- [18. Explain the logical architecture of PostgreSQL](#18-explain-the-logical-architecture-of-postgresql)
- [19. Describe the physical structure of PostgreSQL](#19-describe-the-physical-structure-of-postgresql)
- [20. Explain MVCC in PostgreSQL](#20-explain-mvcc-in-postgresql)
- [21. What is scaling using PostgreSQL as an example?](#21-what-is-scaling-using-postgresql-as-an-example)
- [22. What is sharding?](#22-what-is-sharding)
- [23. What is replication?](#23-what-is-replication)
- [24. What is partitioning in SQL?](#24-what-is-partitioning-in-sql)
- [25. What is a trigger?](#25-what-is-a-trigger)
- [26. What is the difference between a trigger and a procedure/function?](#26-what-is-the-difference-between-a-trigger-and-a-procedurefunction)
- [27. What are views and what are their advantages? How can they be used in the context of OLAP systems?](#27-what-are-views-and-what-are-their-advantages-how-can-they-be-used-in-the-context-of-olap-systems)
- [28. What is lock escalation?](#28-what-is-lock-escalation)
- [29. What are constraints in SQL?](#29-what-are-constraints-in-sql)
- [30. What types of locks exist?](#30-what-types-of-locks-exist)
- [31. What is a deadlock?](#31-what-is-a-deadlock)
- [32. What are roles?](#32-what-are-roles)
- [33. What are window functions in SQL and why are they needed?](#33-what-are-window-functions-in-sql-and-why-are-they-needed)
- [34. How do aggregate functions differ from window functions?](#34-how-do-aggregate-functions-differ-from-window-functions)
- [35. What are PARTITION BY and ORDER BY in window functions?](#35-what-are-partition-by-and-order-by-in-window-functions)
- [36. How do ROW_NUMBER(), RANK() and DENSE_RANK() differ?](#36-how-do-row_number-rank-and-dense_rank-differ)
- [37. What are LAG() and LEAD() used for?](#37-what-are-lag-and-lead-used-for)
- [39. Can window functions be used in WHERE?](#39-can-window-functions-be-used-in-where)
- [40. What is QUALIFY?](#40-what-is-qualify)
- [41. SQL query optimization: EXPLAIN and EXPLAIN ANALYZE](#41-sql-query-optimization-explain-and-explain-analyze)
- [42. What is the BASE approach in databases?](#42-what-is-the-base-approach-in-databases)
- [43. What is the difference between BASE and ACID approaches?](#43-what-is-the-difference-between-base-and-acid-approaches)
- [44. You have a log table with billions of rows. How will you speed up searches by date and `user_id`?](#44-you-have-a-log-table-with-billions-of-rows-how-will-you-speed-up-searches-by-date-and-user_id)
---
- [45. What are JOINs in T-SQL and can you explain the different types?](#45-what-are-joins-in-t-sql-and-can-you-explain-the-different-types)
- [46. Describe recursive CTEs and provide an example of when they might be used.](#46-describe-recursive-ctes-and-provide-an-example-of-when-they-might-be-used)
- [47. Explain how to use the PIVOT and UNPIVOT operators.](#47-explain-how-to-use-the-pivot-and-unpivot-operators)
- [48. What are the scalar and table-valued functions in T-SQL?](#48-what-are-the-scalar-and-table-valued-functions-in-t-sql)
- [49. What best practices should be followed when writing T-SQL code?](#49-what-best-practices-should-be-followed-when-writing-t-sql-code)
- [50. What are the **scalar** and **table-valued functions** in T-SQL?](#50-what-are-the-scalar-and-table-valued-functions-in-t-sql)
- [51. How is a **stored procedure** different from a **function** in T-SQL?](#51-how-is-a-stored-procedure-different-from-a-function-in-t-sql)
- [52. Can you write a simple **stored procedure** with **input** and **output parameters**?](#52-can-you-write-a-simple-stored-procedure-with-input-and-output-parameters)
- [53. Explain how to handle **errors** in **stored procedures**.](#53-explain-how-to-handle-errors-in-stored-procedures)
- [54. How do you use the **EXECUTE statement** in T-SQL?](#54-how-do-you-use-the-execute-statement-in-t-sql)
- [55. What is the significance of the **RETURN statement** in **stored procedures**?](#55-what-is-the-significance-of-the-return-statement-in-stored-procedures)
- [56. Describe the use of **table variables** and **temporary tables** in **stored procedures**.](#56-describe-the-use-of-table-variables-and-temporary-tables-in-stored-procedures)
- [57. Discuss the use of variables in T-SQL.](#57-discuss-the-use-of-variables-in-t-sql)
- [58. How do you use control-of-flow language (IF...ELSE, WHILE) in T-SQL scripts?](#58-how-do-you-use-control-of-flow-language-ifelse-while-in-t-sql-scripts)
- [59. Can you provide an example of a T-SQL CASE statement?](#59-can-you-provide-an-example-of-a-t-sql-case-statement)
- [60. Explain the TRY...CATCH construct in T-SQL error handling.](#60-explain-the-trycatch-construct-in-t-sql-error-handling)
- [61. What are the implications of using CURSORs in T-SQL?](#61-what-are-the-implications-of-using-cursors-in-t-sql)
- [62. What is a transaction in the context of T-SQL?](#62-what-is-a-transaction-in-the-context-of-t-sql)
- [63. How do you use the BEGIN TRANSACTION, COMMIT, and ROLLBACK statements?](#63-how-do-you-use-the-begin-transaction-commit-and-rollback-statements)
- [64. What does it mean to set a transaction isolation level in T-SQL?](#64-what-does-it-mean-to-set-a-transaction-isolation-level-in-t-sql)
- [65. Discuss the potential risks of transaction deadlocks.](#65-discuss-the-potential-risks-of-transaction-deadlocks)
- [66. What is an index in SQL Server and how is it implemented in T-SQL?](#66-what-is-an-index-in-sql-server-and-how-is-it-implemented-in-t-sql)
- [67. What are clustered and non-clustered indexes?](#67-what-are-clustered-and-non-clustered-indexes)
- [68. How can indexing impact the performance of T-SQL queries?](#68-how-can-indexing-impact-the-performance-of-t-sql-queries)
- [69. Discuss the process and reason for index maintenance.](#69-discuss-the-process-and-reason-for-index-maintenance)
- [70. What are included columns in an index and when would you use them?](#70-what-are-included-columns-in-an-index-and-when-would-you-use-them)
- [71. How do you manage **permissions** using T-SQL?](#71-how-do-you-manage-permissions-using-t-sql)
- [72. What are **roles** in SQL Server and how are they used in T-SQL?](#72-what-are-roles-in-sql-server-and-how-are-they-used-in-t-sql)
- [73. Explain the use of T-SQL statements for managing **login accounts**.](#73-explain-the-use-of-t-sql-statements-for-managing-login-accounts)
- [74. How can you secure data against **SQL injection** attacks in T-SQL?](#74-how-can-you-secure-data-against-sql-injection-attacks-in-t-sql)
- [75. What **best practices** should be followed when writing T-SQL code?](#75-what-best-practices-should-be-followed-when-writing-t-sql-code)
- [76. How do you write T-SQL code for **scalability** and **maintainability**?](#76-how-do-you-write-t-sql-code-for-scalability-and-maintainability)
- [77. Discuss **naming conventions** and their importance in T-SQL.](#77-discuss-naming-conventions-and-their-importance-in-t-sql)
- [78. How do you ensure that your T-SQL code is **readable**?](#78-how-do-you-ensure-that-your-t-sql-code-is-readable)
- [79. What are **dynamic SQL queries** and how do you execute them in T-SQL?](#79-what-are-dynamic-sql-queries-and-how-do-you-execute-them-in-t-sql)
- [80. How is **XML data** handled in T-SQL?](#80-how-is-xml-data-handled-in-t-sql)
- [81. What is the difference between SQL Server **temporary tables** and **table variables**?](#81-what-is-the-difference-between-sql-server-temporary-tables-and-table-variables)
- [82. How do you work with **hierarchies** and **recursive relationships** in T-SQL?](#82-how-do-you-work-with-hierarchies-and-recursive-relationships-in-t-sql)
- [83. Explain the use of **spatial data types** in T-SQL.](#83-explain-the-use-of-spatial-data-types-in-t-sql)
- [84. What steps would you take to **troubleshoot** and optimize a slow-running T-SQL query?](#84-what-steps-would-you-take-to-troubleshoot-and-optimize-a-slow-running-t-sql-query)
- [85. Explain the use of **SQL Server Profiler** and **Execution Plan** for performance tuning.](#85-explain-the-use-of-sql-server-profiler-and-execution-plan-for-performance-tuning)
- [86. How do you identify and handle **SQL Server blocking queries**?](#86-how-do-you-identify-and-handle-sql-server-blocking-queries)
- [87. How does T-SQL support **data warehousing** operations?](#87-how-does-t-sql-support-data-warehousing-operations)
- [88. Describe the use of **partitioning** in T-SQL and SQL Server.](#88-describe-the-use-of-partitioning-in-t-sql-and-sql-server)
- [89. Explain the **ETL (Extract, Transform, Load)** process in relation to T-SQL scripting.](#89-explain-the-etl-extract-transform-load-process-in-relation-to-t-sql-scripting)
- [90. How do you enforce **business logic** within T-SQL scripts?](#90-how-do-you-enforce-business-logic-within-t-sql-scripts)
- [91. Describe the use of **constraints** and **triggers** in enforcing **integrity**.](#91-describe-the-use-of-constraints-and-triggers-in-enforcing-integrity)
- [92. What are some ways T-SQL can be used for **data validation**?](#92-what-are-some-ways-t-sql-can-be-used-for-data-validation)
- [93. How does T-SQL work with **SQL Server Reporting Services (SSRS)**?](#93-how-does-t-sql-work-with-sql-server-reporting-services-ssrs)
- [94. Discuss the interaction between T-SQL and **SQL Server Integration Services (SSIS)**.](#94-discuss-the-interaction-between-t-sql-and-sql-server-integration-services-ssis)
- [95. Explain how T-SQL scripts can be used within **SQL Server Agent jobs**.](#95-explain-how-t-sql-scripts-can-be-used-within-sql-server-agent-jobs)
- [96. How can T-SQL be used for data aggregation and summary?](#96-how-can-t-sql-be-used-for-data-aggregation-and-summary)
- [97. Discuss the capabilities of T-SQL for trend analysis.](#97-discuss-the-capabilities-of-t-sql-for-trend-analysis)
- [98. How is T-SQL used to prepare data for business intelligence and analytics?](#98-how-is-t-sql-used-to-prepare-data-for-business-intelligence-and-analytics)
- [99. What are some of the new T-SQL features in the latest version of SQL Server?](#99-what-are-some-of-the-new-t-sql-features-in-the-latest-version-of-sql-server)
- [100. Discuss how T-SQL has evolved to work with big data and in-memory technologies.](#100-discuss-how-t-sql-has-evolved-to-work-with-big-data-and-in-memory-technologies)
- [101. How does T-SQL support cloud scenarios with Azure SQL Database?](#101-how-does-t-sql-support-cloud-scenarios-with-azure-sql-database)
- [102. Explain strategies for handling large-volume data updates and deletes in T-SQL.](#102-explain-strategies-for-handling-large-volume-data-updates-and-deletes-in-t-sql)
- [103. How do you use T-SQL to handle duplicate record scenarios?](#103-how-do-you-use-t-sql-to-handle-duplicate-record-scenarios)
- [104. What is a T-SQL Merge statement and how is it used?](#104-what-is-a-t-sql-merge-statement-and-how-is-it-used)
- [105. What tools and techniques are available for testing T-SQL code?](#105-what-tools-and-techniques-are-available-for-testing-t-sql-code)
- [106. How can you debug a stored procedure in SQL Server Management Studio (SSMS)?](#106-how-can-you-debug-a-stored-procedure-in-sql-server-management-studio-ssms)
- [107. Discuss how assertions and checkpoints can be used in T-SQL scripts.](#107-discuss-how-assertions-and-checkpoints-can-be-used-in-t-sql-scripts)
- [108. Discuss how to work with different date and time data types in T-SQL.](#108-discuss-how-to-work-with-different-date-and-time-data-types-in-t-sql)
- [109. How do you handle time zones in T-SQL?](#109-how-do-you-handle-time-zones-in-t-sql)
- [110. Provide examples of common date and time-related functions in T-SQL.](#110-provide-examples-of-common-date-and-time-related-functions-in-t-sql)
- [111. How does T-SQL accommodate working with JSON data?](#111-how-does-t-sql-accommodate-working-with-json-data)
- [112. What support does T-SQL offer for working with binary and large objects (BLOBs)?](#112-what-support-does-t-sql-offer-for-working-with-binary-and-large-objects-blobs)
- [113. Discuss the use of UDT (User-Defined Types) in T-SQL.](#113-discuss-the-use-of-udt-user-defined-types-in-t-sql)
- [114. How do you manage T-SQL script deployments across different environments?](#114-how-do-you-manage-t-sql-script-deployments-across-different-environments)
- [115. Discuss version control practices for T-SQL scripts.](#115-discuss-version-control-practices-for-t-sql-scripts)
- [116. How do you automate common database administration tasks with T-SQL?](#116-how-do-you-automate-common-database-administration-tasks-with-t-sql)
- [117. Discuss the T-SQL scripts for backing up and restoring SQL Server databases.](#117-discuss-the-t-sql-scripts-for-backing-up-and-restoring-sql-server-databases)
- [118. Explain how to monitor SQL Server health with T-SQL scripts.](#118-explain-how-to-monitor-sql-server-health-with-t-sql-scripts)
- [119. Discuss the use of **output parameters** in stored procedures.](#119-discuss-the-use-of-output-parameters-in-stored-procedures)
- [120. How do **dynamic stored procedures** work?](#120-how-do-dynamic-stored-procedures-work)
- [121. How can you manage **transaction scope** within a stored procedure?](#121-how-can-you-manage-transaction-scope-within-a-stored-procedure)
- [122. How do you **document** your T-SQL code for team collaboration?](#122-how-do-you-document-your-t-sql-code-for-team-collaboration)
- [123. Discuss the role of **code reviews** in the T-SQL development process.](#123-discuss-the-role-of-code-reviews-in-the-t-sql-development-process)
- [124. How would you design a **T-SQL solution** for a banking transaction system?](#124-how-would-you-design-a-t-sql-solution-for-a-banking-transaction-system)
- [125. Provide a **T-SQL solution** for reporting top N customers by sales.](#125-provide-a-t-sql-solution-for-reporting-top-n-customers-by-sales)
- [126. Discuss how you would use **T-SQL** to identify and resolve **data integrity issues**.](#126-discuss-how-you-would-use-t-sql-to-identify-and-resolve-data-integrity-issues)


## 1. Kimball vs Inmon — what they are, how to model, and when to choose

### 1) The Problem Both Approaches Solve
We need to integrate data from multiple sources (CRM, billing, website, payments), bring it to a consistent format, and provide the business with fast and clear reporting.
**The difference** is not whether there is a database, but how it is modeled and in what sequence we move towards analytical data marts.

### 2) Kimball Approach (Dimensional Modeling)

### Idea
We build **data marts** directly for analytics, in the form of **star schemas**:  
- A **fact table** in the center with numeric metrics and foreign keys to dimensions.  
- **Dimensions** are “context” tables: Customer, Product, Time, Geography, etc.  
- BI queries are straightforward: facts + filters/slices by dimensions.

### Key Concepts
- **Grain** — the lowest level of detail in a fact (e.g., “one payment attempt”).  
- **Fact types**:
  - Transactional (events, transactions)
  - Periodic Snapshot (snapshots at regular intervals, e.g., daily balances)
  - Accumulating Snapshot (processes with milestones: order → payment → shipment)  
- **Conformed dimensions** — shared dimensions across marts (one `dim_customer` used in both sales and marketing).  
- **SCD (Slowly Changing Dimensions)**:
  - Type 1 — overwrite (no history)
  - Type 2 — keep history with effective dates
  - Type 3 — limited history, e.g., “previous value” in a separate column  
- **Star vs Snowflake**:
  - **Star** — denormalized dimensions, fewer joins.
  - **Snowflake** — partially normalized, less duplication, more joins.

### Pros / Cons
**Pros**:  
- Fast time-to-value (reports available quickly).  
- Simple and intuitive for BI/analysts.  
- Efficient for analytical queries.

**Cons**:  
- Possible duplication in dimensions.  
- Harder to guarantee a single version of truth across a very large enterprise.

### 3) Inmon Approach (Enterprise Data Warehouse in 3NF)

### Idea
First we build a **centralized EDW** in **normalized form (3NF)**. This is the “big, correct warehouse”:  
- Entities and relationships are modeled cleanly (minimal redundancy).  
- Reference data is unified across the company.  
- On top of EDW, we later build **data marts** (often dimensional, Kimball-style).

### Multi-layered Architecture
- **Staging**: raw ingested data with minimal logic.  
- **EDW (3NF)**: normalized entities, the single version of truth.  
- **Data Marts (Dimensional)**: denormalized marts for BI.


### Pros / Cons
**Pros**:  
- Strong integration and consistency across domains.  
- Easy to extend with new domains without duplicating reference data.  
- Good foundation for MDM, governance, and compliance.  

**Cons**:  
- Slower time-to-value (longer until reports appear).  
- Queries against EDW are complex (lots of joins). BI usually consumes data marts, not EDW directly.

### 4) Comparison in Simple Terms

| Criterium | Kimball (marts directly) | Inmon (EDW 3NF → marts) |
|---|---|---|
| Goal | Fast and clear reports, BI-friendly | Single version of truth across the enterprise |
| Starting point | Directly build star/snowflake | Centralized normalized EDW first |
| Structure | Denormalized dimensions, fact in the center | Fully normalized (3NF) |
| Speed | Faster time-to-value | Slower, more planning |
| Redundancy | Possible in dimensions | Minimized |
| Query complexity | Simple for BI | Complex in EDW, simple in marts |
| Best fit | Small/medium teams, BI focus | Large enterprises, many domains, governance |

### 5) Physical Implementation

- **One system**: EDW and marts can live in the same DB, in different schemas (e.g., Snowflake/Postgres/BigQuery).  
  - `edw_*` — normalized EDW tables  
  - `dm_*` or `mart_*` — dimensional marts  
- **Different systems**: EDW stored in heavy-duty DB (Snowflake, Teradata, Oracle), marts in separate BI-optimized DB (ClickHouse, BigQuery).  

Choice depends on scale, performance needs, governance.

### 6) Common Interview Questions

**Q: What is a conformed dimension?**  
**A:** A dimension that is used consistently across multiple marts/domains (e.g., one `dim_customer` shared between sales and payments). It enables cross-domain reporting.  

**Q: How do you choose the grain of a fact?**  
**A:** Define the business question first, then fix the lowest level of detail (e.g., one payment attempt). All fields must align with that grain.  

**Q: SCD — when Type 1 vs 2 vs 3?**  
**A:**  
- Type 1 — overwrite, no history (e.g., correcting a typo).  
- Type 2 — full history with effective dates (e.g., customer moved country).  
- Type 3 — limited history (rare, e.g., storing previous value in one column).  

**Q: Semi-additive metrics (balances)?**  
**A:** In periodic snapshots: you cannot sum balances over time. Either use end-of-period balances or compute deltas.  

**Q: Multi-currency design?**  
**A:** Fact stores transaction currency + exchange rate reference. Reporting currency conversions handled via a bridge.  

**Q: Late-arriving facts or early-arriving dimensions?**  
**A:**  
- Fact before dimension → insert inferred member in dimension, update later.  
- Dimension updated retroactively → SCD2 with valid date ranges.  

**Q: When to choose Kimball vs Inmon?**  
**A:**  
- Kimball: fast insights, simpler orgs, focus on BI.  
- Inmon: large enterprises, many domains, strong governance needs.  
- Hybrid: EDW core + dimensional marts is very common.


## 2. Main components of SQL

- **DDL (Data Definition Language)** — defines and modifies the structure of the database.
- **DML (Data Manipulation Language)** — manipulates the data, responsible for adding, changing, and deleting records.
- **DCL (Data Control Language)** — manages access rights and permissions in the database.
- **TCL (Transaction Control Language)** — controls transactions (e.g., committing changes with `COMMIT` or rolling them back with `ROLLBACK`).

## 2. Difference between SQL and NoSQL Databases

SQL and NoSQL databases offer different paradigms, each designed for certain types of data and ways of processing them.

### Key Differences

- **SQL**: Primarily intended for structured (and partly semi-structured) data that follows a predefined schema.
- **NoSQL**: Suitable for unstructured or semi-structured data that evolves over time, providing schema flexibility.

- **SQL**: Uses the SQL (Structured Query Language) for modifying and retrieving data.
- **NoSQL**: Provides various APIs (e.g., for document-oriented and key-value stores); support for structured query languages depends on the specific implementation.

- **SQL**: Typically guarantees compliance with **ACID** principles (Atomicity, Consistency, Isolation, Durability), ensuring data integrity.
- **NoSQL**: Often optimized for high performance and horizontal scalability, sometimes at the expense of strict consistency.

### Main Types of NoSQL Databases

- **Document-Oriented Stores**
  - Examples: MongoDB, Couchbase
  - Features: each record is a standalone document (usually JSON). Relationships are handled via embedded documents or references.
  - Example: a user and their blog posts may be stored in one document or linked through references.

- **Key-Value Stores**
  - Examples: Redis, Amazon DynamoDB
  - Features: data is stored as unique keys and values. No mandatory schema.
  - Example: shopping cart items linked to a user ID.

- **Wide-Column Stores (Column Families)**
  - Examples: Apache Cassandra, HBase
  - Features: data is grouped into column families, similar to tables, but with a flexible schema.
  - Example: user profiles where some users may have unique attributes.

- **Graph Databases**
  - Examples: Neo4j, JanusGraph
  - Features: optimized for data with complex relationships. Entities are represented as nodes and relationships as edges.
  - Example: social networks with friendship relationships.

### Differences in Data Modeling

- **SQL**: normalization to minimize redundancy and update anomalies.
- **NoSQL**: denormalization — packing related data together to reduce the number of queries.

### ID Generation

- **SQL**: often uses auto-incrementing unique IDs.
- **NoSQL**: unique identifiers can be generated by external systems or at the document level.

### Working with Relationships

- **SQL**: relationships are implemented through keys (primary and foreign).
- **NoSQL**: relationships may be implemented via embedded documents, references, or graph structures.

### Transaction Support

- **SQL**: transactions are a standard feature.
- **NoSQL**: transaction support depends on the implementation.

### Consistency Levels

- **SQL**: ensures strict consistency.
- **NoSQL**: offers different models — from strict consistency to **eventual consistency**.

### Scalability

- **SQL**: vertical (increasing the power of a single server).
- **NoSQL**: horizontal (distributing load across multiple servers).

### Data Flexibility

- **SQL**: rigid schema, harder to adapt to changing data structures.
- **NoSQL**: supports dynamic schema updates, providing high flexibility.

### Data Integrity and Validation

- **SQL**: integrity is ensured by constraints and strict data types.
- **NoSQL**: relies more on the application to handle integrity and validation.

## 3. What operations can be performed with SQL, what are they used for, and their execution order?

In SQL, commands are divided into several categories.

### DDL (Data Definition Language)
Used to define and modify the structure of database objects.

- **CREATE** — create objects (tables, views, schemas, etc.)
- **ALTER** — modify the structure of objects
- **DROP** — delete objects
- **TRUNCATE** — quickly remove all rows from a table (cannot be rolled back)
- **RENAME** — rename an object

> ⚡ Feature: DDL commands are auto-committed and cannot be rolled back.

### DML (Data Manipulation Language)
Used to work with data in tables.

- **SELECT** — read data
- **INSERT** — add rows
- **UPDATE** — modify rows
- **DELETE** — remove rows

> ⚡ Feature: DML commands support transactions and can be rolled back with **ROLLBACK**.

#### Main operations in a SELECT query
- **SELECT** — defines the columns or expressions to output
- **FROM** — specifies the data sources (tables, views)
- **JOIN** — combines data from different tables
- **WHERE** — filters rows before grouping
- **GROUP BY** — groups rows into aggregates
- **HAVING** — filters grouped results
- **ORDER BY** — sorts the output
- **LIMIT** — limits the number of returned rows

#### Logical execution order of a query
Although we write SQL in one order, it is executed as:  
`FROM → JOIN → WHERE → GROUP BY → HAVING → SELECT → ORDER BY → LIMIT`

> This explains, for example, why aliases from `SELECT` cannot be used in `WHERE`.

### DCL (Data Control Language)
Used to manage user permissions.

- **GRANT** — grant privileges
- **REVOKE** — revoke privileges

### TCL (Transaction Control Language)
Used to control transactions.

- **COMMIT** — persist changes
- **ROLLBACK** — undo changes
- **SAVEPOINT** — set a savepoint inside a transaction

📌 **Quick summary:**
- **DDL** manages the structure of the database (CREATE/ALTER/DROP).
- **DML** manages the data (SELECT/INSERT/UPDATE/DELETE).
- **DCL** manages access (GRANT/REVOKE).
- **TCL** manages transactions (COMMIT/ROLLBACK/SAVEPOINT).
- The logical execution order of `SELECT` differs from its written order, and this is important for understanding queries correctly.

## 4. What types of JOINs exist and what are their differences?

**JOIN** in SQL is used to combine rows from two or more tables based on a related column (usually a key).  
Understanding the types of joins is important for working correctly with normalized data and avoiding accidental Cartesian products.

### Main types of JOIN

- **INNER JOIN** — returns only matching rows in both tables.
- **LEFT JOIN** — returns all rows from the left table and matches from the right; if no matches, `NULL` is used.
- **RIGHT JOIN** — opposite of LEFT JOIN: returns all rows from the right table and matches from the left.
- **FULL JOIN (FULL OUTER JOIN)** — returns all rows from both tables, filling with `NULL` where there are no matches.
- **CROSS JOIN** — forms a Cartesian product (each row from the first table is combined with all rows from the second).
- **SELF JOIN** — a table joined with itself (often used for hierarchies, e.g., “employee → manager”).

### Visualization of JOIN types

![SQL JOINS](../images/sql-joins.png)

## 5. What are join algorithms in SQL and what types exist?

When we write a `JOIN`, the DBMS must choose a **join algorithm**.  
The main options are:

### Nested Loop Join
- The simplest algorithm: for each row from the first table, the DBMS searches for a match in the second.
- Works like a “double loop”.

**When effective:**
- Small tables.
- An index exists on the join key (the inner table can be searched quickly).

**Cons:**
- Very slow on large data sets (O(N*M)).

### Merge Join
- Both tables are sorted by the join key.
- The DBMS then “walks” through both lists, comparing rows.

**When effective:**
- Tables are already sorted (e.g., by an index).
- Suitable for large volumes of data.

**Cons:**
- Requires sorting if data is unordered.

### Hash Join
- A **hash table** is built for the smaller table on the join key.
- Rows from the larger table are checked against this hash.

**When effective:**
- Large tables without indexes.
- Join condition is equality (`=`).

**Cons:**
- Requires memory for the hash table.
- For very large tables, may spill to disk (Hash Join spill).

### Adaptive Join (SQL Server, Oracle)
- The DBMS starts execution and then **switches the algorithm on the fly** if the cardinality estimate (number of rows) turns out to be wrong.

### Comparison of algorithms

| Algorithm      | Best for                  | Complexity | Requirements            |
|----------------|---------------------------|------------|-------------------------|
| Nested Loop    | Small tables, with index  | O(N*M)     | Index for efficiency    |
| Merge Join     | Sorted data               | O(N+M)     | Sorting required        |
| Hash Join      | Large unsorted tables     | O(N+M)     | Memory for hash table   |
| Adaptive Join  | Dynamic optimization      | Depends    | Engine support required |


### Summary
- **Nested Loop** — simple, but slow without an index.
- **Merge Join** — good when data is sorted.
- **Hash Join** — the most common choice for large tables.
- **Adaptive Join** — “smart” option if the DBMS supports it.

## 6. What is Hash Join?

**Hash Join** is a table join algorithm in SQL that uses a **hash table** to speed up finding matches between rows.  
It is used when we perform `JOIN` of two tables on equality condition (`=`).

### How Hash Join works
The algorithm usually consists of two phases:

1. **Build (construction)**
- The DBMS selects the smaller table (or subquery result).
- Creates a **hash table** in memory by the join key.
- In this hash table, rows are stored as "key → row" pairs.

2. **Probe (search)**
- The DBMS sequentially reads rows from the second (larger) table.
- For each row, it calculates a hash by the join key.
- Checks if there is a match in the hash table.
- If there is — rows are joined and included in the result.

### Example
```sql
  SELECT *
  FROM orders o
  JOIN customers c
    ON o.customer_id = c.customer_id;
```
- PostgreSQL may choose **Hash Join** if:
  - `customers` is a small table → a hash table is built for it.
  - `orders` is a large table → for each row, a match is searched by `customer_id` in the hash.

### When Hash Join is effective
- Join by **equality** condition (`=`).
- Tables are large and have **no suitable index**.
- One table is significantly smaller than the other.

### Comparison with other algorithms
| Algorithm     | How it works                                  | When better |
|--------------|-----------------------------------------------|-------------|
| **Nested Loop** | For each row of the left table, searches in the right | Good for small tables or when index exists |
| **Merge Join**  | Sorts both tables and traverses them sequentially | Efficient with sorted data |
| **Hash Join**   | Builds a hash table and searches for matches through hash | Optimal for large tables without indexes |

### Summary
- **Hash Join** is a join through a hash table.
- Usually faster than Nested Loop if there is a lot of data and no indexes.
- But requires additional memory to store the hash table.

## 7. What is a Primary Key in a database?

**Primary Key** is a column or set of columns that uniquely identify each row in a database table.

### Key properties:
- Values **must be unique**.
- Values **cannot be NULL**.
- A table can have **only one primary key**, but it can consist of multiple columns (**composite key**).
- Most DBMSs automatically create an index for the primary key to speed up searches.

### Example

```sql
CREATE TABLE employees (
    employee_id INT PRIMARY KEY,
    name VARCHAR(100)
);
```
### What does this code do?

1. Creates the `employees` table.
2. Defines the `employee_id` column as **PRIMARY KEY**.
3. Ensures uniqueness of each `employee_id` value.
4. Prohibits using `NULL` for `employee_id`.
5. Allows using `employee_id` as a reference (foreign key) in other tables.
6. Automatically creates an index on `employee_id` to speed up searches.

## 8. What is a Foreign Key and how is it used?

**Foreign Key (FOREIGN KEY)** is a column or set of columns in one table that references a primary key in another table.

### Key properties:
- A foreign key always points to a **PRIMARY KEY** or **UNIQUE key** in the parent table.
- Its purpose is to ensure **referential integrity**, that is, consistency of relationships between tables.
- It prevents insertion of values that don't exist in the parent table.
- Can define actions when changing or deleting a row in the parent table:
  - `ON DELETE CASCADE` — deletes related records.
  - `ON DELETE SET NULL` — sets `NULL` value in the foreign key.
  - `RESTRICT` — prohibits deletion/update if there are related records.

### Example

```sql
  CREATE TABLE employees (
    employee_id INT PRIMARY KEY,
    department_id INT,
    FOREIGN KEY (department_id) REFERENCES departments(department_id)
  );
```

## 9. What is the difference between PRIMARY KEY and UNIQUE key?

### PRIMARY KEY
- Uniquely identifies each row in a table.
- Can consist of **one or multiple columns** (composite key).
- A table can have **only one PRIMARY KEY**.
- Values **cannot be NULL**.
- Automatically creates an index when created.

### UNIQUE key
- Guarantees that values in a column (or set of columns) will be unique.
- A table can have **multiple UNIQUE keys**.
- Unlike PRIMARY KEY, allows **one NULL value** (behavior may differ in different DBMSs).
- Also creates a unique index, but doesn't necessarily serve as the "main identifier" of the table.

### Summary
- `PRIMARY KEY` — main row identifier, required for table and always **NOT NULL + UNIQUE**.
- `UNIQUE` — additional constraint to prevent duplicates in other columns, but allows `NULL` (in most DBMSs).

### Example

```sql
  CREATE TABLE users (
      user_id INT PRIMARY KEY,
      email VARCHAR(255) UNIQUE,
      phone VARCHAR(20) UNIQUE
  );
```
## 10. What types of relationships exist between tables and how are they implemented?

In relational databases, there are three main types of relationships between tables: **1:1 (one-to-one)**, **1:M (one-to-many)**, and **M:N (many-to-many)**.

### 1:1 Relationship (One-to-One)

**Definition:** each record in the first table corresponds to no more than one record in the second, and vice versa.

**Example:**
- `users` table — basic user information.
- `user_profiles` table — additional information (e.g., passport data).

**Implementation:**  
In the second table (`user_profiles`), a foreign key (`FOREIGN KEY`) is created, which is also a `PRIMARY KEY`.  
This guarantees that each user will have no more than one profile.

```sql
CREATE TABLE users (  
  user_id INT PRIMARY KEY,  
  name VARCHAR(100)  
);

CREATE TABLE user_profiles (  
  user_id INT PRIMARY KEY,  
  address VARCHAR(200),  
  FOREIGN KEY (user_id) REFERENCES users(user_id)  
);
```

### 1:M Relationship (One-to-Many)

**Definition:** each record in the first table can correspond to many records in the second, but each record in the second table relates to only one record in the first.

**Example:**
- `departments` table — list of departments.
- `employees` table — list of employees.

**Implementation:**  
In the second table (`employees`), a foreign key is created that references the primary key of the first table (`departments`).

```sql
CREATE TABLE departments (  
  department_id INT PRIMARY KEY,  
  department_name VARCHAR(100)  
);

CREATE TABLE employees (  
  employee_id INT PRIMARY KEY,  
  name VARCHAR(100),  
  department_id INT,  
  FOREIGN KEY (department_id) REFERENCES departments(department_id)  
);
```

### M:N Relationship (Many-to-Many)

**Definition:** each record in the first table can correspond to many records in the second, and vice versa.

**Example:**
- `students` table — students.
- `courses` table — courses.
- One student can attend many courses, and one course can be attended by many students.

**Implementation:**  
A third table (intermediate or junction) is created that stores key pairs.

```sql
CREATE TABLE students (  
  student_id INT PRIMARY KEY,  
  name VARCHAR(100)  
);

CREATE TABLE courses (  
  course_id INT PRIMARY KEY,  
  course_name VARCHAR(100)  
);

CREATE TABLE student_courses (  
  student_id INT,  
  course_id INT,  
  PRIMARY KEY (student_id, course_id),  
  FOREIGN KEY (student_id) REFERENCES students(student_id),  
  FOREIGN KEY (course_id) REFERENCES courses(course_id)  
);
```

The `student_courses` table links students and courses, implementing the "many-to-many" relationship.

## 11. What is normalization? Explain with examples

**Normalization** is a database design method aimed at organizing table structure to reduce data redundancy and improve data integrity.  
The process is performed step by step and is divided into several normal forms (1NF, 2NF, 3NF, BCNF, 4NF), each with its own rules.

### Main goals:
1. **Elimination of redundancy**  
   If the same data is duplicated in different places, this leads to a large database size, maintenance complexity, and contradictory values.  
   Normalization eliminates duplication: each logical "piece of knowledge" is stored in one place.
   In addition, redundancy directly causes **anomalies**:
   - *Insertion anomaly* — you cannot add data without additional values (for example, a new customer without an order).
   - *Update anomaly* — you have to change the same value in several places, and some records may be missed.
   - *Deletion anomaly* — when deleting unnecessary data, you can accidentally lose useful related data.
2. **Improved data integrity**  
      When a value is stored in only one table, the risk of "desynchronization" disappears.  
      For example, if a city name is stored only in the city directory, then when it changes, it is updated for all related records.
3. **Simplified maintenance and modification**  
      If the structure follows normal forms, it is easier to add new attributes, change existing ones, and extend the model.  
      There is no risk of "breaking" data by updating something only in one place.
4. **Query flexibility**  
      SQL queries become more logical: we join tables by keys, not search for values in "mixed fields".  
      This simplifies analytics and reporting.

### Example of normalization (Customer Invoices scenario)

#### Unnormalized table (0NF)

| ID | Name | Invoice No. | Invoice Date | Item No. | Description | Quantity | Unit Price |
|----|------|-------------|--------------|----------|-------------|----------|------------|

In this form, all data is stored in one table without structural logic. Records contain both customer data, invoice data, and product data. This leads to redundancy and anomalies.

#### First Normal Form (1NF)

**Rule:** all cells must be atomic (only one value). Related data groups need to be separated.

In the example, we separate into individual tables:

**Customer Details Table**

| ID | Name |
|----|------|

**Invoices Table**

| Invoice No. | Customer_ID | Invoice Date |
|-------------|-------------|--------------|

**Items Table**

| Invoice No. | Item No. | Description | Quantity | Unit Price |
|-------------|----------|-------------|----------|------------|

Now each table is responsible for its own entity.

#### Second Normal Form (2NF)

**Rule:** each non-key column must depend on the entire composite key.

In the example, the `Items` table already satisfies 2NF, since `Description` and `Unit Price` depend on the entire key `(Invoice No., Item No.)`.

#### Third Normal Form (3NF)

**Rule:** there should be no transitive dependencies. Non-key columns should depend only on the primary key.

In our case, the `Invoices` table is refined as follows:

**Updated Invoices Table**

| Invoice No. | Customer_ID | Invoice Date |
|-------------|-------------|--------------|

Here `Customer_ID` is the only attribute related to the customer.

### Practical implications

- Higher normal forms improve data integrity, but may complicate working with them.
- The specifics of the application should be considered when choosing the target normal form.

### Application in the real world

- Most databases strive for at least 3NF.
- For tasks where complete data integrity is important, 4NF and higher are used.

### Example SQL implementation (3NF)

Creating a customers table:

```sql
CREATE TABLE Customers (  
  ID INT PRIMARY KEY,  
  Name VARCHAR(50)  
);

--Creating an invoices table:

CREATE TABLE Invoices (  
  InvoiceNo INT PRIMARY KEY,  
  Customer_ID INT,  
  InvoiceDate DATE,  
  FOREIGN KEY (Customer_ID) REFERENCES Customers(ID)  
);

--Creating an items table:

CREATE TABLE Items (  
  InvoiceNo INT,  
  ItemNo INT,  
  Description VARCHAR(100),  
  Quantity INT,  
  UnitPrice DECIMAL(10,2),  
  PRIMARY KEY (InvoiceNo, ItemNo),  
  FOREIGN KEY (InvoiceNo) REFERENCES Invoices(InvoiceNo)  
);
```

This 3NF structure separates customers, invoices, and items into separate tables, ensuring data integrity and convenience in working with them.

## 12. What is denormalization and when is it used?

**Denormalization** is the process of optimizing database performance by reducing the number of joins and speeding up queries by deliberately adding data redundancy.  
This is the opposite approach to normalization: we sacrifice some integrity for speed.

### Main denormalization techniques

1. **Flattening Relationships**
- Combining related tables to minimize the number of JOINs.
- Example: combining `Order` and `Product` tables to remove many-to-many relationships.

2. **Aggregating Data**
- Pre-calculating values to reduce load during queries.
- Example: adding a `Sales_Total` column to the `Order` table.

3. **Adding Redundant Data**
- Duplicating information to reduce the number of joins.
- Example: both `Customer` and `Sales` tables contain a `Country` field, although it's related through `Customer`.

### Typical application scenarios

- **Reports and Analytics**
  - Reports often require joins of many tables.
  - Denormalization simplifies structure and speeds up report generation.

- **High-Volume Transaction Systems**
  - If short-term data inconsistency is acceptable, denormalization speeds up operations.
  - Example: in e-commerce, a small delay in sales data updates is acceptable for faster order processing.

- **Read-Mostly Applications**
  - Where there are many read operations and few writes.

- **Search- and Query-Intensive Applications**
  - Search engines often store data in denormalized form to speed up search.

- **Distributed Systems (Partitioning Data)**
  - In NoSQL or Hadoop, data is often stored with redundancy to speed up access on different nodes.

### Considerations and trade-offs

- **Performance vs. consistency**
  - Denormalization speeds up queries but reduces data consistency.

- **Maintenance challenges**
  - Redundant data needs to be synchronized and updated.

- **Operational simplicity**
  - Sometimes a simple denormalized structure is more convenient than strict normalization.

- **Query flexibility**
  - Normalized data is easier to adapt to new queries and schema changes, while denormalized structures require additional effort for modifications.

### Summary

Denormalization is used when **access speed and query simplification are more important** than strict data integrity.  
It is usually applied:
- in analytical systems,
- with very large data volumes,
- in search engines,
- in e-commerce to speed up response.

## 13. What are indexes and how do they improve query performance?

**Index** is an additional data structure in a DBMS that stores column values (or columns) in a sorted/optimized form for search along with pointers to corresponding table rows.

**Analogy:**
- Without index: searching for a word in a book by flipping through every page (full scan, table scan).
- With index: looking in the alphabetical index and immediately finding the right page.

### Main types of indexes

#### Clustered Index
1. **What it does:** sets the physical order of row storage in the table.
2. **Limitation:** only one per table.
3. **Example:** if you make `PRIMARY KEY order_id` (e.g., in SQL Server), by default this will be a clustered index — rows will actually be arranged by `order_id`.
4. **Plus:** fast search and sorting by key.
5. **Minus:** when inserting values "not at the end", rows need to be rearranged → can be slow.

#### Non-Clustered Index
- **What it does:** stores a copy of keys + pointer to location in table (Row ID or clustered key).
- **How many allowed:** dozens per table.
- **Example:**

```sql
  CREATE INDEX idx_orders_customer  
  ON orders(customer_id);
```

- **Plus:** fast search by non-primary fields.
- **Minus:** after finding the key, the DBMS still goes to the table for other fields (lookup).

#### Covering Index
- **What it does:** contains all fields needed for a specific query — keys and INCLUDE columns.
- **Why:** to avoid going to the table — everything is read from the index.
- **Example:**
```sql
CREATE INDEX idx_orders_cover  
ON orders(customer_id, order_date)  
INCLUDE(total_amount);
```
Query:
```sql
SELECT customer_id, order_date, total_amount  
FROM orders  
WHERE customer_id = 101;
```
can be executed entirely from the index.

#### Unique Index
- **Purpose:** ensures uniqueness of values.
- **Where it appears:** created automatically with `PRIMARY KEY` and `UNIQUE` constraints.

### How indexes work internally

- Most commonly uses **B-tree**: nodes store value ranges and references; search is `O(log N)` (not `O(N)`).
- For full-text/geo data, other structures may be used: **hash**, **GiST**, **R-tree**.

### When indexes help

- `WHERE` condition on indexed column selecting **small portion of rows**.
- `JOIN` on indexed columns.
- `ORDER BY` / `GROUP BY` on indexable columns.
- Frequent point (`=`) and range (`BETWEEN`, `>`, `<`) queries.

### When indexes don't help

- Very small table — full scan is faster.
- Queries selecting **>20–30%** of rows — reading the entire table may be more beneficial.
- Conditions with functions/expressions on column (index not used):

```sql
    WHERE YEAR(order_date) = 2024    ← index on `order_date` won't apply
```
- Non-selective conditions (e.g., `WHERE gender = 'M'`, if 90% of values are `'M'`).

### Downsides of indexes

- **Memory:** take up disk space.
- **Write:** slow down `INSERT/UPDATE/DELETE` because indexes also need to be updated.
- **Maintenance:** when changing table structure, indexes need to be rebuilt.

### How to check if an index is being used

**PostgreSQL (SQL text):**

```sql
  EXPLAIN ANALYZE  
  SELECT *
  FROM orders  
  WHERE customer_id = 123;
```

Look for **Index Scan** (PostgreSQL) or **Index Seek**/**Index Scan** (SQL Server) in the plan.
- *Index Seek* — fast address search through tree.
- *Index Scan* — sequential reading of entire index (better than table scan, but worse than seek).

### Practical case

**Problem:** in Power BI report, query

```sql
  SELECT customer_id, SUM(amount)  
  FROM orders  
  WHERE order_date >= '2024-01-01'  
  GROUP BY customer_id;
```

took ~2 minutes.

**Solution:** created covering index
```sql
CREATE INDEX idx_orders_perf  
ON orders(order_date, customer_id)  
INCLUDE(amount);
```

— execution time reduced to ~5 seconds.

## 14. What is PL/SQL?

**a.** PL/SQL (Procedural Language/SQL) is Oracle's procedural extension to SQL,  
which allows combining SQL statements with procedural programming capabilities.

**b. Key properties:**
1. **Block structure** — code is organized into declarative, executable, and exception handling sections.
2. **SQL integration** — allows embedding DML statements and SELECT queries within procedural code.
3. **Variables and data types** — support for SQL data types and ability to declare variables, constants, and cursors.
4. **Control structures** — support for `IF-THEN-ELSE`, `CASE`, and loops (`FOR`, `WHILE`, `LOOP`).
5. **Exception handling** — built-in error handling mechanism through `EXCEPTION` blocks.
6. **Modularity** — support for procedures, functions, packages, and triggers.
7. **Performance** — reduces network traffic by executing logic directly in the database.

**Example:**
```sql
DECLARE
  -- Объявляем переменную для хранения количества сотрудников
  v_total NUMBER;
BEGIN
  -- Считаем количество строк в таблице employees и сохраняем в переменную
  SELECT COUNT(*) 
  INTO v_total 
  FROM employees;

  -- Выводим результат на экран
  DBMS_OUTPUT.PUT_LINE('Total employees: ' || v_total);

EXCEPTION
  -- Ловим все возможные ошибки и выводим сообщение
  WHEN OTHERS THEN
    DBMS_OUTPUT.PUT_LINE('Error occurred');
END;
```
**PL/SQL limitations:**
1. **Maintenance complexity** — excessive placement of business logic in DB complicates debugging and testing.
2. **Limited reusability** — PL/SQL code cannot be directly used in external applications written in other languages.
3. **Version dependency** — some functions are only available in newer Oracle versions.
4. **Potential DB load** — moving heavy logic to PL/SQL can overload the server instead of distributing load to application layer.
5. **Need for specialized skills** — not all developers know PL/SQL, which can limit team flexibility.

**Conclusion:**  
PL/SQL is best used selectively, combining database-side logic with application-side processing.

## 15. What is the difference between procedural and declarative approach in SQL?

**Declarative SQL** focuses on *what result* is needed, leaving *how* to get it up to the DBMS.
For example, a simple `SELECT` statement specifies the required data, and the DBMS itself determines the best execution plan. Declarative SQL is concise and optimized by the database, but provides little control over execution order.

**Procedural SQL** combines SQL with programming constructs to explicitly specify *how* to perform operations.
In PL/SQL, for example, you can use loops, conditions, and error handling to implement complex business logic.
This approach provides more control and flexibility, but can be verbose and harder to maintain.

## 16. Explain ACID properties in the context of databases

**ACID** properties describe the fundamental guarantees that a database management system (DBMS) must provide when executing transactions. They include:

1. **Atomicity (Атомарность)**
- A transaction is treated as a single unit: either it executes completely or not at all.
- If an error occurs during execution, all changes are rolled back (rollback).

2. **Consistency (Согласованность)**
- A transaction must transition the database from one consistent state to another.
- All rules, constraints, and data relationships (constraints, triggers) are preserved.

3. **Isolation (Изолированность)**
- Concurrent transaction execution should not lead to conflicts.
- The result of parallel transactions is equivalent to their sequential execution (serializability).

4. **Durability (Надёжность/Долговечность)**
- After commit, the transaction result is saved in the database even in case of system failures or power outages.

**Example:**  
If a user transfers money between accounts:
- Atomicity guarantees that money will be debited from one account and credited to another, or the operation won't happen at all.
- Consistency guarantees that the sum on both accounts will comply with the rules.
- Isolation guarantees that parallel transfers won't "mix up" data.
- Durability guarantees that after successful transfer, data won't be lost.

## 17. What is transaction isolation level? What types exist and what problems do they solve?

**Transaction isolation level** is a rule that determines how one transaction sees data being modified by other transactions and how strictly the database protects data from competing changes.

Standardized in **SQL (ANSI/ISO SQL-92)**.

### Problems that isolation solves
When transactions run concurrently, anomalies can occur:

1. **Dirty Read (грязное чтение)**
- A transaction reads data that hasn't been committed yet (`COMMIT`).
- If the source transaction rolls back, the data turns out to be "non-existent".

2. **Non-Repeatable Read (неповторяемое чтение)**
- In one transaction, the same row is read twice, but the result is different because another transaction changed the data.

3. **Phantom Read (фантомные чтения)**
- In one transaction, a query is executed by condition (e.g., `WHERE salary > 1000`).
- When executed again, new rows appear (inserted by another transaction).

4. **Lost Update (потерянное обновление)**
- Two transactions read a row, change it, and save it.
- One transaction's update is "overwritten" by another.

### Isolation levels

1. **READ UNCOMMITTED**
- Minimum isolation.
- Allows *dirty reads*.
- Fast, but unsafe.
- Rarely used (e.g., for analytics where inaccurate data is acceptable).

2. **READ COMMITTED** (default in PostgreSQL, Oracle, MS SQL)
- Prohibits *dirty reads*.
- But allows *non-repeatable reads* and *phantoms*.
- Balance between performance and safety.

3. **REPEATABLE READ** (default in MySQL InnoDB)
- Guarantees that when re-reading rows, the result will be the same.
- Excludes *dirty* and *non-repeatable reads*.
- But phantoms may appear (in PostgreSQL solved through MVCC and actually no phantoms).

4. **SERIALIZABLE**
- Maximum isolation.
- Emulates sequential transaction execution.
- Excludes all anomalies (dirty, non-repeatable, phantoms).
- But very expensive in terms of performance (many locks or conflict checks).

### Comparison of isolation levels

| Level           | Dirty Read | Non-Repeatable Read | Phantom Read | Performance |
|-------------------|------------|---------------------|--------------|--------------------|
| READ UNCOMMITTED  | ✔          | ✔                   | ✔            | Very high      |
| READ COMMITTED    | ✘          | ✔                   | ✔            | High            |
| REPEATABLE READ   | ✘          | ✘                   | ✔*           | Medium            |
| SERIALIZABLE      | ✘          | ✘                   | ✘            | Low             |

\* In PostgreSQL with `REPEATABLE READ`, there are no phantoms due to MVCC mechanism.

### Pros and cons

- **READ UNCOMMITTED**
  - ✅ Fastest
  - ❌ Dirty data, risk of errors

- **READ COMMITTED**
  - ✅ Good balance for OLTP
  - ❌ Possible non-repeatable reads, phantoms

- **REPEATABLE READ**
  - ✅ Suitable for reporting and analytics
  - ❌ May lock more rows than needed

- **SERIALIZABLE**
  - ✅ Maximum correctness
  - ❌ Strong drop in concurrency, locks/conflicts

### Summary
- Isolation levels are needed to choose a compromise between **data integrity** and **performance**.
- The higher the level — the more protection from anomalies, but the lower transaction concurrency.
- In practice:
  - `READ COMMITTED` — standard for OLTP systems.
  - `REPEATABLE READ` — good for analytics.
  - `SERIALIZABLE` — rarely, when absolute correctness is needed.

## 18. Explain the logical architecture of PostgreSQL

The logical architecture of PostgreSQL defines how data is organized and accessed at the conceptual level.

**Main components:**
1. **Cluster** — collection of databases managed by one PostgreSQL instance.
2. **Database** — isolated container for schemas, tables, and roles.
3. **Schema** — namespace that groups database objects and prevents name conflicts.
4. **Table** — main data storage structure in rows and columns.
5. **Index** — speeds up query execution through fast search.
6. **View** — logical representation of query result (can be regular or materialized).
7. **Role/User** — manage user access and permissions.

**How query execution works:**
- PostgreSQL checks access permissions.
- Parses the query.
- Generates execution plan.
- Extracts data from appropriate logical objects (tables, views, and indexes) without direct access to physical files.

![DB LOGICAL SCHEMA](../images/db-logical-schema.png)

## 19. Describe the physical structure of PostgreSQL

The physical architecture of PostgreSQL describes how the database is organized at the file system level and how background processes manage data storage, retrieval, and recovery. It consists of **data files, configuration files, background processes, memory structures, and transaction logging mechanisms**.

### File system structure
1. **Data Directory (PGDATA)** — root folder containing all database files.
2. **base/** — stores subdirectories for each database in the cluster with separate files for tables and indexes.
3. **global/** — stores cluster-level data (e.g., roles and tablespace definitions).
4. **pg_wal/** — contains WAL segments (Write-Ahead Log) that record all changes before applying them to data files.
5. **pg_tblspc/** — symbolic links to user-created tablespaces outside the default directory.
6. **pg_stat/** and **pg_stat_tmp/** — store collected statistics used by the query planner.

### Background processes
PostgreSQL uses a *process-per-connection* model. Main background processes:

- **Postmaster** — main management process, starts and manages other server processes.
- **Backend Process** — executes SQL queries for a specific client.
- **WAL Writer** — writes transaction changes to WAL for reliability.
- **Background Writer** — flushes modified ("dirty") pages from buffer to disk.
- **Checkpointer** — periodically flushes all pages to disk, creating a consistent recovery point (checkpoint).
- **Autovacuum Launcher & Workers** — remove "dead" records and maintain performance.
- **Archiver** — moves completed WAL segments to archive (for high-availability and backup).

### Write-Ahead Logging (WAL)
1. All changes are first written to WAL, then to data files.
2. Ensures **durability** and ability to recover after failures, as well as replication.
3. WAL is used for **point-in-time recovery** and **streaming replication**.

### Memory structures
1. **Shared Buffers** — main cache for tables and indexes, shared across all connections.
2. **WAL Buffers** — temporary storage for WAL records before flushing to disk.
3. **Work Memory** — allocated for query operations (sorting, joins, etc.).
4. **Maintenance Work Memory** — used for maintenance tasks (vacuum, index creation).

### Data flow
1. Client sends SQL query.
2. Backend process parses and plans the query.
3. Query executes and data is read from Shared Buffers (or from disk if not cached).
4. Updates are written to WAL.
5. Background Writer and Checkpointer flush data to disk.

## 20. Explain MVCC in PostgreSQL

MVCC (Multi-Version Concurrency Control) is a mechanism that allows multiple transactions to simultaneously read and modify data without mutual locking, while maintaining transaction isolation.  
Instead of locking rows for readers, PostgreSQL stores multiple versions of a row and uses "transaction snapshots" to determine data visibility.

### How it works:
1. Each row has two hidden system columns:
- **xmin** — ID of transaction that created the row.
- **xmax** — ID of transaction that deleted or modified the row.
2. On `UPDATE`, PostgreSQL marks the old row version with `xmax` value and inserts a new row with new `xmin`.
3. On `SELECT`, the database returns only row versions visible in the current transaction snapshot.
4. Old versions become "dead tuples" and are removed by `VACUUM` or `Autovacuum`.

### Advantages:
1. Readers don't block writers and vice versa.
2. Improved concurrency and performance in high-load systems.
3. Seamless work with WAL for ensuring recovery after failures.

### Disadvantages:
1. Regular `VACUUM` is required to free space.
2. "Table bloat" may occur if there are many updates but cleanup is delayed.

## 21. What is scaling using PostgreSQL as an example?

Scaling is the process of increasing PostgreSQL performance and throughput to handle more data or more concurrent queries.

### Main approaches:
1. **Vertical Scaling (scaling "up")**
- Increasing resources of one server: CPU, RAM, disks.
- Simple path: upgrade "hardware" or PostgreSQL configuration parameters.
- Example: increase `shared_buffers`, move to more powerful server.

**Pros:**
- Easy to implement.
- Doesn't require changing application architecture.

**Cons:**
- There's a physical limit (expensive and not infinite).
- One server remains a "bottleneck".

2. **Horizontal Scaling (scaling "out")**
- Adding multiple servers to distribute load.
- In PostgreSQL implemented through:
  1. **Read Replicas (Streaming Replication)** — replicas for reading.
  2. **Logical Replication** — replication at table/record level, can partially synchronize data.
  3. **Sharding** — splitting data across servers (no built-in support; use Citus, Yugabyte, etc.).
  4. **Pgpool-II / PgBouncer** — load balancing between servers.

**Pros:**
- No hard limit on power.
- Can scale reading and partially writing.

**Cons:**
- More complex setup and maintenance.
- Data consistency is harder to ensure.

### Commonly used approaches in production:
1. For **OLTP systems**: vertical scaling and read replicas.
2. For **OLAP systems**: sharding, distributed databases (e.g., Citus, Greenplum).

## 22. What is sharding?

Sharding is a **horizontal scaling** method where the database is divided into several parts (**shards**). Each shard is stored on a separate server or PostgreSQL instance.

### How it works:
1. Data is distributed across shards by sharding key (e.g., `customer_id`, `region`, date range).
2. Each shard is a full PostgreSQL database containing only part of the data.
3. Query routing logic (which shard to use) is located:
- in the application,
- in middleware,
- or in PostgreSQL extension (e.g., **Citus**).

### Types of sharding:
1. **Range sharding** — splitting by value ranges.
2. **Hash sharding** — distribution by hash function:
- ensures even data distribution across shards;
- prevents "hot nodes" (e.g., for `user_id` keys).
- General scheme:
  1. Choose sharding key (`user_id`, `order_id`, `region_id`).
  2. Calculate hash (e.g., MD5, SHA-1).
  3. Convert hex hash string to number.
  4. Take remainder when divided by number of shards (`% N`).
  5. Get shard number.
3. **Directory-based sharding** — separate dictionary table stores key-to-shard mapping.

### Pros:
1. Almost unlimited data growth.
2. Ability to process queries in parallel on different shards.
3. Load distribution.

### Cons:
1. Complex queries spanning multiple shards.
2. Difficulties in transactions between shards.
3. More complex infrastructure and maintenance.

## 23. What is replication?

Replication is the process of **copying data** from the main server (**primary**) to one or more backup servers (**standby**) to improve availability, performance, and fault tolerance.

### Streaming Replication
- Uses **WAL (Write-Ahead Log)** to transmit changes in real time.
- Can be:
  1. **Asynchronous** — primary doesn't wait for standby response.
  - Plus: faster.
  - Minus: possible small data loss on failure.
  2. **Synchronous** — primary waits for standby confirmation.
  - Plus: no data loss.
  - Minus: higher latency.

### Logical Replication
1. Transmits changes at row and table level.
2. Can replicate only **specific tables**.
3. Suitable for integration with other systems or partial data copying.

### Cascading Replication
- A replica can itself be a data source for other replicas.

### Pros:
1. High availability.
2. Read scaling (queries can be moved to replicas).
3. Real-time backup.

### Cons:
1. No write scaling (replicas are read-only).
2. In synchronous replication — higher latencies.
3. In asynchronous replication — risk of losing latest transactions.

## 24. What is partitioning in SQL?

Partitioning is splitting one large table into smaller **partitions** within **one database and one server**.  
To the user, it looks like one logical table, but physically — it's a set of separate tables.

### Types of partitioning in PostgreSQL:
1. **Range** — splitting by value ranges (e.g., by dates).
2. **List** — splitting by value lists (e.g., by regions).
3. **Hash** — distribution by hash function.

### Pros:
1. Performance improvement through **partition pruning** (only needed partition is read).
2. Convenience of maintenance (can delete or archive old partitions).
3. Suitable for working with large tables and time series.

### Cons:
1. All partitions are stored on **one server** → scaling limited by its resources.
2. Setup and maintenance can be more complex than regular tables.

### Key difference from other approaches:
1. **Sharding** — horizontal scaling (data on different servers).
2. **Partitioning** — optimization within one server.
3. **Replication** — data duplication for fault tolerance and read scaling.

## 25. What is a trigger?

A trigger is a database object that automatically executes a specified code block (usually in PL/pgSQL language) **when a certain event occurs** in a table or view.  
It "fires" without explicit call from the application — on insert, update, delete, or other events.

### Types of triggers by execution moment:
1. **BEFORE** — before operation execution (can modify data or cancel action).
2. **AFTER** — after operation execution (can log, update other tables).
3. **INSTEAD OF** — used for views, replacing operation with its own.

### Event types for triggers:
- **INSERT** — row insertion.
- **UPDATE** — row modification.
- **DELETE** — row deletion.
- **TRUNCATE** — table cleanup.

### Example in PostgreSQL
```sql
-- Function that will be called by trigger
CREATE OR REPLACE FUNCTION log_update()
RETURNS trigger AS $$
BEGIN
  INSERT INTO audit_log(table_name, operation, changed_at)
  VALUES (TG_TABLE_NAME, TG_OP, NOW());
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Create trigger
CREATE TRIGGER trg_log_update
AFTER UPDATE ON employees
FOR EACH ROW
EXECUTE FUNCTION log_update();
```

In this example:
- **TG_TABLE_NAME** — system variable with table name.
- **TG_OP** — operation type (INSERT, UPDATE, DELETE).

### Pros of triggers:
1. Task automation (logging, validation, cascading changes).
2. Centralizing logic at DB level.
3. Guarantee that rule will execute regardless of application.

### Cons of triggers:
1. Hidden logic (harder to debug).
2. May slow down operations with complex logic.
3. Risk of recursive trigger calls.

## 26. What is the difference between a trigger and a procedure/function?

### Function or procedure
- **Function (FUNCTION)** — object that executes code and returns a value.
  - Can be used in queries, expressions, other functions.
  - In PostgreSQL can return simple value or table (`RETURNS TABLE`).
  - Called explicitly:
    ```
    SELECT my_function(123);
    ```
- **Procedure (PROCEDURE)** — appeared in PostgreSQL 11.
  - Not required to return a value.
  - Can execute transactional commands (`COMMIT`, `ROLLBACK`).
  - Called with command:
    ```
    CALL my_procedure(123);
    ```

### Trigger
**Trigger (TRIGGER)** — object that **is not called explicitly**, but executes **automatically** when a certain event occurs (`INSERT`, `UPDATE`, `DELETE`, `TRUNCATE`).
The trigger itself doesn't contain code, but **references a function** that executes when triggered.
In PostgreSQL, trigger function must return `NEW` or `OLD` (for BEFORE triggers), or `NULL` (if operation should be cancelled).


### Key differences

| Characteristic     | Function / Procedure                           | Trigger                                       |
|--------------------|-----------------------------------------------|-----------------------------------------------|
| **How called** | Explicitly (`SELECT` or `CALL`)                   | Automatically on event                     |
| **Return value** | Function — yes, procedure — no               | Depends on type (`NEW` / `OLD`)               |
| **Binding**       | Not bound to table                       | Bound to specific table or `VIEW`     |
| **Usage**  | Calculations, business logic, data operations | Logging, validation, cascading changes   |
| **Logic visibility** | Obvious in application code                  | May be hidden inside DB                   |

## 27. What are views and what are their advantages? How can they be used in the context of OLAP systems?

**View** in SQL is a virtual table based on the result of a predefined query.
Data in view is not stored (except **materialized views**, where data is saved).
Views can combine tables, filter rows, and output aggregated results.

### Advantages of views
1. **Simplification** — simplify work by hiding complex queries behind one object.
2. **Security** — can grant access only to needed columns/rows without revealing entire table.
3. **Reusability** — business logic can be reused in different applications.
4. **Abstraction** — hide applications from database schema changes.
5. **Aggregation** — allow creating pre-aggregated results for analytics.

### Usage in OLAP systems
1. Presenting aggregated metrics (e.g., sales by regions and months).
2. Working as **semantic layer** between data warehouse and BI tools.
3. Using **materialized views** to store pre-aggregated data and speed up reports.
4. Simplifying multidimensional queries (encapsulating complex `JOIN` and filters).

### Example
```sql
  CREATE VIEW sales_summary AS
  SELECT region, date_trunc('month', sale_date) AS month, SUM(amount) AS total_sales
  FROM sales
  GROUP BY region, date_trunc('month', sale_date);
```
> Such view can be directly used in BI tool to get monthly summary data without recalculating aggregates.

## 28. What is lock escalation?

Lock escalation is the process where the DBMS automatically replaces many small locks with one larger lock.

### Why this is done:
1. To reduce resources spent on lock management.
2. To avoid exceeding the limit on number of locks.

### How it works:
- If a transaction locks too many rows or pages, the DBMS can "escalate" the lock level:
  - instead of many **row-level locks** → one **table-level lock**.

**Result:**
1. DBMS manages fewer locks → less memory and CPU costs.
2. But concurrency decreases — other transactions can't work with the table.

### Lock escalation in PostgreSQL
- In PostgreSQL there is **no automatic** lock escalation (unlike, e.g., MS SQL Server).
- However, similar effect can occur if manually taking higher-level lock:
```sql
    LOCK TABLE employees IN EXCLUSIVE MODE;
```
- Also, with long transactions with many updates, competition for access may arise → other sessions will wait.

### In other DBMSs
1. **SQL Server** — lock escalation can occur from row/page level to table.
2. **Oracle** — more flexible strategy, escalation not always automatic.
3. Main reason — optimizing memory usage in lock manager.

### Pros:
1. Less overhead on lock management.
2. Reduced load on lock manager.

### Cons:
1. Sharp drop in concurrency.
2. Risk of entire table being locked due to one transaction.

## 29. What are constraints in SQL?

Constraint is a database-level rule that **limits acceptable values in columns or tables** to guarantee data integrity and correctness.

### Main types of constraints in SQL:
1. **PRIMARY KEY**
- Uniquely identifies each row.
- Doesn't allow `NULL`.
- Can be composite (multiple columns).

2. **FOREIGN KEY**
- References key in another table.
- Ensures referential integrity.
- Can specify actions on delete/update:
  - `ON DELETE CASCADE`
  - `SET NULL`
  - `RESTRICT`

3. **UNIQUE**
- Guarantees uniqueness of values in column (or set of columns).
- Can contain `NULL` (depending on DBMS).

4. **NOT NULL**
- Prohibits storing `NULL` in column.

5. **CHECK**
- Allows setting arbitrary condition.
```sql
    CHECK (salary > 0)
```
6. **EXCLUDE** (specific to PostgreSQL)
- Prohibits simultaneous execution of certain condition for different rows.
- Useful for geo data and time intervals.

### Advantages of using constraints:
1. Data quality guarantee — even with application errors.
2. Data integrity at DB level.
3. Logic simplification — some checks moved from application code to database.

### Disadvantages:
1. Excessive number of complex constraints can slow down data insertion and updates.
2. Not always flexible for complex business logic (some rules easier to implement in application).

## 30. What types of locks exist?

### Row-level locks
- **Purpose:** prevent simultaneous modification of same row by multiple transactions.
- **Example commands:**
  1. `SELECT ... FOR UPDATE` — locks selected rows for modification.
  2. `SELECT ... FOR SHARE` — locks rows for reading with prohibition on modification.
- **Characteristics:**
  - Allow parallel work with other rows in table.
  - Records are locked only when selected.
- **Pros:** minimize conflict between transactions.
- **Cons:** with mass locks, competition for access may arise  
  (in PostgreSQL no automatic escalation to table-level lock).

### Table-level locks
- **Purpose:** lock entire table or impose restrictions on certain actions.
- **Example commands:**
  1. `LOCK TABLE table_name IN ACCESS EXCLUSIVE MODE;` — full lock for any operations.
  2. Automatically applied during DDL operations (`ALTER TABLE`, `DROP TABLE`).
- **Lock modes:**
  - From `ACCESS SHARE` (minimal restriction, only reading allowed).
  - To `ACCESS EXCLUSIVE` (full prohibition on reading/writing by other transactions).
- **Pros:** simplifies integrity control during mass operations.
- **Cons:** may seriously reduce concurrency.

### Advisory Locks (advisory locks)
- **Purpose:** locks managed manually from application, not automatic in PostgreSQL.
- **Features:**
  1. Not tied to specific row or table.
  2. Can be used for synchronization between processes/applications.
- **Example commands:**
  1. `pg_advisory_lock(key)` — acquire lock with given numeric key.
  2. `pg_try_advisory_lock(key)` — attempt to acquire lock without waiting.
  3. `pg_advisory_unlock(key)` — release lock.
- **Pros:** flexible control over lock logic, ability to build custom synchronization mechanisms.
- **Cons:** developer responsible for acquiring and releasing.

## 31. What is a deadlock?

Deadlock occurs when two processes wait for each other while holding locks needed for the other to continue.  
For example:
- Process 1 locked row A and waits for row B.
- Process 2 locked row B and waits for row A → Both processes cannot continue execution.

In DB this often happens due to incorrect lock acquisition order or long transactions.  
Locks are automatically released when `COMMIT`/`ROLLBACK` is executed or if one process is forcibly terminated.

### Deadlock in PostgreSQL
- PostgreSQL **automatically detects** mutual locks.
- To prevent hanging:
  1. One transaction is **interrupted**.
  2. The other continues execution.
  3. Message *`deadlock detected`* appears if PostgreSQL finds circular dependency.

### Main causes of deadlock:
1. Different order of resource access in transactions.
2. Long transactions holding locks too long.
3. Mass updates/deletions in multiple tables without consistent order.

### Summary:
Deadlock is a design error or transaction management issue, and should be prevented by:
- consistent lock order;
- minimizing lock hold time;
- breaking long transactions into shorter ones.

## 32. What are roles?

Roles are named groups of privileges that can be assigned to users or other roles.

### Main capabilities of roles:
1. Managing access to database objects (tables, schemas, functions).
2. Centralizing access rights: instead of assigning rights to each user separately, they can be combined in roles.
3. Ability to create hierarchies: role can contain other roles.

### Typical roles in PostgreSQL:
- **LOGIN ROLE (user)** — role that can log into system.
- **GROUP ROLE (group)** — role that combines rights of multiple users.

### Examples:
```sql
    --Creating role:
    CREATE ROLE analyst;
    --Assigning role to user: 
    GRANT analyst TO user1; 
    --Creating role with login rights:
    CREATE ROLE app_user LOGIN PASSWORD 'secret';
```

## 33. What are window functions in SQL and why are they needed?

**Window functions (window functions)** are special SQL functions that perform calculations **over a set of rows (window)**, while preserving detail of each row.

Difference from aggregate functions:
- aggregates (`SUM`, `AVG`, etc.) **collapse rows** into one result,
- window functions **preserve each row** and add additional calculated value.

### Why are window functions needed?
They allow solving analytical tasks that are hard to express with regular aggregates or subqueries:
- comparing current row with others (previous/next record);
- row numbering and ranking;
- cumulative sums and moving averages;
- calculating percentages and shares;
- simplifying report building (e.g., "top N per group").

### Syntax
```sql
  <function>() OVER (
      PARTITION BY <partitioning>
      ORDER BY <sorting>
      ROWS|RANGE <window>
  )
```
- PARTITION BY — divides rows into groups (like GROUP BY, but without aggregation).
- ORDER BY — row order in window.
- ROWS / RANGE — window bounds (current, previous, all up to current, etc.).

### Examples
1. Row numbering
```sql
  SELECT
    employee_id,
    department,
    ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) AS rank_in_dept
  FROM employees;
```

2. Cumulative sum
```sql
  SELECT
    order_id,
    order_date,
    SUM(amount) OVER (ORDER BY order_date) AS running_total
  FROM orders;
```

3. Comparison with previous value
```sql
  SELECT
    order_id,
    amount,
    LAG(amount, 1) OVER (ORDER BY order_date) AS prev_amount
  FROM orders;
```

4. Share in group
```sql
  SELECT
   department,
   salary,
   salary * 100.0 / SUM(salary) OVER (PARTITION BY department) AS pct_in_dept
  FROM employees;
```

### Main types of window functions

1. **Aggregate functions as window**
- `SUM`
- `AVG`
- `COUNT`
- `MAX`
- `MIN`

2. **Ranking**
- `ROW_NUMBER()` — unique row number
- `RANK()` / `DENSE_RANK()` — ranking with or without gaps
- `NTILE(N)` — dividing into groups

3. **Offset functions**
- `LAG()` / `LEAD()` — access to previous or next row
- `FIRST_VALUE()` / `LAST_VALUE()` — first/last value in window

### Advantages

- SQL query readability (without complex subqueries).
- Higher performance than nested queries and joins.
- Support for analytical tasks (BI, OLAP).
- Universality (PostgreSQL, Oracle, SQL Server, MySQL ≥ 8).

## 34. How do aggregate functions differ from window functions?

### Aggregate functions
Aggregate functions (`SUM`, `AVG`, `COUNT`, `MAX`, `MIN`, etc.)
- Applied to **multiple rows** and return **one value** for entire group.
- Used together with `GROUP BY`.

**Example:**
```sql
  SELECT department, AVG(salary) AS avg_salary
  FROM employees
  GROUP BY department;
```
→ For each department returns **one row** with averaged salary.

### Window functions
Window functions use same aggregates but with `OVER()` addition.
- Preserve **all rows** in result.
- Add new calculated value for each row based on "window".

**Example:**
```sql
  SELECT
    employee_id,
    department,
    salary,
    AVG(salary) OVER (PARTITION BY department) AS avg_salary_in_dept
  FROM employees;
```
→ Each employee gets a column with average salary in their department, but rows **don't collapse**.

### Summary
- **Aggregate functions** answer: *"What's the overall result for the group?"*
- **Window functions** answer: *"What's the result for this row in context of its group?"*

## 35. What are PARTITION BY and ORDER BY in window functions?

#### PARTITION BY
- Divides set of rows into **groups (partitions)**, within which window function is applied.
- Similar to `GROUP BY`, but main difference — rows don't collapse, but remain in result.

**Example:**
```sql
  SELECT
    employee_id,
    department,
    salary,
    AVG(salary) OVER (PARTITION BY department) AS avg_salary_in_dept
  FROM employees;
```
→ Average salary is calculated **separately for each department**, but all employee rows are preserved.

### ORDER BY
- Defines **row order within each group (partition)**.
- Affects how function calculates values (especially for `ROW_NUMBER`, `RANK`, `LAG`, `LEAD`, cumulative sums).

**Example:**
```sql
  SELECT
    employee_id,
    department,
    salary,
    ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) AS rank_in_dept
  FROM employees;
```
→ In each department employees will be numbered by salary from highest to lowest.

### Using together
Usually `PARTITION BY` and `ORDER BY` work **in pair**:
- `PARTITION BY` responsible for group division.
- `ORDER BY` defines order within group.

**Example:**
```sql
  SELECT
    order_id,
    customer_id,
    order_date,
    SUM(amount) OVER (
      PARTITION BY customer_id
      ORDER BY order_date
    ) AS running_total
  FROM orders;
```
→ For each customer (`customer_id`) **cumulative sum of orders** is calculated by date.

### Summary
- **PARTITION BY** — sets "window" for function, defining groups.
- **ORDER BY** — defines sequence of rows within each group.  
  Together they allow creating powerful analytical calculations directly in SQL.

## 36. How do ROW_NUMBER(), RANK() and DENSE_RANK() differ?

#### ROW_NUMBER()
- Assigns unique sequential number to row within window.
- **Numbers always go consecutively** (1, 2, 3 …), even if values are same.

**Example:**
```sql
  SELECT
    employee_id,
    department,
    salary,
    ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) AS row_num
  FROM employees;
```
→ In each department employees get unique numbers by salary. Even with same salaries, numbers will be different.

### RANK()
- Assigns rank to rows based on value.
- With same values, **same rank** is assigned, but then there will be "gaps".

**Example:**
```sql
  SELECT
    employee_id,
    department,
    salary,
    RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS rank_in_dept
  FROM employees;
```
→ If two employees tie for 1st place, both get `1`, and next gets `3` (there will be gap).

### DENSE_RANK()
- Works like `RANK()`, but **without gaps**.
- With same values, rows get same rank, but next goes immediately in order.

**Example:**
```sql
  SELECT
    employee_id,
    department,
    salary,
    DENSE_RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS dense_rank_in_dept
  FROM employees;
```
→ If two employees tie for 1st place, both get `1`, and next gets `2` (no gap).

### Summary
- **ROW_NUMBER()** — unique row numbering.
- **RANK()** — considers value equality, but leaves "holes" in numbering.
- **DENSE_RANK()** — like `RANK()`, but without gaps.

## 37. What are LAG() and LEAD() used for?

- **LAG()** and **LEAD()** are window offset functions that allow getting value from **previous** or **next** row without using subqueries or JOIN.
- Used in analytics for comparing current row with neighbors.

### LAG()
- Returns value from **previous row** within window.
- If no previous row, can specify default value.

**Example:**
```sql
  SELECT
    order_id,
    order_date,
    amount,
    LAG(amount, 1, 0) OVER (ORDER BY order_date) AS prev_amount
  FROM orders;
```
→ For each order adds amount of previous order (`0` if no previous).

### LEAD()
- Returns value from **next row** within window.
- Can also specify default value.

**Example:**
```sql
  SELECT
    order_id,
    order_date,
    amount,
    LEAD(amount, 1, 0) OVER (ORDER BY order_date) AS next_amount
  FROM orders;
```
→ For each order adds amount of next order (`0` if no next).

### Practical application
1. Trend analysis (e.g., difference between current and previous value).
```sql
  SELECT
    order_date,
    amount,
    amount - LAG(amount) OVER (ORDER BY order_date) AS diff_from_prev
  FROM orders;
```
2. Finding "dips" or "spikes" in data.
3. Comparing current and next event (e.g., determine time until next customer order).
4. In financial and BI reports — calculating indicator changes by periods.

---

### Summary
- **LAG()** — gets data from previous row.
- **LEAD()** — gets data from next row.  
  Both functions simplify analytics, replacing complex subqueries and making SQL more readable.

### 38. What is ROWS BETWEEN and how does it work?

`ROWS BETWEEN` is part of `OVER()` construct in window functions that sets **window bounds (frame)** relative to current row.  
It determines which rows will be included in calculation for each result row.

#### Syntax
```sql
  () OVER (
    PARTITION BY …
    ORDER BY …
    ROWS BETWEEN <frame_start> AND <frame_end>
  )
```

### Window frame options
- `UNBOUNDED PRECEDING` — from first row to current.
- `n PRECEDING` — n rows before current.
- `CURRENT ROW` — only current row.
- `n FOLLOWING` — n rows after current.
- `UNBOUNDED FOLLOWING` — to last row.

### Examples

**a) Cumulative sum (from beginning to current row):**
```sql
  SELECT
    order_id,
    order_date,
    amount,
    SUM(amount) OVER (
      ORDER BY order_date
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_total
  FROM orders;
```
→ Shows cumulative total for each row.

**b) Moving average (current row + 2 previous):**
```sql
  SELECT
    order_id,
    amount,
    AVG(amount) OVER (
      ORDER BY order_date
      ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS moving_avg
  FROM orders;
```
→ For each row takes its value and two previous, then calculates average.

**c) Comparison with neighbors (previous, current, next):**
```sql
  SELECT
    order_id,
    amount,
    SUM(amount) OVER (
      ORDER BY order_date
      ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
    ) AS local_sum
  FROM orders;
```
→ For each row sum is calculated from three values: previous + current + next.

### Summary
- `ROWS BETWEEN` controls which rows are included in window function calculation.
- Allows creating **cumulative totals**, **moving averages**, **local aggregates**.
- Differs from simple `PARTITION BY` + `ORDER BY` by providing fine control over window bounds.

## 39. Can window functions be used in WHERE?

No, **window functions cannot be used directly in `WHERE`**.  
Reason:
- `WHERE` executes **before** window function calculation.
- Window functions are calculated at result set formation stage (after `FROM` → `WHERE` → `GROUP BY` → `HAVING` → `WINDOW/SELECT` → `ORDER BY`).

### What happens if you try?
```sql
  SELECT
    employee_id,
    department,
    salary,
    ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) AS row_num
  FROM employees
  WHERE row_num = 1;  -- Error!
```
→ Error: column `row_num` doesn't exist at `WHERE` execution time.

### How to use correctly?
There are three main approaches:

**a) Subquery (derived table / CTE)**
```sql
  WITH ranked AS (
    SELECT
      employee_id,
      department,
      salary,
      ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) AS row_num
    FROM employees
  )
  SELECT *
  FROM ranked
  WHERE row_num = 1;
```
→ Get one employee with maximum salary in each department.

**b) Filtering through `QUALIFY` (if supported)**
Some DBMSs (Snowflake, BigQuery, Oracle 23c, DuckDB) support `QUALIFY`:
```sql
  SELECT
    employee_id,
    department,
    salary,
    ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) AS row_num
  FROM employees
  QUALIFY row_num = 1;
```
→ Simplified syntax that filters by window functions.

**c) Using window function in `HAVING` (rarely)**
Usually not applied directly, since `HAVING` comes after aggregates, but theoretically can be wrapped in subquery.

### Summary
- **Cannot** use window functions in `WHERE` directly.
- **Can** filter by their result through **CTE**, **subquery**, or **QUALIFY** (if supported).

## 40. What is QUALIFY?

### Definition
`QUALIFY` is a SQL operator that allows filtering rows **by window function results**, similar to how:
- `WHERE` filters by regular columns,
- `HAVING` filters by aggregates.

In other words:  
`QUALIFY` = "WHERE for window functions".

### Why is it needed?
Window functions are calculated **after** `WHERE` and `GROUP BY` execution.  
Usually, to filter by window function (`ROW_NUMBER`, `RANK`, `LAG`, etc.), you have to use CTE or subquery.

`QUALIFY` simplifies this: allows writing window function filtering directly in main query.

### Example without QUALIFY (via CTE)
```sql
  WITH ranked AS (
    SELECT
      employee_id,
      department,
      salary,
      ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) AS row_num
    FROM employees
  )
  SELECT *
  FROM ranked
  WHERE row_num = 1;
```

### Same query with QUALIFY
```sql
  SELECT
    employee_id,
    department,
    salary,
    ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) AS row_num
  FROM employees
  QUALIFY row_num = 1;
```
→ Get employees with **maximum salary** in each department, but record looks much shorter.

### Where QUALIFY is supported
- **BigQuery** (Google)
- **Snowflake**
- **Teradata**
- **DuckDB**
- **Oracle 23c** (newer versions)

⚠️ In PostgreSQL, MySQL, MS SQL Server **QUALIFY operator doesn't exist**, there they use CTE or subquery.

## 41. SQL query optimization: EXPLAIN and EXPLAIN ANALYZE

### SQL query optimization
DBMSs (PostgreSQL, MySQL, Oracle, SQL Server, etc.) have **query optimizers** that:
- choose best execution plan (`execution plan`);
- decide which indexes to use;
- determine join order (`join order`);
- choose join algorithms (Nested Loop, Merge Join, Hash Join);
- estimate cost (`cost`) of each operation.

To understand **how exactly the query will be executed**, use the `EXPLAIN` command.

### What is EXPLAIN?
`EXPLAIN <query>` shows **execution plan** of query:
- what steps the DBMS will take to get result;
- whether indexes will be used or full table scan (Seq Scan);
- order of table joins;
- estimated execution cost (`cost`).

**Example:**
```sql
  EXPLAIN SELECT * FROM orders WHERE customer_id = 123;
    --Output (PostgreSQL):
    -- Seq Scan on orders  (cost=0.00..431.00 rows=10 width=64)
    -- Filter: (customer_id = 123)
```
→ Query will do sequential scan of entire table (`Seq Scan`).

### What is EXPLAIN ANALYZE?
- `EXPLAIN ANALYZE <query>` actually executes query and shows:
  - actual execution time of each step;
  - number of actually read rows;
  - how many rows each action returned;
  - difference between **optimizer estimates** and **reality**.

**Example:**
```sql
    EXPLAIN ANALYZE
    SELECT * FROM orders WHERE customer_id = 123;
    Output:
    Seq Scan on orders  (cost=0.00..431.00 rows=10 width=64) (actual time=0.012..15.234 rows=8 loops=1)
    Filter: (customer_id = 123)
    Rows Removed by Filter: 5000
    Execution Time: 15.300 ms
```

### How to analyze result
1. **Scan type**
- `Seq Scan` — full table pass (slow for large data).
- `Index Scan` / `Index Only Scan` — using index (faster).
- `Bitmap Heap Scan` — combined reading through index.

2. **Join methods**
- `Nested Loop` — good for small datasets.
- `Merge Join` — good for sorted data.
- `Hash Join` — good for large unsorted tables.

3. **Cost**
- Written as `cost=start..end`.
- This is "cost estimate" (lower is better).
- Used by optimizer to choose plan.
- - What makes up cost
    Optimizer considers:
- **Row scanning**
  - sequential (`Seq Scan`) more expensive for large tables;
  - through index (`Index Scan`) cheaper if selection is small.
- **Selectivity** of condition (how many rows will match result).
- **Cost of reading from disk vs memory** (in PostgreSQL configured by `seq_page_cost`, `random_page_cost`, `cpu_tuple_cost` parameters).
- **Sorting** and **table joins**.

4. **Rows**
- `rows=10` — optimizer prediction.
- `actual rows=...` — real data with `EXPLAIN ANALYZE`.
- Big difference → optimizer incorrectly estimated condition selectivity.

5. **Time**
- `actual time=0.012..15.234` — actual execution time of step.
- `Execution Time` — total query execution time.

### How to use for optimization
- If you see `Seq Scan` on large table → need **index**.
- If too many rows "Rows Removed by Filter" → maybe worth **rewriting condition**.
- If `Nested Loop` on large tables → worth thinking about **Hash Join** or **Merge Join**.
- If `actual rows` differs significantly from `rows` → worth updating statistics (`ANALYZE`) or rewriting query.

### Summary
- `EXPLAIN` — shows execution plan (optimizer estimates).
- `EXPLAIN ANALYZE` — actually executes query and shows **real numbers**.
- Optimization boils down to:
  - using indexes,
  - choosing right join types,
  - minimizing unnecessary row reading,
  - updating table statistics.

## 42. What is the BASE approach in databases?

**BASE** is an acronym opposed to the classical **ACID** model.  
Used mainly in **NoSQL** and distributed databases where scalability and availability are important.

BASE stands for:
- **Basically Available** — "basically available" (system always responds, even if not always with current data).
- **Soft state** — "soft state" (system state can change over time, even without new operations, e.g., during replication).
- **Eventually consistent** — "eventual consistency" (data in different nodes synchronizes over time, but not instantly).

### Difference from ACID
- **ACID**: strict adherence to atomicity, consistency, isolation, and durability of transactions.
- **BASE**: sacrifices strict consistency for **availability and scalability**.

### Where it's applied
- Distributed systems (Cassandra, DynamoDB, CouchDB, Riak).
- High-load web applications where it's more important that system **doesn't crash** and responds quickly.
- When some data synchronization delay is acceptable.

### Example
Airline booking system in BASE style:
- Multiple users can simultaneously see the same "last ticket".
- It's possible that system will sell ticket to two customers.
- But through short time data synchronizes and real availability is corrected.

### Pros
- High availability even during failures.
- Excellent scalability in distributed systems.
- Higher performance.

### Cons
- No strict consistency (can temporarily read "stale" data).
- Harder to develop applications requiring strict transactional logic.
- Consistency achieved not instantly, but "over time".

### Summary
**BASE** is an approach opposed to ACID, focused on **scalability and availability** at the cost of strict consistency.  
It's ideal for NoSQL and distributed systems where it's important to "respond inaccurately rather than not respond at all".

## 43. What is the difference between BASE and ACID approaches?

### ACID (traditional approach for relational DBs)
- **Atomicity (Атомарность)** — transaction executes completely or not at all.
- **Consistency (Согласованность)** — data always remains in consistent state.
- **Isolation (Изолированность)** — parallel transactions don't interfere with each other.
- **Durability (Надёжность)** — data persists even after failure.

➡ Suitable for banking, financial systems where **strict correctness** is important.

### BASE (approach for distributed NoSQL systems)
- **Basically Available** — system always responds, even if not all data is consistent.
- **Soft State** — data state can change over time (e.g., during replication).
- **Eventually Consistent** — eventually data synchronizes, but not necessarily instantly.

➡ Suitable for scalable and high-load applications (social networks, online stores) where **availability** and **performance** are important.

### ACID vs BASE comparison

| Criterion           | ACID                                   | BASE                                      |
|--------------------|----------------------------------------|-------------------------------------------|
| **Goal**           | Correctness and reliability              | Scalability and availability            |
| **Consistency** | Immediately and always                         | Eventually (eventual consistency)   |
| **Data nature**| Strict state                      | Soft state (soft state)             |
| **Transactions**     | Strict, atomic                     | Relaxed, distributed               |
| **Application**     | Banks, finance, ERP systems            | Big Data, social networks, e-commerce, IoT        |
| **Pros**          | High correctness                   | High availability and scalability    |
| **Cons**         | Limited scalability          | Possible temporary inconsistency      |

### Summary
- **ACID** = strict consistency and reliability, but less flexibility when scaling.
- **BASE** = high availability and scalability, but consistency achieved not immediately.

## 44. You have a log table with billions of rows. How will you speed up searches by date and `user_id`?

Optimization should be built in several directions:

### 1. Partitioning
- Split table by date (`RANGE PARTITION BY ts`), so queries immediately discard unnecessary partitions (*partition pruning*).
- For large partitions can add sub-partitioning by hash of `user_id`.  
  → This allows reading only relevant data chunks.

### 2. Indexes
- Create composite index `(user_id, ts)` or `(ts, user_id)` — order chosen by real query patterns.
- For wide date ranges use **BRIN index** on `ts`, it's very lightweight and works well on large append-only logs.
- For frequently used "hot" data create **partial index**, e.g., only for last 30 days.
- If queries return additional fields — use **covering index (INCLUDE)**.

### 3. Query rewriting
- Don't use functions on date column (`date(ts)`) — this breaks index usage.
- Instead specify range:
```sql
  ts >= '2025-08-24 00:00:00' AND ts < '2025-08-25 00:00:00'
```
### 4. Clustering and maintenance
- Use `CLUSTER` or `pg_repack` to maintain data locality.
- Monitor statistics (`ANALYZE`) and `autovacuum` parameters.

### 5. Additionally
- For complex analytical scenarios use **TimescaleDB (hypertables)** or materialized views.
- Check real execution plan through:
```sql
  EXPLAIN (ANALYZE, BUFFERS)
    SELECT …
```

### Summary
- **Date partitioning** + **indexes on `user_id` and `ts`** — main solution.
- **BRIN and partial indexes** give advantage for huge volumes.
- Key principle — "so that DBMS reads as few rows and pages as possible".  

## 45. What are JOINs in T-SQL and can you explain the different types?

### Overview
JOINs in T-SQL are used to combine rows from two or more tables based on related columns. T-SQL supports all standard SQL JOIN types with some Microsoft-specific enhancements.

### Types of JOINs in T-SQL

#### 1. INNER JOIN
Returns only rows that have matching values in both tables.

```sql
-- Sample data
SELECT 'Orders' AS TableName, OrderID, CustomerID, OrderDate FROM Orders
UNION ALL
SELECT 'Customers' AS TableName, CustomerID, CustomerName, NULL FROM Customers;
```

**Sample Data:**
```
TableName  | OrderID | CustomerID | OrderDate
-----------|---------|------------|----------
Orders     | 1       | 101        | 2024-01-15
Orders     | 2       | 102        | 2024-01-16
Orders     | 3       | 101        | 2024-01-17
Customers  | 101     | John Smith | NULL
Customers  | 102     | Jane Doe   | NULL
Customers  | 103     | Bob Johnson| NULL
```

```sql
SELECT o.OrderID, c.CustomerName
FROM Orders o
INNER JOIN Customers c ON o.CustomerID = c.CustomerID;
```

**INNER JOIN Result:**
```
OrderID | CustomerName
--------|-------------
1       | John Smith
2       | Jane Doe
3       | John Smith
```

#### 2. LEFT OUTER JOIN (LEFT JOIN)
Returns all rows from the left table and matching rows from the right table. NULLs for non-matching rows.

```sql
SELECT c.CustomerName, o.OrderID
FROM Customers c
LEFT JOIN Orders o ON c.CustomerID = o.CustomerID;
```

**LEFT JOIN Result:**
```
CustomerName | OrderID
-------------|--------
John Smith   | 1
John Smith   | 3
Jane Doe     | 2
Bob Johnson  | NULL
```

#### 3. RIGHT OUTER JOIN (RIGHT JOIN)
Returns all rows from the right table and matching rows from the left table.

```sql
SELECT c.CustomerName, o.OrderID
FROM Customers c
RIGHT JOIN Orders o ON c.CustomerID = o.CustomerID;
```

**RIGHT JOIN Result:**
```
CustomerName | OrderID
-------------|--------
John Smith   | 1
Jane Doe     | 2
John Smith   | 3
NULL         | 4
```

#### 4. FULL OUTER JOIN (FULL JOIN)
Returns all rows when there's a match in either table.

```sql
SELECT c.CustomerName, o.OrderID
FROM Customers c
FULL OUTER JOIN Orders o ON c.CustomerID = o.CustomerID;
```

**FULL OUTER JOIN Result:**
```
CustomerName | OrderID
-------------|--------
John Smith   | 1
John Smith   | 3
Jane Doe     | 2
Bob Johnson  | NULL
NULL         | 4
```

#### 5. CROSS JOIN
Returns Cartesian product of both tables. This join type doesn’t require any explicit join conditions.

```sql
-- Sample data
SELECT ProductName FROM Products;
SELECT CategoryName FROM Categories;
```

**Sample Data:**
```
Products:           Categories:
ProductName         CategoryName
-----------         ------------
Laptop              Electronics
Mouse               Accessories
Keyboard            Office
```

```sql
SELECT p.ProductName, c.CategoryName
FROM Products p
CROSS JOIN Categories c;
```

**CROSS JOIN Result:**
```
ProductName | CategoryName
------------|-------------
Laptop      | Electronics
Laptop      | Accessories
Laptop      | Office
Mouse       | Electronics
Mouse       | Accessories
Mouse       | Office
Keyboard    | Electronics
Keyboard    | Accessories
Keyboard    | Office
```

#### 6. SELF JOIN
Joins a table with itself. It’s useful when a table has a ‘parent’ and ‘child’ relationship, such as an employee’s hierarchical structure.

```sql
-- Sample employee data
SELECT EmployeeID, EmployeeName, ManagerID FROM Employees;
```

**Sample Employee Data:**
```
EmployeeID | EmployeeName | ManagerID
-----------|--------------|----------
1          | John (CEO)   | NULL
2          | Alice        | 1
3          | Bob          | 1
4          | Carol        | 2
```

```sql
SELECT e1.EmployeeName, e2.EmployeeName AS ManagerName
FROM Employees e1
INNER JOIN Employees e2 ON e1.ManagerID = e2.EmployeeID;
```

**SELF JOIN Result:**
```
EmployeeName | ManagerName
-------------|------------
Alice        | John (CEO)
Bob          | John (CEO)
Carol        | Alice
```

### 7. Anti Join
An Anti Join is similar to a LEFT JOIN, but it only returns rows for which there is **no match** in the other table.

**Пример:**
```sql
SELECT c.CustomerID  
FROM Customers c  
LEFT JOIN Orders o ON c.CustomerID = o.CustomerID  
WHERE o.CustomerID IS NULL;
```

### 8. Semi Join
A Semi Join returns rows from the first table if a match exists in the second table, without duplicating rows. It is usually implemented with EXISTS or IN.

**Пример:**
```sql
SELECT c.CustomerID  
FROM Customers c  
WHERE EXISTS (  
    SELECT 1   
    FROM Orders o   
    WHERE o.CustomerID = c.CustomerID  
);
```

### 9. Equi Join
An Equi Join is based on equality between columns. This is the most common type of join (INNER JOIN with `=`).

**Пример:**
```sql
SELECT e.EmployeeID, d.DepartmentName  
FROM Employees e  
INNER JOIN Departments d   
ON e.DepartmentID = d.DepartmentID;
```

### 10. Non-Equi Join
A Non-Equi Join uses a condition other than equality (e.g., `<`, `>`, `BETWEEN`).

**Пример:**
```sql
SELECT p.ProductID, r.Rating  
FROM Products p  
JOIN Ratings r   
ON p.Price BETWEEN r.MinPrice AND r.MaxPrice;
```

### T-SQL Specific JOIN Features

#### APPLY Operators
- **CROSS APPLY**: Similar to INNER JOIN but works with table-valued functions
- **OUTER APPLY**: Similar to LEFT JOIN but works with table-valued functions

```sql
SELECT c.CustomerID, c.CustomerName, o.OrderDate
FROM Customers c
CROSS APPLY dbo.GetCustomerOrders(c.CustomerID) o;
```

### Performance Considerations
- Use appropriate indexes on JOIN columns
- Consider JOIN order for optimal execution plans
- Use query hints if necessary: `OPTION (HASH JOIN)`, `OPTION (MERGE JOIN)`

## 46. Describe recursive CTEs and provide an example of when they might be used

### What are Recursive CTEs?
Recursive Common Table Expressions (CTEs) allow you to write queries that reference themselves, enabling hierarchical data traversal and iterative operations.

### Syntax Structure
```sql
WITH RecursiveCTE AS (
    -- Anchor member (base case)
    SELECT columns FROM table WHERE condition
    
    UNION ALL
    
    -- Recursive member (recursive case)
    SELECT columns FROM table t
    INNER JOIN RecursiveCTE r ON t.parent = r.child
)
SELECT * FROM RecursiveCTE;
```

### Example 1: Employee Hierarchy
```sql
-- Original employee data
SELECT EmployeeID, EmployeeName, ManagerID
FROM Employees;
```

**Original Employee Data:**
```
EmployeeID | EmployeeName | ManagerID
-----------|--------------|----------
1          | John (CEO)   | NULL
2          | Alice        | 1
3          | Bob          | 1
4          | Carol        | 2
5          | David        | 2
6          | Eve          | 3
```

```sql
WITH EmployeeHierarchy AS (
    -- Anchor: Top-level employees (no manager)
    SELECT EmployeeID, EmployeeName, ManagerID, 0 AS Level
    FROM Employees
    WHERE ManagerID IS NULL
    
    UNION ALL
    
    -- Recursive: Subordinates
    SELECT e.EmployeeID, e.EmployeeName, e.ManagerID, eh.Level + 1
    FROM Employees e
    INNER JOIN EmployeeHierarchy eh ON e.ManagerID = eh.EmployeeID
)
SELECT EmployeeID, EmployeeName, Level,
       REPLICATE('  ', Level) + EmployeeName AS Hierarchy
FROM EmployeeHierarchy
ORDER BY Level, EmployeeName;
```

**Hierarchy Result:**
```
EmployeeID | EmployeeName | Level | Hierarchy
-----------|--------------|-------|------------------
1          | John (CEO)   | 0     | John (CEO)
2          | Alice        | 1     |   Alice
3          | Bob          | 1     |   Bob
4          | Carol        | 2     |     Carol
5          | David        | 2     |     David
6          | Eve          | 2     |     Eve
```

### Example 2: Bill of Materials (BOM)
```sql
-- Original BOM data
SELECT ProductID, ProductName, ParentProductID, ComponentID, Quantity
FROM BillOfMaterials;
```

**Original BOM Data:**
```
ProductID | ProductName | ParentProductID | ComponentID | Quantity
----------|-------------|-----------------|-------------|----------
1001      | Laptop      | NULL            | 2001        | 1
1001      | Laptop      | NULL            | 2002        | 1
2001      | Screen      | 1001            | 3001        | 1
2001      | Screen      | 1001            | 3002        | 2
2002      | Keyboard    | 1001            | 3003        | 1
3001      | LCD Panel   | 2001            | NULL        | 1
3002      | Backlight   | 2001            | NULL        | 1
3003      | Key Switch  | 2002            | NULL        | 1
```

```sql
WITH BOMExplosion AS (
    -- Anchor: Top-level products
    SELECT ProductID, ProductName, ComponentID, Quantity, 1 AS Level
    FROM BillOfMaterials
    WHERE ParentProductID IS NULL
    
    UNION ALL
    
    -- Recursive: Components and sub-components
    SELECT b.ProductID, b.ProductName, b.ComponentID, 
           b.Quantity * be.Quantity, be.Level + 1
    FROM BillOfMaterials b
    INNER JOIN BOMExplosion be ON b.ParentProductID = be.ComponentID
)
SELECT * FROM BOMExplosion
WHERE ProductID = 1001; -- Specific product explosion
```

**BOM Explosion Result:**
```
ProductID | ProductName | ComponentID | Quantity | Level
----------|-------------|-------------|----------|-------
1001      | Laptop      | 2001        | 1        | 1
1001      | Laptop      | 2002        | 1        | 1
1001      | Laptop      | 3001        | 1        | 2
1001      | Laptop      | 3002        | 2        | 2
1001      | Laptop      | 3003        | 1        | 2
```

### Example 3: Organizational Chart
```sql
WITH OrgChart AS (
    -- Anchor: CEO
    SELECT EmployeeID, EmployeeName, ManagerID, 
           CAST(EmployeeName AS VARCHAR(1000)) AS HierarchyPath,
           1 AS Level
    FROM Employees
    WHERE ManagerID IS NULL
    
    UNION ALL
    
    -- Recursive: All other employees
    SELECT e.EmployeeID, e.EmployeeName, e.ManagerID,
           CAST(oc.HierarchyPath + ' -> ' + e.EmployeeName AS VARCHAR(1000)),
           oc.Level + 1
    FROM Employees e
    INNER JOIN OrgChart oc ON e.ManagerID = oc.EmployeeID
)
SELECT EmployeeID, EmployeeName, HierarchyPath, Level
FROM OrgChart
ORDER BY Level, EmployeeName;
```

**Organizational Chart Result:**
```
EmployeeID | EmployeeName | HierarchyPath                    | Level
-----------|--------------|----------------------------------|-------
1          | John (CEO)   | John (CEO)                       | 1
2          | Alice        | John (CEO) -> Alice              | 2
3          | Bob          | John (CEO) -> Bob                | 2
4          | Carol        | John (CEO) -> Alice -> Carol     | 3
5          | David        | John (CEO) -> Alice -> David     | 3
6          | Eve          | John (CEO) -> Bob -> Eve         | 3
```

### Important Considerations
- **MAXRECURSION**: Default limit is 100. Use `OPTION (MAXRECURSION 0)` for unlimited recursion
- **Performance**: Can be expensive for deep hierarchies
- **Infinite loops**: Ensure proper termination conditions
- **Indexing**: Proper indexes on parent-child relationships improve performance

## 47. Explain how to use the PIVOT and UNPIVOT operators

### PIVOT Operator
Converts rows into columns, creating a cross-tabulation or pivot table.

#### Basic Syntax
```sql
SELECT columns
FROM source_table
PIVOT (
    aggregate_function(column_to_aggregate)
    FOR column_to_pivot IN (pivot_values)
) AS pivot_alias;
```

#### Example 1: Sales by Quarter
```sql
-- Original data structure
SELECT ProductID, Quarter, SalesAmount
FROM Sales;
```

**Original Data:**
```
ProductID | Quarter | SalesAmount
----------|---------|------------
1         | Q1      | 1000
1         | Q2      | 1500
1         | Q3      | 1200
1         | Q4      | 1800
2         | Q1      | 800
2         | Q2      | 900
2         | Q3      | 1100
2         | Q4      | 1300
```

```sql
-- Pivoted result
SELECT ProductID, [Q1], [Q2], [Q3], [Q4]
FROM (
    SELECT ProductID, Quarter, SalesAmount
    FROM Sales
) AS SourceTable
PIVOT (
    SUM(SalesAmount)
    FOR Quarter IN ([Q1], [Q2], [Q3], [Q4])
) AS PivotTable;
```

**Pivoted Result:**
```
ProductID | Q1   | Q2   | Q3   | Q4
----------|------|------|------|-----
1         | 1000 | 1500 | 1200 | 1800
2         | 800  | 900  | 1100 | 1300
```

#### Example 2: Employee Skills Matrix
```sql
-- Original data
SELECT EmployeeID, Skill, Proficiency
FROM EmployeeSkills;
```

**Original Data:**
```
EmployeeID | Skill      | Proficiency
-----------|------------|------------
101        | SQL        | 8
101        | C#         | 7
101        | JavaScript | 6
102        | SQL        | 9
102        | Python     | 8
102        | JavaScript | 7
103        | C#         | 9
103        | Python     | 6
```

```sql
SELECT EmployeeID, [SQL], [C#], [JavaScript], [Python]
FROM (
    SELECT EmployeeID, Skill, Proficiency
    FROM EmployeeSkills
) AS SourceTable
PIVOT (
    MAX(Proficiency)
    FOR Skill IN ([SQL], [C#], [JavaScript], [Python])
) AS PivotTable;
```

**Pivoted Result:**
```
EmployeeID | SQL | C# | JavaScript | Python
-----------|-----|----|----------- |-------
101        | 8   | 7  | 6          | NULL
102        | 9   |NULL| 7          | 8
103        |NULL | 9  | NULL       | 6
```

#### Dynamic PIVOT
```sql
DECLARE @columns NVARCHAR(MAX) = '';
DECLARE @sql NVARCHAR(MAX) = '';

-- Build column list dynamically
SELECT @columns = @columns + '[' + Quarter + '],'
FROM (SELECT DISTINCT Quarter FROM Sales) AS Quarters;

-- Remove trailing comma
SET @columns = LEFT(@columns, LEN(@columns) - 1);

-- Build dynamic SQL
SET @sql = '
SELECT ProductID, ' + @columns + '
FROM (
    SELECT ProductID, Quarter, SalesAmount
    FROM Sales
) AS SourceTable
PIVOT (
    SUM(SalesAmount)
    FOR Quarter IN (' + @columns + ')
) AS PivotTable;';

EXEC sp_executesql @sql;
```

### UNPIVOT Operator
Converts columns into rows, the reverse of PIVOT.

#### Basic Syntax
```sql
SELECT columns
FROM source_table
UNPIVOT (
    value_column FOR pivot_column IN (columns_to_unpivot)
) AS unpivot_alias;
```

#### Example 1: Unpivot Quarterly Sales
```sql
-- Original pivoted data
SELECT ProductID, Q1, Q2, Q3, Q4
FROM SalesPivot;
```

**Original Pivoted Data:**
```
ProductID | Q1   | Q2   | Q3   | Q4
----------|------|------|------|-----
1         | 1000 | 1500 | 1200 | 1800
2         | 800  | 900  | 1100 | 1300
```

```sql
-- Unpivoted result
SELECT ProductID, Quarter, SalesAmount
FROM SalesPivot
UNPIVOT (
    SalesAmount FOR Quarter IN (Q1, Q2, Q3, Q4)
) AS UnpivotTable;
```

**Unpivoted Result:**
```
ProductID | Quarter | SalesAmount
----------|---------|------------
1         | Q1      | 1000
1         | Q2      | 1500
1         | Q3      | 1200
1         | Q4      | 1800
2         | Q1      | 800
2         | Q2      | 900
2         | Q3      | 1100
2         | Q4      | 1300
```

#### Example 2: Normalize Employee Skills
```sql
-- Original pivoted data
SELECT EmployeeID, SQL, CSharp, JavaScript, Python
FROM EmployeeSkillsMatrix;
```

**Original Pivoted Data:**
```
EmployeeID | SQL | CSharp | JavaScript | Python
-----------|-----|--------|------------|-------
101        | 8   | 7      | 6          | NULL
102        | 9   | NULL   | 7          | 8
103        | NULL| 9      | NULL       | 6
```

```sql
SELECT EmployeeID, Skill, Proficiency
FROM EmployeeSkillsMatrix
UNPIVOT (
    Proficiency FOR Skill IN (SQL, CSharp, JavaScript, Python)
) AS UnpivotTable
WHERE Proficiency IS NOT NULL;
```

**Unpivoted Result:**
```
EmployeeID | Skill      | Proficiency
-----------|------------|------------
101        | SQL        | 8
101        | CSharp     | 7
101        | JavaScript | 6
102        | SQL        | 9
102        | JavaScript | 7
102        | Python     | 8
103        | CSharp     | 9
103        | Python     | 6
```

### Advanced PIVOT Examples

#### Multiple Aggregations
```sql
SELECT ProductID, 
       [Q1_Sales], [Q1_Count], 
       [Q2_Sales], [Q2_Count],
       [Q3_Sales], [Q3_Count], 
       [Q4_Sales], [Q4_Count]
FROM (
    SELECT ProductID, 
           Quarter + '_Sales' AS SalesCol,
           Quarter + '_Count' AS CountCol,
           SalesAmount,
           OrderCount
    FROM Sales
) AS SourceTable
PIVOT (
    SUM(SalesAmount) FOR SalesCol IN ([Q1_Sales], [Q2_Sales], [Q3_Sales], [Q4_Sales])
) AS SalesPivot
PIVOT (
    SUM(OrderCount) FOR CountCol IN ([Q1_Count], [Q2_Count], [Q3_Count], [Q4_Count])
) AS CountPivot;
```

### Performance Considerations
- PIVOT can be memory-intensive for large datasets
- Consider using window functions as alternatives
- Proper indexing on pivot columns improves performance
- Dynamic PIVOT requires careful SQL injection prevention

## 48. What are the scalar and table-valued functions in T-SQL?

### Scalar Functions
Return a single value and can be used in SELECT, WHERE, and other clauses.

#### Built-in Scalar Functions
```sql
-- String functions
SELECT UPPER('hello'), LOWER('WORLD'), LEN('test'), SUBSTRING('hello', 1, 3);

-- Date functions
SELECT GETDATE(), YEAR(GETDATE()), MONTH(GETDATE()), DATEDIFF(day, '2023-01-01', GETDATE());

-- Mathematical functions
SELECT ABS(-5), ROUND(3.14159, 2), CEILING(3.2), FLOOR(3.8);

-- Conversion functions
SELECT CAST(123 AS VARCHAR(10)), CONVERT(VARCHAR(10), 456), ISNULL(NULL, 'default');
```

#### User-Defined Scalar Functions
```sql
-- Create scalar function
CREATE FUNCTION dbo.CalculateAge(@BirthDate DATE)
RETURNS INT
AS
BEGIN
    DECLARE @Age INT;
    SET @Age = DATEDIFF(YEAR, @BirthDate, GETDATE());
    
    -- Adjust if birthday hasn't occurred this year
    IF DATEADD(YEAR, @Age, @BirthDate) > GETDATE()
        SET @Age = @Age - 1;
    
    RETURN @Age;
END;

-- Use scalar function
SELECT EmployeeName, dbo.CalculateAge(BirthDate) AS Age
FROM Employees;
```

**Function Result:**
```
EmployeeName | BirthDate  | Age
-------------|------------|----
John Smith   | 1985-03-15 | 39
Jane Doe     | 1990-07-22 | 34
Bob Johnson  | 1978-12-10 | 45
```

### Inline Table-Valued Functions
Return a table result set and can be used in FROM clause.

```sql
-- Create inline table-valued function
CREATE FUNCTION dbo.GetEmployeesByDepartment(@DeptID INT)
RETURNS TABLE
AS
RETURN (
    SELECT EmployeeID, EmployeeName, Salary
    FROM Employees
    WHERE DepartmentID = @DeptID
);

-- Use inline table-valued function
SELECT * FROM dbo.GetEmployeesByDepartment(1);
```

**Function Result:**
```
EmployeeID | EmployeeName | Salary
-----------|--------------|-------
101        | John Smith   | 75000
102        | Jane Doe     | 68000
103        | Bob Johnson  | 72000
```

### Multi-Statement Table-Valued Functions
Allow complex logic with multiple statements.

```sql
CREATE FUNCTION dbo.GetEmployeeHierarchy(@EmployeeID INT)
RETURNS @Result TABLE (
    EmployeeID INT,
    EmployeeName VARCHAR(100),
    Level INT,
    ManagerName VARCHAR(100)
)
AS
BEGIN
    -- Insert the employee
    INSERT INTO @Result
    SELECT e.EmployeeID, e.EmployeeName, 0, m.EmployeeName
    FROM Employees e
    LEFT JOIN Employees m ON e.ManagerID = m.EmployeeID
    WHERE e.EmployeeID = @EmployeeID;
    
    -- Insert subordinates recursively
    INSERT INTO @Result
    SELECT e.EmployeeID, e.EmployeeName, 1, e.EmployeeName
    FROM Employees e
    WHERE e.ManagerID = @EmployeeID;
    
    RETURN;
END;

-- Use multi-statement table-valued function
SELECT * FROM dbo.GetEmployeeHierarchy(100);
```

**Function Result:**
```
EmployeeID | EmployeeName | Level | ManagerName
-----------|--------------|-------|------------
100        | John Smith   | 0     | NULL
101        | Jane Doe     | 1     | John Smith
102        | Bob Johnson  | 1     | John Smith
```

### Function Categories

#### 1. Deterministic vs Non-Deterministic
```sql
-- Deterministic: Always returns same result for same input
CREATE FUNCTION dbo.Square(@Number INT)
RETURNS INT
AS
BEGIN
    RETURN @Number * @Number;
END;

-- Non-deterministic: May return different results
CREATE FUNCTION dbo.GetCurrentTime()
RETURNS DATETIME
AS
BEGIN
    RETURN GETDATE();
END;
```

#### 2. Schema-Bound Functions
A **schema-bound function** is a user-defined function created with the `WITH SCHEMABINDING` option.  
It is tightly bound to the schema of the underlying tables or views it references.
##### Key Characteristics
- The function cannot be separated from its dependencies:
- You **cannot drop or alter** referenced tables or columns while the function exists.
- This prevents accidental schema changes that would break the function.
- Usually implemented as scalar or table-valued functions.

##### Why it matters
- **Integrity**: ensures that dependent objects remain consistent with the function.
- **Optimization**: allows SQL Server to better optimize execution plans.
- **Special usage**: required for features like indexed views and persisted computed columns.

```sql
-- Schema-bound function (cannot modify underlying tables)
CREATE FUNCTION dbo.GetEmployeeCount()
RETURNS INT
WITH SCHEMABINDING
AS
BEGIN
    DECLARE @Count INT;
    SELECT @Count = COUNT(*)
    FROM dbo.Employees;
    RETURN @Count;
END;
```

### Performance Considerations

#### Scalar Functions
- Can cause performance issues in WHERE clauses
- Consider using computed columns or views
- Avoid in large result sets

```sql
-- Slow: Scalar function in WHERE
SELECT * FROM Orders
WHERE dbo.CalculateAge(CustomerBirthDate) > 25;

-- Better: Direct calculation
SELECT * FROM Orders
WHERE DATEDIFF(YEAR, CustomerBirthDate, GETDATE()) > 25;
```

#### Table-Valued Functions
- Inline functions are generally faster than multi-statement
- Can be used in JOINs and subqueries
- Consider indexing on return columns

```sql
-- Efficient: Inline function in JOIN
SELECT o.OrderID, e.EmployeeName
FROM Orders o
INNER JOIN dbo.GetEmployeesByDepartment(1) e ON o.EmployeeID = e.EmployeeID;
```

### Best Practices
- Use appropriate function type for the task
- Consider performance implications
- Document function behavior and parameters
- Use meaningful names and parameter names
- Handle NULL values appropriately

## 49. What best practices should be followed when writing T-SQL code?

### Code Structure and Formatting

#### 1. Consistent Naming Conventions
```sql
-- Use PascalCase for objects
CREATE TABLE CustomerOrders (
    OrderID INT IDENTITY(1,1) PRIMARY KEY,
    CustomerID INT NOT NULL,
    OrderDate DATETIME2 NOT NULL,
    TotalAmount DECIMAL(10,2) NOT NULL
);

-- Use descriptive names
CREATE PROCEDURE sp_GetCustomerOrderHistory
    @CustomerID INT,
    @StartDate DATETIME2,
    @EndDate DATETIME2
AS
BEGIN
    -- Procedure body
END;
```

#### 2. Proper Indentation and Spacing
```sql
-- Good formatting
SELECT 
    c.CustomerName,
    o.OrderDate,
    o.TotalAmount,
    p.ProductName
FROM Customers c
    INNER JOIN Orders o ON c.CustomerID = o.CustomerID
    INNER JOIN OrderDetails od ON o.OrderID = od.OrderID
    INNER JOIN Products p ON od.ProductID = p.ProductID
WHERE c.CustomerID = @CustomerID
    AND o.OrderDate BETWEEN @StartDate AND @EndDate
ORDER BY o.OrderDate DESC;
```

### Performance Best Practices

#### 1. Use Appropriate Data Types
```sql
-- Good: Specific data types
CREATE TABLE Orders (
    OrderID INT IDENTITY(1,1) PRIMARY KEY,
    OrderDate DATETIME2(0) NOT NULL,  -- Precise to seconds
    Amount DECIMAL(10,2) NOT NULL,    -- Currency precision
    Status VARCHAR(20) NOT NULL       -- Fixed length for status
);

-- Avoid: Generic types
CREATE TABLE Orders (
    OrderID INT IDENTITY(1,1) PRIMARY KEY,
    OrderDate DATETIME NOT NULL,      -- Less precise
    Amount FLOAT NOT NULL,            -- Floating point for currency
    Status NVARCHAR(MAX) NOT NULL     -- Overkill for status
);
```

#### 2. Proper Indexing Strategy
```sql
-- Create indexes on frequently queried columns
CREATE INDEX IX_Orders_CustomerID_OrderDate 
ON Orders (CustomerID, OrderDate);

-- Include columns for covering indexes
CREATE INDEX IX_Orders_CustomerID_OrderDate_Covering
ON Orders (CustomerID, OrderDate)
INCLUDE (TotalAmount, Status);
```

#### 3. Avoid SELECT *
```sql
-- Bad
SELECT * FROM Orders WHERE CustomerID = @CustomerID;

-- Good
SELECT OrderID, OrderDate, TotalAmount, Status
FROM Orders 
WHERE CustomerID = @CustomerID;
```

### Error Handling

#### 1. Use TRY-CATCH Blocks
```sql
BEGIN TRY
    BEGIN TRANSACTION;
    
    INSERT INTO Orders (CustomerID, OrderDate, TotalAmount)
    VALUES (@CustomerID, @OrderDate, @TotalAmount);
    
    UPDATE Customers 
    SET LastOrderDate = @OrderDate
    WHERE CustomerID = @CustomerID;
    
    COMMIT TRANSACTION;
END TRY
BEGIN CATCH
    IF @@TRANCOUNT > 0
        ROLLBACK TRANSACTION;
    
    DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
    DECLARE @ErrorSeverity INT = ERROR_SEVERITY();
    DECLARE @ErrorState INT = ERROR_STATE();
    
    RAISERROR(@ErrorMessage, @ErrorSeverity, @ErrorState);
END CATCH;
```

#### 2. Validate Input Parameters
```sql
CREATE PROCEDURE sp_InsertOrder
    @CustomerID INT,
    @OrderDate DATETIME2,
    @TotalAmount DECIMAL(10,2)
AS
BEGIN
    -- Validate parameters
    IF @CustomerID IS NULL OR @CustomerID <= 0
    BEGIN
        RAISERROR('Invalid CustomerID', 16, 1);
        RETURN;
    END;
    
    IF @TotalAmount IS NULL OR @TotalAmount < 0
    BEGIN
        RAISERROR('Invalid TotalAmount', 16, 1);
        RETURN;
    END;
    
    -- Procedure logic
END;
```

### Security Best Practices

#### 1. Use Parameterized Queries
```sql
-- Good: Parameterized query
DECLARE @SQL NVARCHAR(MAX) = 
    'SELECT * FROM Customers WHERE CustomerID = @CustomerID';
EXEC sp_executesql @SQL, N'@CustomerID INT', @CustomerID = 123;

-- Bad: String concatenation (SQL Injection risk)
DECLARE @SQL NVARCHAR(MAX) = 
    'SELECT * FROM Customers WHERE CustomerID = ' + CAST(@CustomerID AS VARCHAR(10));
EXEC sp_executesql @SQL;
```
1. A **template query** is created as text:  
   *“Select all customers where the ID equals @CustomerID.”*

2. The parameter `@CustomerID` is **declared as an integer**.

3. A **real value (123)** is passed into that parameter.

4. SQL Server executes the query and returns the row(s) for the customer with ID **123**.

👉 This is called **parameterization**: instead of hardcoding values, placeholders are used.  
It makes queries **safer** (protects against SQL injection) and **faster** (reuses execution plans).

#### 2. Principle of Least Privilege
```sql
-- Create specific roles with minimal permissions
CREATE ROLE OrderReader;
GRANT SELECT ON Orders TO OrderReader;

CREATE ROLE OrderWriter;
GRANT INSERT, UPDATE ON Orders TO OrderWriter;
```

### Code Organization

#### 1. Use Stored Procedures for Complex Logic
```sql
CREATE PROCEDURE sp_ProcessOrder
    @CustomerID INT,
    @OrderItems OrderItemType READONLY
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @OrderID INT;
    DECLARE @TotalAmount DECIMAL(10,2);
    
    -- Calculate total
    SELECT @TotalAmount = SUM(Quantity * UnitPrice)
    FROM @OrderItems;
    
    -- Insert order
    INSERT INTO Orders (CustomerID, OrderDate, TotalAmount)
    VALUES (@CustomerID, GETDATE(), @TotalAmount);
    
    SET @OrderID = SCOPE_IDENTITY();
    
    -- Insert order details
    INSERT INTO OrderDetails (OrderID, ProductID, Quantity, UnitPrice)
    SELECT @OrderID, ProductID, Quantity, UnitPrice
    FROM @OrderItems;
    
    RETURN @OrderID;
END;
```

#### 2. Use Table-Valued Parameters
```sql
-- Define table type
CREATE TYPE OrderItemType AS TABLE (
    ProductID INT,
    Quantity INT,
    UnitPrice DECIMAL(10,2)
);

-- Use in procedure
CREATE PROCEDURE sp_InsertOrderWithItems
    @CustomerID INT,
    @OrderItems OrderItemType READONLY
AS
BEGIN
    -- Procedure implementation
END;
```

### Documentation and Comments

#### 1. Document Complex Logic
```sql
CREATE PROCEDURE sp_CalculateCustomerLifetimeValue
    @CustomerID INT
AS
/*
    Purpose: Calculate customer lifetime value based on order history
    Parameters: @CustomerID - Customer identifier
    Returns: Customer lifetime value and related metrics
    Author: Development Team
    Created: 2024-01-01
    Modified: 2024-01-15
*/
BEGIN
    -- Calculate total revenue from all orders
    -- Apply discount factor for time value of money
    -- Return calculated metrics
END;
```

#### 2. Use Meaningful Comments
```sql
-- Calculate running total for each customer
-- This uses window function for better performance than self-join
SELECT 
    CustomerID,
    OrderDate,
    TotalAmount,
    SUM(TotalAmount) OVER (
        PARTITION BY CustomerID 
        ORDER BY OrderDate 
        ROWS UNBOUNDED PRECEDING
    ) AS RunningTotal
FROM Orders
ORDER BY CustomerID, OrderDate;
```

### Testing and Validation

#### 1. Test Edge Cases
```sql
-- Test with NULL values
EXEC sp_GetCustomerOrders @CustomerID = NULL;

-- Test with boundary values
EXEC sp_GetCustomerOrders @CustomerID = 0;
EXEC sp_GetCustomerOrders @CustomerID = -1;

-- Test with large datasets
EXEC sp_GetCustomerOrders @CustomerID = 999999;
```

#### 2. Use SET Options Consistently
```sql
CREATE PROCEDURE sp_Example
AS
BEGIN
    SET NOCOUNT ON;           -- Reduce network traffic
    SET ANSI_NULLS ON;        -- Standard NULL handling
    SET QUOTED_IDENTIFIER ON; -- Standard identifier handling
    SET XACT_ABORT ON;        -- Rollback on error
    
    -- Procedure logic
END;
```

### Maintenance and Monitoring

#### 1. Include Execution Statistics
```sql
CREATE PROCEDURE sp_GetOrderStatistics
AS
BEGIN
    SET STATISTICS IO ON;
    SET STATISTICS TIME ON;
    
    -- Query logic
    
    SET STATISTICS IO OFF;
    SET STATISTICS TIME OFF;
END;
```

#### 2. Log Important Operations
```sql
CREATE PROCEDURE sp_DeleteCustomer
    @CustomerID INT
AS
BEGIN
    -- Log the deletion
    INSERT INTO AuditLog (TableName, Operation, RecordID, UserID, Timestamp)
    VALUES ('Customers', 'DELETE', @CustomerID, SUSER_SNAME(), GETDATE());
    
    -- Perform deletion
    DELETE FROM Customers WHERE CustomerID = @CustomerID;
END;
```

## 50. What are the **scalar** and **table-valued functions** in T-SQL?

### Scalar Functions
Return a single value and can be used in SELECT, WHERE, and other clauses.

#### Built-in Scalar Functions Examples
```sql
-- String functions
SELECT 
    UPPER('hello world') AS UpperCase,
    LOWER('HELLO WORLD') AS LowerCase,
    LEN('Hello') AS StringLength,
    SUBSTRING('Hello World', 1, 5) AS Substring;
```

**Result:**
```
UpperCase | LowerCase | StringLength | Substring
----------|-----------|--------------|----------
HELLO WORLD| hello world| 5           | Hello
```

```sql
-- Date functions
SELECT 
    GETDATE() AS CurrentDateTime,
    YEAR(GETDATE()) AS CurrentYear,
    MONTH(GETDATE()) AS CurrentMonth,
    DATEDIFF(day, '2023-01-01', GETDATE()) AS DaysSince2023;
```

**Result:**
```
CurrentDateTime | CurrentYear | CurrentMonth | DaysSince2023
----------------|-------------|--------------|--------------
2024-01-15 14:30:25.123 | 2024 | 1 | 379
```

#### User-Defined Scalar Function Example
```sql
-- Create function to calculate age
CREATE FUNCTION dbo.CalculateAge(@BirthDate DATE)
RETURNS INT
AS
BEGIN
    DECLARE @Age INT;
    SET @Age = DATEDIFF(YEAR, @BirthDate, GETDATE());
    
    -- Adjust if birthday hasn't occurred this year
    IF DATEADD(YEAR, @Age, @BirthDate) > GETDATE()
        SET @Age = @Age - 1;
    
    RETURN @Age;
END;

-- Use the function
SELECT 
    EmployeeName, 
    BirthDate,
    dbo.CalculateAge(BirthDate) AS Age
FROM Employees;
```

**Result:**
```
EmployeeName | BirthDate  | Age
-------------|------------|----
John Smith   | 1985-03-15 | 39
Jane Doe     | 1990-07-22 | 34
Bob Johnson  | 1978-12-10 | 45
```

### Table-Valued Functions

#### Inline Table-Valued Function
```sql
-- Create inline function
CREATE FUNCTION dbo.GetEmployeesByDepartment(@DeptID INT)
RETURNS TABLE
AS
RETURN (
    SELECT EmployeeID, EmployeeName, Salary, HireDate
    FROM Employees
    WHERE DepartmentID = @DeptID
);

-- Use the function
SELECT * FROM dbo.GetEmployeesByDepartment(1);
```

**Result:**
```
EmployeeID | EmployeeName | Salary | HireDate
-----------|--------------|--------|----------
101        | John Smith   | 75000  | 2020-01-15
102        | Jane Doe     | 68000  | 2021-03-20
103        | Bob Johnson  | 72000  | 2019-11-10
```

#### Multi-Statement Table-Valued Function
```sql
CREATE FUNCTION dbo.GetEmployeeHierarchy(@EmployeeID INT)
RETURNS @Result TABLE (
    EmployeeID INT,
    EmployeeName VARCHAR(100),
    Level INT,
    ManagerName VARCHAR(100)
)
AS
BEGIN
    -- Insert the employee
    INSERT INTO @Result
    SELECT e.EmployeeID, e.EmployeeName, 0, m.EmployeeName
    FROM Employees e
    LEFT JOIN Employees m ON e.ManagerID = m.EmployeeID
    WHERE e.EmployeeID = @EmployeeID;
    
    -- Insert subordinates
    INSERT INTO @Result
    SELECT e.EmployeeID, e.EmployeeName, 1, e.EmployeeName
    FROM Employees e
    WHERE e.ManagerID = @EmployeeID;
    
    RETURN;
END;

-- Use the function
SELECT * FROM dbo.GetEmployeeHierarchy(100);
```

**Result:**
```
EmployeeID | EmployeeName | Level | ManagerName
-----------|--------------|-------|------------
100        | John Smith   | 0     | NULL
101        | Jane Doe     | 1     | John Smith
102        | Bob Johnson  | 1     | John Smith
```

## 51. How is a **stored procedure** different from a **function** in T-SQL?

### Key Differences

| Aspect | Stored Procedure | Function |
|--------|------------------|----------|
| **Return Type** | Can return multiple result sets, output parameters, or nothing | Must return a single value (scalar) or table |
| **Usage** | EXEC/EXECUTE statement | Can be used in SELECT, WHERE, FROM clauses |
| **DML Operations** | Can perform INSERT, UPDATE, DELETE | Cannot perform DML operations (except table variables) |
| **Transaction Control** | Can use BEGIN/COMMIT/ROLLBACK | Cannot control transactions |
| **Error Handling** | Can use TRY-CATCH | Limited error handling |
| **Temporary Tables** | Can create and use temporary tables | Cannot create temporary tables |

### Stored Procedure Example
```sql
CREATE PROCEDURE sp_GetCustomerOrders
    @CustomerID INT,
    @StartDate DATETIME2 = NULL,
    @EndDate DATETIME2 = NULL,
    @OrderCount INT OUTPUT
AS
BEGIN
    -- Suppresses "(x rows affected)" messages to reduce overhead and keep output clean
    SET NOCOUNT ON;
    
    -- Set default dates if not provided
    IF @StartDate IS NULL
        SET @StartDate = DATEADD(MONTH, -1, GETDATE());
    IF @EndDate IS NULL
        SET @EndDate = GETDATE();
    
    -- Get orders
    SELECT OrderID, OrderDate, TotalAmount, Status
    FROM Orders
    WHERE CustomerID = @CustomerID
        AND OrderDate BETWEEN @StartDate AND @EndDate
    ORDER BY OrderDate DESC;
    
    -- Set output parameter
    SELECT @OrderCount = COUNT(*)
    FROM Orders
    WHERE CustomerID = @CustomerID
        AND OrderDate BETWEEN @StartDate AND @EndDate;
END;

-- Execute stored procedure
DECLARE @Count INT;
EXEC sp_GetCustomerOrders 
    @CustomerID = 101,
    @StartDate = '2024-01-01',
    @EndDate = '2024-01-31',
    @OrderCount = @Count OUTPUT;

SELECT @Count AS OrderCount;
```

**Execution Result:**
```
OrderID | OrderDate  | TotalAmount | Status
--------|------------|-------------|--------
3       | 2024-01-25 | 150.00      | Completed
1       | 2024-01-15 | 200.00      | Completed

OrderCount: 2
```

### Function Example
```sql
-- Scalar function
CREATE FUNCTION dbo.GetCustomerOrderCount(@CustomerID INT)
RETURNS INT
AS
BEGIN
    DECLARE @Count INT;
    SELECT @Count = COUNT(*)
    FROM Orders
    WHERE CustomerID = @CustomerID;
    RETURN @Count;
END;

-- Use function in SELECT
SELECT 
    CustomerID,
    CustomerName,
    dbo.GetCustomerOrderCount(CustomerID) AS OrderCount
FROM Customers
WHERE CustomerID = 101;
```

**Result:**
```
CustomerID | CustomerName | OrderCount
-----------|--------------|-----------
101        | John Smith   | 2
```

## 52. Can you write a simple **stored procedure** with **input** and **output parameters**?

### Basic Stored Procedure with Parameters
```sql
CREATE PROCEDURE sp_ProcessOrder
    @CustomerID INT,                    -- Input parameter
    @ProductID INT,                     -- Input parameter
    @Quantity INT,                      -- Input parameter
    @OrderID INT OUTPUT,                -- Output parameter
    @TotalAmount DECIMAL(10,2) OUTPUT,  -- Output parameter
    @Success BIT OUTPUT                 -- Output parameter
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @UnitPrice DECIMAL(10,2);
    DECLARE @ErrorMessage NVARCHAR(4000);
    
    BEGIN TRY
        -- Validate input parameters
        IF @CustomerID IS NULL OR @CustomerID <= 0
        BEGIN
            RAISERROR('Invalid CustomerID', 16, 1);
            RETURN;
        END;
        
        IF @Quantity IS NULL OR @Quantity <= 0
        BEGIN
            RAISERROR('Invalid Quantity', 16, 1);
            RETURN;
        END;
        
        -- Get product price
        SELECT @UnitPrice = UnitPrice
        FROM Products
        WHERE ProductID = @ProductID;
        
        IF @UnitPrice IS NULL
        BEGIN
            RAISERROR('Product not found', 16, 1);
            RETURN;
        END;
        
        -- Calculate total amount
        SET @TotalAmount = @Quantity * @UnitPrice;
        
        -- Insert order
        INSERT INTO Orders (CustomerID, OrderDate, TotalAmount, Status)
        VALUES (@CustomerID, GETDATE(), @TotalAmount, 'Pending');
        
        SET @OrderID = SCOPE_IDENTITY();
        
        -- Insert order details
        INSERT INTO OrderDetails (OrderID, ProductID, Quantity, UnitPrice)
        VALUES (@OrderID, @ProductID, @Quantity, @UnitPrice);
        
        -- Update customer's last order date
        UPDATE Customers
        SET LastOrderDate = GETDATE()
        WHERE CustomerID = @CustomerID;
        
        SET @Success = 1;
        
    END TRY
    BEGIN CATCH
        SET @Success = 0;
        SET @OrderID = NULL;
        SET @TotalAmount = NULL;
        
        SET @ErrorMessage = ERROR_MESSAGE();
        RAISERROR(@ErrorMessage, 16, 1);
    END CATCH;
END;
```

### Executing the Stored Procedure
```sql
-- Declare variables for output parameters
DECLARE @NewOrderID INT;
DECLARE @CalculatedTotal DECIMAL(10,2);
DECLARE @OperationSuccess BIT;

-- Execute the stored procedure
EXEC sp_ProcessOrder
    @CustomerID = 101,
    @ProductID = 1001,
    @Quantity = 2,
    @OrderID = @NewOrderID OUTPUT,
    @TotalAmount = @CalculatedTotal OUTPUT,
    @Success = @OperationSuccess OUTPUT;

-- Display results
SELECT 
    @NewOrderID AS OrderID,
    @CalculatedTotal AS TotalAmount,
    @OperationSuccess AS Success;
```

**Execution Result:**
```
OrderID | TotalAmount | Success
--------|-------------|--------
1001    | 299.98      | 1
```

## 53. Explain how to handle **errors** in **stored procedures**

### TRY-CATCH Error Handling
```sql
CREATE PROCEDURE sp_TransferFunds
    @FromAccount INT,
    @ToAccount INT,
    @Amount DECIMAL(10,2),
    @Success BIT OUTPUT,
    @ErrorMessage NVARCHAR(4000) OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    SET @Success = 0;
    SET @ErrorMessage = '';
    
    BEGIN TRY
        -- Validate parameters
        IF @Amount <= 0
        BEGIN
            -- Throw error, it is catched in CATCH block below
            RAISERROR('Amount must be greater than zero', 16, 1);
            RETURN;
        END;
        
        IF @FromAccount = @ToAccount
        BEGIN
            -- Throw error, it is catched in CATCH block below
            RAISERROR('Cannot transfer to the same account', 16, 1);
            RETURN;
        END;
        
        BEGIN TRANSACTION;
        
        -- Check if source account has sufficient funds
        -- It fetches the current balance of the source account and saves it in a variable
        DECLARE @CurrentBalance DECIMAL(10,2);
        SELECT @CurrentBalance = Balance
        FROM Accounts
        WHERE AccountID = @FromAccount;
        
        IF @CurrentBalance < @Amount
        BEGIN
            RAISERROR('Insufficient funds', 16, 1);
            RETURN;
        END;
        
        -- Debit from source account
        UPDATE Accounts
        SET Balance = Balance - @Amount
        WHERE AccountID = @FromAccount;
        
        -- Credit to destination account
        UPDATE Accounts
        SET Balance = Balance + @Amount
        WHERE AccountID = @ToAccount;
        
        -- Record transaction
        INSERT INTO Transactions (FromAccount, ToAccount, Amount, TransactionDate)
        VALUES (@FromAccount, @ToAccount, @Amount, GETDATE());
        
        COMMIT TRANSACTION;
        SET @Success = 1;
        
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;
        
        SET @Success = 0;
        SET @ErrorMessage = ERROR_MESSAGE();
        
        -- Log error details
        INSERT INTO ErrorLog (ProcedureName, ErrorMessage, ErrorSeverity, ErrorState, ErrorTime)
        VALUES (
            'sp_TransferFunds',
            ERROR_MESSAGE(),
            ERROR_SEVERITY(),
            ERROR_STATE(),
            GETDATE()
        );
        
        -- Re-raise the error
        DECLARE @ErrorSeverity INT = ERROR_SEVERITY();
        DECLARE @ErrorState INT = ERROR_STATE();
        RAISERROR(@ErrorMessage, @ErrorSeverity, @ErrorState);
    END CATCH;
END;
```

### Testing Error Handling
```sql
-- Test with valid data
DECLARE @Success BIT, @ErrorMessage NVARCHAR(4000);
EXEC sp_TransferFunds 
    @FromAccount = 1001,
    @ToAccount = 1002,
    @Amount = 500.00,
    @Success = @Success OUTPUT,
    @ErrorMessage = @ErrorMessage OUTPUT;

SELECT @Success AS Success, @ErrorMessage AS ErrorMessage;
```

**Results:**
```
-- Valid transfer:
Success | ErrorMessage
--------|-------------
1       | 

-- Invalid amount:
Success | ErrorMessage
--------|-------------
0       | Amount must be greater than zero

-- Insufficient funds:
Success | ErrorMessage
--------|-------------
0       | Insufficient funds
```

### T-SQL Error Handling — Key Nuances

#### 1) TRY…CATCH doesn’t catch everything
- **Not catchable:** compile-time (syntax) errors, connection failures, some severe system errors (severity ≥ 20).
- **Implication:** always complement TRY…CATCH with logging/return codes and defensive checks.

#### 2) RAISERROR vs THROW
- **RAISERROR:** legacy but flexible (custom severity/state, formatted messages).
- **THROW (SQL Server 2012+):** simpler, preserves original error info when rethrowing, recommended for new code, but always severity 16.
- **Guideline:** use `THROW` to bubble errors; use `RAISERROR` if you need custom severity/state.

#### 3) Transactions & @@TRANCOUNT
- In `CATCH`, check `IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;` to avoid orphaned transactions.
- Pair every `BEGIN TRAN` with a clear **commit/rollback path** inside TRY/CATCH.

#### 4) SET XACT_ABORT
- **OFF (default):** some errors won’t auto-rollback; you must handle rollback manually.
- **ON:** any runtime error (severity ≥11) auto-rolls back the whole transaction.
- **Guideline:** prefer `SET XACT_ABORT ON;` for data-integrity critical procedures; be aware it rolls back on “minor” errors too.

#### 5) Return codes & OUTPUT parameters
- **RETURN (int):** machine-readable status (e.g., `0=OK`, `100=NotFound`, `200=ValidationError`).
- **OUTPUT params:** human-readable details (e.g., error text) or extra data.
- **Benefit:** clean contract between SQL and caller (app/ETL).

#### 6) Structured error logging
- Capture details in `CATCH` via:
    - `ERROR_MESSAGE(), ERROR_SEVERITY(), ERROR_STATE(), ERROR_NUMBER(), ERROR_LINE(), ERROR_PROCEDURE()`
- Persist to an `ErrorLog` table with timestamp and user/context for troubleshooting.

#### 7) Nested TRY…CATCH
- Inner `CATCH` can **swallow** errors.
- If the outer scope must react, **rethrow** explicitly (`THROW;` or `RAISERROR`).
- Keep nesting shallow; isolate concerns per block (validation, DML, post-actions).

## 54. How do you use the **EXECUTE statement** in T-SQL?

### Basic EXECUTE Syntax
```sql
-- Execute stored procedure
EXEC sp_GetCustomerOrders @CustomerID = 101;

-- Alternative syntax (older style)
EXECUTE sp_GetCustomerOrders 101;

-- With multiple parameters
EXEC sp_GetCustomerOrders 
    @CustomerID = 101,
    @StartDate = '2024-01-01',
    @EndDate = '2024-01-31';
```

### EXECUTE with Output Parameters
```sql
-- Declare variables for output parameters
DECLARE @OrderCount INT;
DECLARE @TotalAmount DECIMAL(10,2);

-- Execute with output parameters
-- Calls the stored procedure with @CustomerID = 101.
-- The procedure fills the OUTPUT parameters (@OrderCount, @TotalAmount)
-- so their values can be used after execution.
EXEC sp_GetCustomerSummary
    @CustomerID = 101,
    @OrderCount = @OrderCount OUTPUT,
    @TotalAmount = @TotalAmount OUTPUT;

-- Display results
SELECT 
    @OrderCount AS OrderCount,
    @TotalAmount AS TotalAmount;
```

**Result:**
```
OrderCount | TotalAmount
-----------|------------
5          | 1250.75
```

### Dynamic SQL with EXECUTE
Dynamic SQL is a way to build SQL statements as strings at runtime and then execute them. It gives flexibility (dynamic filters, table/column names, pivoting) but must be handled carefully to avoid SQL injection and performance issues.
```sql
-- Build dynamic SQL
DECLARE @TableName NVARCHAR(100) = 'Customers';
DECLARE @ColumnName NVARCHAR(100) = 'CustomerName';
DECLARE @SearchValue NVARCHAR(100) = 'John';
DECLARE @SQL NVARCHAR(MAX);

-- Construct dynamic query
SET @SQL = 'SELECT * FROM ' + QUOTENAME(@TableName) + 
           ' WHERE ' + QUOTENAME(@ColumnName) + ' LIKE @SearchValue';

-- Execute dynamic SQL
EXEC sp_executesql 
    @SQL, 
    N'@SearchValue NVARCHAR(100)', 
    @SearchValue = '%' + @SearchValue + '%';
```

**Result:**
```
CustomerID | CustomerName | Email           | Phone
-----------|--------------|-----------------|----------
101        | John Smith   | john@email.com  | 555-1234
105        | John Doe     | john.doe@email.com | 555-5678
```

## 55. What is the significance of the **RETURN statement** in **stored procedures**?

### RETURN Statement Overview
The RETURN statement immediately exits a stored procedure and can optionally return an integer value.

### Basic RETURN Usage
```sql
CREATE PROCEDURE sp_CheckCustomerExists
    @CustomerID INT
AS
BEGIN
    IF NOT EXISTS (SELECT 1 FROM Customers WHERE CustomerID = @CustomerID)
    BEGIN
        PRINT 'Customer not found';
        RETURN 1; -- Return error code
    END;
    
    PRINT 'Customer exists';
    RETURN 0; -- Return success code
END;

-- Execute and check return value
DECLARE @Result INT;
EXEC @Result = sp_CheckCustomerExists @CustomerID = 999;

SELECT 
    @Result AS ReturnCode,
    CASE @Result
        WHEN 0 THEN 'Success'
        WHEN 1 THEN 'Customer not found'
        ELSE 'Unknown error'
    END AS Status;
```

**Result:**
```
ReturnCode | Status
-----------|--------
1          | Customer not found
```

### RETURN with Different Exit Points
```sql
CREATE PROCEDURE sp_ProcessOrderWithValidation
    @CustomerID INT,
    @ProductID INT,
    @Quantity INT
AS
BEGIN
    -- Validate customer
    IF NOT EXISTS (SELECT 1 FROM Customers WHERE CustomerID = @CustomerID)
    BEGIN
        RAISERROR('Customer not found', 16, 1);
        RETURN 100; -- Custom error code
    END;
    
    -- Validate product
    IF NOT EXISTS (SELECT 1 FROM Products WHERE ProductID = @ProductID)
    BEGIN
        RAISERROR('Product not found', 16, 1);
        RETURN 101; -- Custom error code
    END;
    
    -- Validate quantity
    IF @Quantity <= 0
    BEGIN
        RAISERROR('Invalid quantity', 16, 1);
        RETURN 102; -- Custom error code
    END;
    
    -- Check stock
    DECLARE @StockQuantity INT;
    SELECT @StockQuantity = StockQuantity
    FROM Products
    WHERE ProductID = @ProductID;
    
    IF @StockQuantity < @Quantity
    BEGIN
        RAISERROR('Insufficient stock', 16, 1);
        RETURN 103; -- Custom error code
    END;
    
    -- Process order
    BEGIN TRY
        BEGIN TRANSACTION;
        
        INSERT INTO Orders (CustomerID, OrderDate, Status)
        VALUES (@CustomerID, GETDATE(), 'Pending');
        
        DECLARE @OrderID INT = SCOPE_IDENTITY();
        
        INSERT INTO OrderDetails (OrderID, ProductID, Quantity)
        VALUES (@OrderID, @ProductID, @Quantity);
        
        UPDATE Products
        SET StockQuantity = StockQuantity - @Quantity
        WHERE ProductID = @ProductID;
        
        COMMIT TRANSACTION;
        
        SELECT @OrderID AS NewOrderID;
        RETURN 0; -- Success
        
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;
        
        RAISERROR('Order processing failed', 16, 1);
        RETURN 200; -- Processing error
    END CATCH;
END;
```

### Testing Different Return Codes
```sql
-- Test with valid data
DECLARE @Result INT;
EXEC @Result = sp_ProcessOrderWithValidation 
    @CustomerID = 101, 
    @ProductID = 1001, 
    @Quantity = 2;

SELECT 
    @Result AS ReturnCode,
    CASE @Result
        WHEN 0 THEN 'Success'
        WHEN 100 THEN 'Customer not found'
        WHEN 101 THEN 'Product not found'
        WHEN 102 THEN 'Invalid quantity'
        WHEN 103 THEN 'Insufficient stock'
        WHEN 200 THEN 'Processing error'
        ELSE 'Unknown error'
    END AS Status;
```

**Results for different test cases:**
```
-- Valid data:
ReturnCode | Status
-----------|--------
0          | Success

-- Invalid customer:
ReturnCode | Status
-----------|--------
100        | Customer not found

-- Invalid product:
ReturnCode | Status
-----------|--------
101        | Product not found

-- Invalid quantity:
ReturnCode | Status
-----------|--------
102        | Invalid quantity
```

## 56. Describe the use of **table variables** and **temporary tables** in **stored procedures**

### Table Variables vs Temporary Tables

| Aspect | Table Variables | Temporary Tables |
|--------|----------------|------------------|
| **Scope** | Batch/Procedure scope | Session scope |
| **Storage** | Memory (tempdb) | Disk (tempdb) |
| **Indexes** | Limited (PRIMARY KEY, UNIQUE) | Full indexing support |
| **Statistics** | No statistics | Has statistics |
| **Performance** | Better for small datasets | Better for large datasets |
| **Transaction** | Not affected by rollbacks | Affected by rollbacks |

### Table Variables Example
```sql
CREATE PROCEDURE sp_ProcessOrderItems
    @CustomerID INT
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Declare table variable
    DECLARE @OrderItems TABLE (
        ProductID INT,
        ProductName NVARCHAR(100),
        Quantity INT,
        UnitPrice DECIMAL(10,2),
        TotalPrice DECIMAL(10,2)
    );
    
    -- Populate table variable
    INSERT INTO @OrderItems (ProductID, ProductName, Quantity, UnitPrice, TotalPrice)
    SELECT 
        p.ProductID,
        p.ProductName,
        od.Quantity,
        od.UnitPrice,
        od.Quantity * od.UnitPrice
    FROM OrderDetails od
    INNER JOIN Products p ON od.ProductID = p.ProductID
    INNER JOIN Orders o ON od.OrderID = o.OrderID
    WHERE o.CustomerID = @CustomerID
        AND o.Status = 'Pending';
    
    -- Process items
    SELECT 
        ProductName,
        Quantity,
        UnitPrice,
        TotalPrice,
        CASE 
            WHEN TotalPrice > 1000 THEN 'High Value'
            WHEN TotalPrice > 500 THEN 'Medium Value'
            ELSE 'Low Value'
        END AS ValueCategory
    FROM @OrderItems
    ORDER BY TotalPrice DESC;
    
    -- Calculate totals
    SELECT 
        COUNT(*) AS ItemCount,
        SUM(TotalPrice) AS GrandTotal,
        AVG(TotalPrice) AS AverageItemPrice
    FROM @OrderItems;
END;

-- Execute procedure
EXEC sp_ProcessOrderItems @CustomerID = 101;
```

**Result:**
```
ProductName | Quantity | UnitPrice | TotalPrice | ValueCategory
------------|----------|-----------|------------|--------------
Laptop      | 1        | 1299.99   | 1299.99    | High Value
Mouse       | 2        | 25.99     | 51.98      | Low Value
Keyboard    | 1        | 89.99     | 89.99      | Low Value

ItemCount | GrandTotal | AverageItemPrice
----------|------------|-----------------
3         | 1441.96    | 480.65
```

### Temporary Tables Example
```sql
CREATE PROCEDURE sp_AnalyzeCustomerOrders
    @StartDate DATETIME2,
    @EndDate DATETIME2
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Create temporary table
    CREATE TABLE #CustomerOrderSummary (
        CustomerID INT,
        CustomerName NVARCHAR(100),
        OrderCount INT,
        TotalAmount DECIMAL(10,2),
        AverageOrderValue DECIMAL(10,2),
        LastOrderDate DATETIME2
    );
    
    -- Populate temporary table
    INSERT INTO #CustomerOrderSummary
    SELECT 
        c.CustomerID,
        c.CustomerName,
        COUNT(o.OrderID) AS OrderCount,
        SUM(o.TotalAmount) AS TotalAmount,
        AVG(o.TotalAmount) AS AverageOrderValue,
        MAX(o.OrderDate) AS LastOrderDate
    FROM Customers c
    INNER JOIN Orders o ON c.CustomerID = o.CustomerID
    WHERE o.OrderDate BETWEEN @StartDate AND @EndDate
    GROUP BY c.CustomerID, c.CustomerName;
    
    -- Create index on temporary table
    CREATE INDEX IX_Temp_CustomerOrderSummary_TotalAmount 
    ON #CustomerOrderSummary (TotalAmount DESC);
    
    -- Analyze data
    SELECT 
        'Top Customers' AS AnalysisType,
        CustomerName,
        OrderCount,
        TotalAmount,
        AverageOrderValue
    FROM #CustomerOrderSummary
    WHERE TotalAmount > (
        SELECT AVG(TotalAmount) * 1.5 
        FROM #CustomerOrderSummary
    )
    ORDER BY TotalAmount DESC;
    
    -- Summary statistics
    SELECT 
        COUNT(*) AS TotalCustomers,
        SUM(OrderCount) AS TotalOrders,
        SUM(TotalAmount) AS TotalRevenue,
        AVG(AverageOrderValue) AS OverallAverageOrderValue
    FROM #CustomerOrderSummary;
    
    -- Clean up
    DROP TABLE #CustomerOrderSummary;
END;

-- Execute procedure
EXEC sp_AnalyzeCustomerOrders 
    @StartDate = '2024-01-01',
    @EndDate = '2024-01-31';
```

**Result:**
```
AnalysisType | CustomerName | OrderCount | TotalAmount | AverageOrderValue
-------------|--------------|------------|-------------|------------------
Top Customers| John Smith   | 5          | 2500.00     | 500.00
Top Customers| Jane Doe     | 3          | 1800.00     | 600.00

TotalCustomers | TotalOrders | TotalRevenue | OverallAverageOrderValue
---------------|-------------|--------------|------------------------
15             | 45          | 12500.00     | 277.78
```

### Performance Comparison Example
```sql
CREATE PROCEDURE sp_PerformanceComparison
    @UseTableVariable BIT = 1
AS
BEGIN
    SET NOCOUNT ON;
    
    IF @UseTableVariable = 1
    BEGIN
        -- Using table variable
        DECLARE @Products TABLE (
            ProductID INT PRIMARY KEY,
            ProductName NVARCHAR(100),
            CategoryID INT,
            Price DECIMAL(10,2)
        );
        
        INSERT INTO @Products
        SELECT ProductID, ProductName, CategoryID, UnitPrice
        FROM Products
        WHERE UnitPrice > 100;
        
        SELECT COUNT(*) AS ProductCount
        FROM @Products p
        INNER JOIN Categories c ON p.CategoryID = c.CategoryID
        WHERE c.CategoryName = 'Electronics';
    END
    ELSE
    BEGIN
        -- Using temporary table
        CREATE TABLE #Products (
            ProductID INT PRIMARY KEY,
            ProductName NVARCHAR(100),
            CategoryID INT,
            Price DECIMAL(10,2)
        );
        
        CREATE INDEX IX_Temp_Products_CategoryID ON #Products (CategoryID);
        
        INSERT INTO #Products
        SELECT ProductID, ProductName, CategoryID, UnitPrice
        FROM Products
        WHERE UnitPrice > 100;
        
        SELECT COUNT(*) AS ProductCount
        FROM #Products p
        INNER JOIN Categories c ON p.CategoryID = c.CategoryID
        WHERE c.CategoryName = 'Electronics';
        
        DROP TABLE #Products;
    END;
END;

-- Test both approaches
EXEC sp_PerformanceComparison @UseTableVariable = 1; -- Table variable
EXEC sp_PerformanceComparison @UseTableVariable = 0; -- Temporary table
```

## 57. Discuss the use of variables in T-SQL

### Nuances of Using Variables in T-SQL

#### 1. Scope
- Variables exist only within the current **batch**, **stored procedure**, or **function**.
- They cannot be shared across batches separated by `GO`.
- Block-level variables live until the end of the procedure, but cursor variables have stricter scope rules.

#### 2. Initialization and Assignment
- Variables can be initialized on declaration: `DECLARE @x INT = 10;`.
- Assignment options:
    - `SET` → assigns a single value (safe, one variable at a time).
    - `SELECT` → can assign multiple variables at once, but if the query returns multiple rows, the **last row value** is taken.

#### 3. NULL Behavior
- If a `SELECT ... INTO @var` query returns no rows, the variable becomes **NULL**.
- This can cause hidden bugs if not explicitly checked.

#### 4. Data Types
- Implicit conversions may **truncate** data silently (e.g., `NVARCHAR(10)` cutting strings).
- Always choose proper data types and lengths to avoid data loss.

#### 5. Performance and Optimization
- Variables **do not have statistics**.
- When used in `WHERE` clauses, the optimizer may generate poor execution plans.
- For complex queries, prefer **stored procedure parameters** over local variables.

#### 6. Table Variables
- Declared with `DECLARE @t TABLE(...)`.
- Stored in **tempdb**, not only in memory.
- Do not maintain statistics → optimizer assumes **1 row**, which can degrade performance on large datasets.
- Use **temporary tables** (`#TempTable`) instead for large or indexed data.

#### 7. Dynamic SQL
- Local variables cannot be directly injected into dynamic SQL.
- Always pass them through `sp_executesql` parameters:
```sql
EXEC sp_executesql @SQL, N’@param INT’, @param = @Variable;
```
- This improves **security** (prevents SQL injection) and **plan reuse**.

#### 8. Iteration and WHILE Loops
- Variables are often used as counters in `WHILE` loops.
- However, set-based solutions are preferred for performance and readability whenever possible.


### Variable Declaration and Assignment
```sql
-- Declare variables
DECLARE @CustomerID INT = 101;
DECLARE @CustomerName NVARCHAR(100);
DECLARE @OrderCount INT;
DECLARE @TotalAmount DECIMAL(10,2) = 0;
DECLARE @CurrentDate DATETIME2 = GETDATE();

-- Assign values using SELECT
SELECT 
    @CustomerName = CustomerName,
    @OrderCount = COUNT(*),
    @TotalAmount = SUM(TotalAmount)
FROM Customers c
INNER JOIN Orders o ON c.CustomerID = o.CustomerID
WHERE c.CustomerID = @CustomerID;

-- Display results
SELECT 
    @CustomerID AS CustomerID,
    @CustomerName AS CustomerName,
    @OrderCount AS OrderCount,
    @TotalAmount AS TotalAmount,
    @CurrentDate AS CurrentDate;
```

**Result:**
```
CustomerID | CustomerName | OrderCount | TotalAmount | CurrentDate
-----------|--------------|------------|-------------|------------------
101        | John Smith   | 5          | 1250.75     | 2024-01-15 14:30:25
```

### Variable Scope and Lifetime
```sql
CREATE PROCEDURE sp_VariableScopeDemo
AS
BEGIN
    -- Variables declared in procedure scope
    DECLARE @ProcedureVariable NVARCHAR(50) = 'Procedure Level';
    DECLARE @Counter INT = 0;
    
    PRINT 'Procedure variable: ' + @ProcedureVariable;
    
    -- Variables in different blocks
    BEGIN
        DECLARE @BlockVariable NVARCHAR(50) = 'Block Level';
        SET @Counter = @Counter + 1;
        
        PRINT 'Block variable: ' + @BlockVariable;
        PRINT 'Counter: ' + CAST(@Counter AS VARCHAR(10));
    END;
    
    -- Block variable is not accessible here
    -- PRINT @BlockVariable; -- This would cause an error
    
    PRINT 'Counter after block: ' + CAST(@Counter AS VARCHAR(10));
END;

EXEC sp_VariableScopeDemo;
```

**Result:**
```
Procedure variable: Procedure Level
Block variable: Block Level
Counter: 1
Counter after block: 1
```

### Dynamic Variable Usage
```sql
CREATE PROCEDURE sp_DynamicVariableDemo
    @TableName NVARCHAR(100),
    @ColumnName NVARCHAR(100),
    @SearchValue NVARCHAR(100)
AS
BEGIN
    DECLARE @SQL NVARCHAR(MAX);
    DECLARE @ParameterDefinition NVARCHAR(500);
    DECLARE @ResultCount INT;
    
    -- Build dynamic SQL with variables
    SET @SQL = 'SELECT @Count = COUNT(*) FROM ' + QUOTENAME(@TableName) + 
               ' WHERE ' + QUOTENAME(@ColumnName) + ' LIKE @SearchValue';
    
    SET @ParameterDefinition = '@Count INT OUTPUT, @SearchValue NVARCHAR(100)';
    
    -- Execute dynamic SQL
    EXEC sp_executesql 
        @SQL,
        @ParameterDefinition,
        @Count = @ResultCount OUTPUT,
        @SearchValue = '%' + @SearchValue + '%';
    
    -- Use the result
    SELECT 
        @TableName AS TableName,
        @ColumnName AS ColumnName,
        @SearchValue AS SearchValue,
        @ResultCount AS MatchCount;
END;

-- Execute with different parameters
EXEC sp_DynamicVariableDemo 
    @TableName = 'Customers',
    @ColumnName = 'CustomerName',
    @SearchValue = 'John';
```

**Result:**
```
TableName | ColumnName  | SearchValue | MatchCount
----------|-------------|-------------|-----------
Customers | CustomerName| John        | 2
```

### Variable Arrays and Collections
```sql
CREATE PROCEDURE sp_ProcessMultipleCustomers
    @CustomerIDs NVARCHAR(MAX) -- Comma-separated list
AS
BEGIN
    DECLARE @CustomerID INT;
    DECLARE @Position INT = 1;
    DECLARE @Delimiter CHAR(1) = ',';
    DECLARE @Results TABLE (
        CustomerID INT,
        CustomerName NVARCHAR(100),
        OrderCount INT,
        TotalAmount DECIMAL(10,2)
    );
    
    -- Parse comma-separated values
    WHILE @Position <= LEN(@CustomerIDs)
    BEGIN
        SET @CustomerID = CAST(
            SUBSTRING(@CustomerIDs, @Position, 
                CHARINDEX(@Delimiter, @CustomerIDs + @Delimiter, @Position) - @Position
            ) AS INT
        );
        
        -- Process each customer
        INSERT INTO @Results
        SELECT 
            c.CustomerID,
            c.CustomerName,
            COUNT(o.OrderID) AS OrderCount,
            ISNULL(SUM(o.TotalAmount), 0) AS TotalAmount
        FROM Customers c
        LEFT JOIN Orders o ON c.CustomerID = o.CustomerID
        WHERE c.CustomerID = @CustomerID
        GROUP BY c.CustomerID, c.CustomerName;
        
        -- Move to next position
        SET @Position = CHARINDEX(@Delimiter, @CustomerIDs, @Position) + 1;
    END;
    
    -- Return results
    SELECT * FROM @Results
    ORDER BY TotalAmount DESC;
END;

-- Execute with multiple customer IDs
EXEC sp_ProcessMultipleCustomers @CustomerIDs = '101,102,103,104';
```

**Result:**
```
CustomerID | CustomerName | OrderCount | TotalAmount
-----------|--------------|------------|------------
102        | Jane Doe     | 3          | 1800.00
101        | John Smith   | 5          | 1250.75
103        | Bob Johnson  | 2          | 450.50
104        | Alice Brown  | 0          | 0.00
```

## 58. How do you use control-of-flow language (IF...ELSE, WHILE) in T-SQL scripts?

### IF...ELSE Statements
```sql
CREATE PROCEDURE sp_CustomerAnalysis
    @CustomerID INT
AS
BEGIN
    DECLARE @OrderCount INT;
    DECLARE @TotalAmount DECIMAL(10,2);
    DECLARE @CustomerType NVARCHAR(20);
    DECLARE @Discount DECIMAL(5,2) = 0;
    
    -- Get customer data
    SELECT 
        @OrderCount = COUNT(*),
        @TotalAmount = SUM(TotalAmount)
    FROM Orders
    WHERE CustomerID = @CustomerID;
    
    -- Determine customer type and discount
    IF @OrderCount = 0
    BEGIN
        SET @CustomerType = 'New Customer';
        SET @Discount = 0;
        PRINT 'Welcome! No orders yet.';
    END
    ELSE IF @OrderCount < 5
    BEGIN
        SET @CustomerType = 'Regular Customer';
        SET @Discount = 5.00;
        PRINT 'Regular customer with ' + CAST(@OrderCount AS VARCHAR(10)) + ' orders.';
    END
    ELSE IF @OrderCount < 10
    BEGIN
        SET @CustomerType = 'Frequent Customer';
        SET @Discount = 10.00;
        PRINT 'Frequent customer with ' + CAST(@OrderCount AS VARCHAR(10)) + ' orders.';
    END
    ELSE
    BEGIN
        SET @CustomerType = 'VIP Customer';
        SET @Discount = 15.00;
        PRINT 'VIP customer with ' + CAST(@OrderCount AS VARCHAR(10)) + ' orders!';
    END;
    
    -- Additional logic based on total amount
    IF @TotalAmount > 5000
    BEGIN
        SET @Discount = @Discount + 5.00;
        PRINT 'Bonus discount for high-value customer!';
    END;
    
    -- Return results
    SELECT 
        @CustomerID AS CustomerID,
        @OrderCount AS OrderCount,
        @TotalAmount AS TotalAmount,
        @CustomerType AS CustomerType,
        @Discount AS DiscountPercent;
END;

-- Test with different customers
EXEC sp_CustomerAnalysis @CustomerID = 101; -- Regular customer
EXEC sp_CustomerAnalysis @CustomerID = 102; -- VIP customer
EXEC sp_CustomerAnalysis @CustomerID = 999; -- New customer
```

**Results:**
```
-- Customer 101 (Regular):
CustomerID | OrderCount | TotalAmount | CustomerType    | DiscountPercent
-----------|------------|-------------|-----------------|----------------
101        | 3          | 1250.75     | Regular Customer| 5.00

-- Customer 102 (VIP):
CustomerID | OrderCount | TotalAmount | CustomerType | DiscountPercent
-----------|------------|-------------|--------------|----------------
102        | 12         | 8500.00     | VIP Customer | 20.00
```

### WHILE Loops
```sql
CREATE PROCEDURE sp_GenerateOrderNumbers
    @StartNumber INT,
    @Count INT
AS
BEGIN
    DECLARE @Counter INT = 0;
    DECLARE @CurrentNumber INT = @StartNumber;
    DECLARE @Results TABLE (
        OrderNumber NVARCHAR(20),
        GeneratedDate DATETIME2
    );
    
    -- Generate order numbers using WHILE loop
    WHILE @Counter < @Count
    BEGIN
        -- Generate order number with prefix
        DECLARE @OrderNumber NVARCHAR(20) = 'ORD-' + 
            RIGHT('000000' + CAST(@CurrentNumber AS VARCHAR(10)), 6);
        
        -- Insert into results
        INSERT INTO @Results (OrderNumber, GeneratedDate)
        VALUES (@OrderNumber, GETDATE());
        
        -- Increment counters
        SET @Counter = @Counter + 1;
        SET @CurrentNumber = @CurrentNumber + 1;
        
        -- Optional: Add delay for demonstration
        WAITFOR DELAY '00:00:00.001';
    END;
    
    -- Return generated order numbers
    SELECT * FROM @Results;
    
    -- Summary
    SELECT 
        COUNT(*) AS TotalGenerated,
        MIN(OrderNumber) AS FirstOrder,
        MAX(OrderNumber) AS LastOrder
    FROM @Results;
END;

-- Generate 5 order numbers starting from 1001
EXEC sp_GenerateOrderNumbers @StartNumber = 1001, @Count = 5;
```

**Result:**
```
OrderNumber | GeneratedDate
------------|------------------
ORD-001001  | 2024-01-15 14:30:25.123
ORD-001002  | 2024-01-15 14:30:25.124
ORD-001003  | 2024-01-15 14:30:25.125
ORD-001004  | 2024-01-15 14:30:25.126
ORD-001005  | 2024-01-15 14:30:25.127

TotalGenerated | FirstOrder | LastOrder
---------------|------------|----------
5              | ORD-001001 | ORD-001005
```

### Nested Control Structures
```sql
CREATE PROCEDURE sp_ProcessOrdersByStatus
AS
BEGIN
    DECLARE @Status NVARCHAR(20);
    DECLARE @OrderCount INT;
    DECLARE @TotalAmount DECIMAL(10,2);
    DECLARE @ProcessedCount INT = 0;
    
    -- Cursor to iterate through different statuses
    DECLARE status_cursor CURSOR FOR
    SELECT DISTINCT Status FROM Orders;
    
    OPEN status_cursor;
    FETCH NEXT FROM status_cursor INTO @Status;
    
    WHILE @@FETCH_STATUS = 0
    BEGIN
        -- Get count and total for current status
        SELECT 
            @OrderCount = COUNT(*),
            @TotalAmount = SUM(TotalAmount)
        FROM Orders
        WHERE Status = @Status;
        
        -- Process based on status
        IF @Status = 'Pending'
        BEGIN
            PRINT 'Processing ' + CAST(@OrderCount AS VARCHAR(10)) + ' pending orders...';
            -- Simulate processing
            SET @ProcessedCount = @ProcessedCount + @OrderCount;
        END
        ELSE IF @Status = 'Completed'
        BEGIN
            PRINT 'Found ' + CAST(@OrderCount AS VARCHAR(10)) + ' completed orders totaling $' + 
                  CAST(@TotalAmount AS VARCHAR(20));
        END
        ELSE IF @Status = 'Cancelled'
        BEGIN
            PRINT 'Skipping ' + CAST(@OrderCount AS VARCHAR(10)) + ' cancelled orders.';
        END
        ELSE
        BEGIN
            PRINT 'Unknown status: ' + @Status + ' with ' + CAST(@OrderCount AS VARCHAR(10)) + ' orders.';
        END;
        
        FETCH NEXT FROM status_cursor INTO @Status;
    END;
    
    CLOSE status_cursor;
    DEALLOCATE status_cursor;
    
    PRINT 'Total processed orders: ' + CAST(@ProcessedCount AS VARCHAR(10));
END;

EXEC sp_ProcessOrdersByStatus;
```

**Result:**
```
Processing 15 pending orders...
Found 25 completed orders totaling $12500.75
Skipping 3 cancelled orders.
Total processed orders: 15
```

## 59. Can you provide an example of a T-SQL CASE statement?

Control-of-flow language in T-SQL allows you to control the order of statement execution.

### Key Constructs
- **IF...ELSE** → conditional logic
- **WHILE** → loops with `BREAK` and `CONTINUE`
- **RETURN** → exit procedure with status code
- **GOTO** → jump to a label (rarely used)

### Simple CASE Statement
```sql
-- Categorize products by price
SELECT 
    ProductName,
    UnitPrice,
    CASE 
        WHEN UnitPrice < 50 THEN 'Budget'
        WHEN UnitPrice < 200 THEN 'Standard'
        WHEN UnitPrice < 500 THEN 'Premium'
        ELSE 'Luxury'
    END AS PriceCategory,
    CASE 
        WHEN UnitPrice < 50 THEN 'Low'
        WHEN UnitPrice < 200 THEN 'Medium'
        WHEN UnitPrice < 500 THEN 'High'
        ELSE 'Very High'
    END AS PriceLevel
FROM Products
ORDER BY UnitPrice DESC;
```

**Result:**
```
ProductName | UnitPrice | PriceCategory | PriceLevel
------------|-----------|---------------|------------
Laptop Pro  | 1299.99   | Luxury        | Very High
Gaming PC   | 899.99    | Premium       | High
Tablet      | 299.99    | Premium       | High
Smartphone  | 199.99    | Standard      | Medium
Mouse       | 25.99     | Budget        | Low
```

### Searched CASE Statement
```sql
-- Complex business logic with CASE
SELECT 
    c.CustomerName,
    COUNT(o.OrderID) AS OrderCount,
    SUM(o.TotalAmount) AS TotalSpent,
    CASE 
        WHEN COUNT(o.OrderID) = 0 THEN 'No Orders'
        WHEN COUNT(o.OrderID) < 3 THEN 'New Customer'
        WHEN COUNT(o.OrderID) < 10 THEN 'Regular Customer'
        WHEN COUNT(o.OrderID) < 20 THEN 'Frequent Customer'
        ELSE 'VIP Customer'
    END AS CustomerSegment,
    CASE 
        WHEN SUM(o.TotalAmount) IS NULL THEN 'No Spending'
        WHEN SUM(o.TotalAmount) < 500 THEN 'Low Value'
        WHEN SUM(o.TotalAmount) < 2000 THEN 'Medium Value'
        WHEN SUM(o.TotalAmount) < 5000 THEN 'High Value'
        ELSE 'Premium Value'
    END AS ValueSegment,
    CASE 
        WHEN COUNT(o.OrderID) >= 10 AND SUM(o.TotalAmount) >= 2000 THEN 'Gold Member'
        WHEN COUNT(o.OrderID) >= 5 AND SUM(o.TotalAmount) >= 1000 THEN 'Silver Member'
        WHEN COUNT(o.OrderID) >= 1 THEN 'Bronze Member'
        ELSE 'Prospect'
    END AS MembershipLevel
FROM Customers c
LEFT JOIN Orders o ON c.CustomerID = o.CustomerID
GROUP BY c.CustomerID, c.CustomerName
ORDER BY TotalSpent DESC;
```

**Result:**
```
CustomerName | OrderCount | TotalSpent | CustomerSegment | ValueSegment | MembershipLevel
-------------|------------|------------|-----------------|--------------|----------------
John Smith   | 12         | 8500.00    | Frequent Customer| Premium Value| Gold Member
Jane Doe     | 8          | 3200.00    | Regular Customer| High Value   | Silver Member
Bob Johnson  | 3          | 750.00     | New Customer    | Medium Value | Bronze Member
Alice Brown  | 0          | NULL       | No Orders       | No Spending  | Prospect
```

### CASE in UPDATE Statement
```sql
-- Update customer status based on order history
UPDATE Customers
SET Status = CASE 
    WHEN LastOrderDate IS NULL THEN 'Inactive'
    WHEN LastOrderDate < DATEADD(MONTH, -6, GETDATE()) THEN 'Dormant'
    WHEN LastOrderDate < DATEADD(MONTH, -3, GETDATE()) THEN 'At Risk'
    WHEN LastOrderDate < DATEADD(MONTH, -1, GETDATE()) THEN 'Active'
    ELSE 'Very Active'
END,
Priority = CASE 
    WHEN LastOrderDate IS NULL THEN 5
    WHEN LastOrderDate < DATEADD(MONTH, -6, GETDATE()) THEN 4
    WHEN LastOrderDate < DATEADD(MONTH, -3, GETDATE()) THEN 3
    WHEN LastOrderDate < DATEADD(MONTH, -1, GETDATE()) THEN 2
    ELSE 1
END;

-- View updated results
SELECT 
    CustomerName,
    LastOrderDate,
    Status,
    Priority,
    CASE Priority
        WHEN 1 THEN 'High'
        WHEN 2 THEN 'Medium-High'
        WHEN 3 THEN 'Medium'
        WHEN 4 THEN 'Low'
        WHEN 5 THEN 'Very Low'
    END AS PriorityDescription
FROM Customers
ORDER BY Priority, LastOrderDate DESC;
```

**Result:**
```
CustomerName | LastOrderDate | Status      | Priority | PriorityDescription
-------------|---------------|-------------|----------|-------------------
John Smith   | 2024-01-10    | Very Active | 1        | High
Jane Doe     | 2023-12-15    | Active      | 2        | Medium-High
Bob Johnson  | 2023-10-20    | At Risk     | 3        | Medium
Alice Brown  | 2023-06-01    | Dormant     | 4        | Low
```

### CASE in ORDER BY Clause
```sql
-- Custom sorting with CASE
SELECT 
    ProductName,
    CategoryName,
    UnitPrice,
    StockQuantity,
    CASE 
        WHEN StockQuantity = 0 THEN 'Out of Stock'
        WHEN StockQuantity < 10 THEN 'Low Stock'
        WHEN StockQuantity < 50 THEN 'Medium Stock'
        ELSE 'In Stock'
    END AS StockStatus
FROM Products p
INNER JOIN Categories c ON p.CategoryID = c.CategoryID
ORDER BY 
    CASE 
        WHEN StockQuantity = 0 THEN 1  -- Out of stock first
        WHEN StockQuantity < 10 THEN 2 -- Low stock second
        ELSE 3                         -- In stock last
    END,
    UnitPrice DESC;
```

**Result:**
```
ProductName | CategoryName | UnitPrice | StockQuantity | StockStatus
------------|--------------|-----------|---------------|-------------
Gaming PC   | Electronics  | 899.99    | 0             | Out of Stock
Laptop Pro  | Electronics  | 1299.99   | 2             | Low Stock
Tablet      | Electronics  | 299.99    | 5             | Low Stock
Smartphone  | Electronics  | 199.99    | 25            | Medium Stock
Mouse       | Accessories  | 25.99     | 100           | In Stock
```

## 60. Explain the TRY...CATCH construct in T-SQL error handling

### Basic TRY-CATCH Structure
```sql
CREATE PROCEDURE sp_SafeDataInsert
    @CustomerName NVARCHAR(100),
    @Email NVARCHAR(100),
    @Phone NVARCHAR(20)
AS
BEGIN
    SET NOCOUNT ON;
    
    BEGIN TRY
        -- Validate input
        IF @CustomerName IS NULL OR LEN(TRIM(@CustomerName)) = 0
        BEGIN
            RAISERROR('Customer name cannot be empty', 16, 1);
        END;
        
        -- Insert customer
        INSERT INTO Customers (CustomerName, Email, Phone, CreatedDate)
        VALUES (@CustomerName, @Email, @Phone, GETDATE());
        
        -- Get the new customer ID
        DECLARE @NewCustomerID INT = SCOPE_IDENTITY();
        
        -- Log successful insertion
        INSERT INTO AuditLog (TableName, Operation, RecordID, UserID, Timestamp)
        VALUES ('Customers', 'INSERT', @NewCustomerID, SUSER_SNAME(), GETDATE());
        
        SELECT 
            @NewCustomerID AS CustomerID,
            'Customer inserted successfully' AS Status;
            
    END TRY
    BEGIN CATCH
        -- Handle the error
        DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
        DECLARE @ErrorSeverity INT = ERROR_SEVERITY();
        DECLARE @ErrorState INT = ERROR_STATE();
        DECLARE @ErrorNumber INT = ERROR_NUMBER();
        DECLARE @ErrorLine INT = ERROR_LINE();
        DECLARE @ErrorProcedure NVARCHAR(128) = ERROR_PROCEDURE();
        
        -- Log error details
        INSERT INTO ErrorLog (
            ErrorNumber, ErrorSeverity, ErrorState, ErrorProcedure, 
            ErrorLine, ErrorMessage, ErrorTime, UserName
        )
        VALUES (
            @ErrorNumber, @ErrorSeverity, @ErrorState, @ErrorProcedure,
            @ErrorLine, @ErrorMessage, GETDATE(), SUSER_SNAME()
        );
        
        -- Return user-friendly error
        SELECT 
            NULL AS CustomerID,
            'Error: ' + @ErrorMessage AS Status;
        
        -- Re-raise error if needed
        -- RAISERROR(@ErrorMessage, @ErrorSeverity, @ErrorState);
    END CATCH;
END;

-- Test with valid data
EXEC sp_SafeDataInsert 
    @CustomerName = 'John Smith',
    @Email = 'john@example.com',
    @Phone = '555-1234';

-- Test with invalid data
EXEC sp_SafeDataInsert 
    @CustomerName = '',
    @Email = 'invalid-email',
    @Phone = '555-5678';
```

**Results:**
```
-- Valid data:
CustomerID | Status
-----------|------------------
1001       | Customer inserted successfully

-- Invalid data:
CustomerID | Status
-----------|------------------
NULL       | Error: Customer name cannot be empty
```

### TRY-CATCH with Transactions
```sql
CREATE PROCEDURE sp_TransferFundsWithErrorHandling
    @FromAccount INT,
    @ToAccount INT,
    @Amount DECIMAL(10,2)
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @TransactionID INT;
    DECLARE @Success BIT = 0;
    
    BEGIN TRY
        -- Validate parameters
        IF @Amount <= 0
        BEGIN
            RAISERROR('Transfer amount must be greater than zero', 16, 1);
        END;
        
        IF @FromAccount = @ToAccount
        BEGIN
            RAISERROR('Cannot transfer to the same account', 16, 1);
        END;
        
        -- Start transaction
        BEGIN TRANSACTION;
        
        -- Check if accounts exist
        IF NOT EXISTS (SELECT 1 FROM Accounts WHERE AccountID = @FromAccount)
        BEGIN
            RAISERROR('Source account does not exist', 16, 1);
        END;
        
        IF NOT EXISTS (SELECT 1 FROM Accounts WHERE AccountID = @ToAccount)
        BEGIN
            RAISERROR('Destination account does not exist', 16, 1);
        END;
        
        -- Check sufficient funds
        DECLARE @CurrentBalance DECIMAL(10,2);
        SELECT @CurrentBalance = Balance
        FROM Accounts
        WHERE AccountID = @FromAccount;
        
        IF @CurrentBalance < @Amount
        BEGIN
            RAISERROR('Insufficient funds. Available: $%.2f, Required: $%.2f', 16, 1, 
                     @CurrentBalance, @Amount);
        END;
        
        -- Perform transfer
        UPDATE Accounts
        SET Balance = Balance - @Amount
        WHERE AccountID = @FromAccount;
        
        UPDATE Accounts
        SET Balance = Balance + @Amount
        WHERE AccountID = @ToAccount;
        
        -- Record transaction
        INSERT INTO Transactions (FromAccount, ToAccount, Amount, TransactionDate, Status)
        VALUES (@FromAccount, @ToAccount, @Amount, GETDATE(), 'Completed');
        
        SET @TransactionID = SCOPE_IDENTITY();
        SET @Success = 1;
        
        -- Commit transaction
        COMMIT TRANSACTION;
        
        -- Log success
        INSERT INTO AuditLog (TableName, Operation, RecordID, UserID, Timestamp)
        VALUES ('Transactions', 'INSERT', @TransactionID, SUSER_SNAME(), GETDATE());
        
        SELECT 
            @TransactionID AS TransactionID,
            @Success AS Success,
            'Transfer completed successfully' AS Message;
            
    END TRY
    BEGIN CATCH
        -- Rollback transaction if active
        IF @@TRANCOUNT > 0
        BEGIN
            ROLLBACK TRANSACTION;
        END;
        
        -- Log error
        INSERT INTO ErrorLog (
            ErrorNumber, ErrorSeverity, ErrorState, ErrorProcedure,
            ErrorLine, ErrorMessage, ErrorTime, UserName
        )
        VALUES (
            ERROR_NUMBER(), ERROR_SEVERITY(), ERROR_STATE(), ERROR_PROCEDURE(),
            ERROR_LINE(), ERROR_MESSAGE(), GETDATE(), SUSER_SNAME()
        );
        
        -- Return error information
        SELECT 
            NULL AS TransactionID,
            0 AS Success,
            'Transfer failed: ' + ERROR_MESSAGE() AS Message;
    END CATCH;
END;

-- Test successful transfer
EXEC sp_TransferFundsWithErrorHandling 
    @FromAccount = 1001,
    @ToAccount = 1002,
    @Amount = 500.00;

-- Test insufficient funds
EXEC sp_TransferFundsWithErrorHandling 
    @FromAccount = 1001,
    @ToAccount = 1002,
    @Amount = 10000.00;
```

**Results:**
```
-- Successful transfer:
TransactionID | Success | Message
--------------|---------|------------------
1001          | 1       | Transfer completed successfully

-- Insufficient funds:
TransactionID | Success | Message
--------------|---------|------------------
NULL          | 0       | Transfer failed: Insufficient funds. Available: $1500.00, Required: $10000.00
```

### Nested TRY-CATCH Blocks
```sql
CREATE PROCEDURE sp_ComplexDataProcessing
    @CustomerID INT
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @ProcessedOrders INT = 0;
    DECLARE @TotalAmount DECIMAL(10,2) = 0;
    
    BEGIN TRY
        -- Outer try block
        PRINT 'Starting data processing for customer: ' + CAST(@CustomerID AS VARCHAR(10));
        
        -- Process orders
        BEGIN TRY
            -- Inner try block for order processing
            UPDATE Orders
            SET Status = 'Processed',
                ProcessedDate = GETDATE()
            WHERE CustomerID = @CustomerID 
                AND Status = 'Pending';
            
            SET @ProcessedOrders = @@ROWCOUNT;
            PRINT 'Processed ' + CAST(@ProcessedOrders AS VARCHAR(10)) + ' orders';
            
        END TRY
        BEGIN CATCH
            -- Handle order processing errors
            PRINT 'Order processing failed: ' + ERROR_MESSAGE();
            SET @ProcessedOrders = 0;
        END CATCH;
        
        -- Calculate totals
        BEGIN TRY
            -- Inner try block for calculations
            SELECT @TotalAmount = SUM(TotalAmount)
            FROM Orders
            WHERE CustomerID = @CustomerID 
                AND Status = 'Processed';
            
            PRINT 'Total amount: $' + CAST(@TotalAmount AS VARCHAR(20));
            
        END TRY
        BEGIN CATCH
            -- Handle calculation errors
            PRINT 'Calculation failed: ' + ERROR_MESSAGE();
            SET @TotalAmount = 0;
        END CATCH;
        
        -- Update customer record
        BEGIN TRY
            -- Inner try block for customer update
            UPDATE Customers
            SET LastProcessedDate = GETDATE(),
                ProcessedOrderCount = @ProcessedOrders,
                TotalProcessedAmount = @TotalAmount
            WHERE CustomerID = @CustomerID;
            
            PRINT 'Customer record updated successfully';
            
        END TRY
        BEGIN CATCH
            -- Handle customer update errors
            PRINT 'Customer update failed: ' + ERROR_MESSAGE();
        END CATCH;
        
        -- Final success message
        PRINT 'Data processing completed successfully';
        
    END TRY
    BEGIN CATCH
        -- Outer catch block
        PRINT 'Critical error in data processing: ' + ERROR_MESSAGE();
        
        -- Log critical error
        INSERT INTO ErrorLog (
            ErrorNumber, ErrorSeverity, ErrorState, ErrorProcedure,
            ErrorLine, ErrorMessage, ErrorTime, UserName
        )
        VALUES (
            ERROR_NUMBER(), ERROR_SEVERITY(), ERROR_STATE(), ERROR_PROCEDURE(),
            ERROR_LINE(), ERROR_MESSAGE(), GETDATE(), SUSER_SNAME()
        );
    END CATCH;
    
    -- Return summary
    SELECT 
        @CustomerID AS CustomerID,
        @ProcessedOrders AS ProcessedOrders,
        @TotalAmount AS TotalAmount;
END;

-- Test the procedure
EXEC sp_ComplexDataProcessing @CustomerID = 101;
```

**Result:**
```
Starting data processing for customer: 101
Processed 3 orders
Total amount: $1250.75
Customer record updated successfully
Data processing completed successfully

CustomerID | ProcessedOrders | TotalAmount
-----------|-----------------|------------
101        | 3               | 1250.75
```

## 61. What are the implications of using CURSORs in T-SQL?

### Definition
A **cursor** in T-SQL is a database object that allows you to **process query results row by row**, but they can have significant performance implications. 
Instead of handling all rows in a single set-based operation, a cursor fetches one row at a time into variables for further logic.

### How it Works
1. **Declare** the cursor with a query.
2. **Open** the cursor — SQL Server prepares the result set.
3. **Fetch** rows one by one into variables.
4. **Process** each row individually (e.g., update, log, conditional logic).
5. **Close and deallocate** the cursor to free resources.

### Analogy with Python Generators
- Like a **Python generator (`yield`)**, a cursor provides results **one at a time**.
- Both are useful when you don’t want (or cannot) load/process all results at once.

### When to Use
- When complex **per-row business logic** is required.
- When integrating with **external systems** (logging, API calls, etc.).
- For operations that **cannot be expressed** with standard set-based SQL (`JOIN`, `CASE`, `GROUP BY`).

### Drawbacks
- **Slower** than set-based queries.
- Higher **resource usage** (memory, locks).
- Should be a **last resort** in performance-critical systems.

### Basic CURSOR Example
```sql
CREATE PROCEDURE sp_ProcessOrdersWithCursor
    @CustomerID INT
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @OrderID INT;
    DECLARE @OrderDate DATETIME2;
    DECLARE @TotalAmount DECIMAL(10,2);
    DECLARE @ProcessedCount INT = 0;
    DECLARE @TotalProcessed DECIMAL(10,2) = 0;
    
    -- Declare cursor
    DECLARE order_cursor CURSOR FOR
    SELECT OrderID, OrderDate, TotalAmount
    FROM Orders
    WHERE CustomerID = @CustomerID
        AND Status = 'Pending'
    ORDER BY OrderDate;
    
    -- Open cursor
    OPEN order_cursor;
    
    -- Fetch first row
    FETCH NEXT FROM order_cursor INTO @OrderID, @OrderDate, @TotalAmount;
    
    -- Process each row
    WHILE @@FETCH_STATUS = 0
    BEGIN
        -- Process the order
        UPDATE Orders
        SET Status = 'Processing',
            ProcessedDate = GETDATE()
        WHERE OrderID = @OrderID;
        
        -- Log processing
        INSERT INTO OrderProcessingLog (OrderID, ProcessedDate, ProcessedBy)
        VALUES (@OrderID, GETDATE(), SUSER_SNAME());
        
        -- Update counters
        SET @ProcessedCount = @ProcessedCount + 1;
        SET @TotalProcessed = @TotalProcessed + @TotalAmount;
        
        -- Fetch next row
        FETCH NEXT FROM order_cursor INTO @OrderID, @OrderDate, @TotalAmount;
    END;
    
    -- Clean up cursor
    CLOSE order_cursor;
    DEALLOCATE order_cursor;
    
    -- Return summary
    SELECT 
        @ProcessedCount AS ProcessedOrders,
        @TotalProcessed AS TotalAmount;
END;

-- Execute procedure
EXEC sp_ProcessOrdersWithCursor @CustomerID = 101;
```

**Result:**
```
ProcessedOrders | TotalAmount
----------------|------------
3               | 1250.75
```

### CURSOR Performance Implications
```sql
-- BAD: Using cursor for simple operations
CREATE PROCEDURE sp_BadCursorExample
AS
BEGIN
    DECLARE @CustomerID INT;
    DECLARE @TotalAmount DECIMAL(10,2);
    DECLARE @Results TABLE (
        CustomerID INT,
        TotalAmount DECIMAL(10,2)
    );
    
    -- Declare cursor
    DECLARE customer_cursor CURSOR FOR
    SELECT CustomerID FROM Customers;
    
    OPEN customer_cursor;
    FETCH NEXT FROM customer_cursor INTO @CustomerID;
    
    WHILE @@FETCH_STATUS = 0
    BEGIN
        -- Calculate total for each customer
        SELECT @TotalAmount = SUM(TotalAmount)
        FROM Orders
        WHERE CustomerID = @CustomerID;
        
        INSERT INTO @Results (CustomerID, TotalAmount)
        VALUES (@CustomerID, ISNULL(@TotalAmount, 0));
        
        FETCH NEXT FROM customer_cursor INTO @CustomerID;
    END;
    
    CLOSE customer_cursor;
    DEALLOCATE customer_cursor;
    
    SELECT * FROM @Results;
END;

-- GOOD: Set-based approach
CREATE PROCEDURE sp_GoodSetBasedExample
AS
BEGIN
    SELECT 
        c.CustomerID,
        ISNULL(SUM(o.TotalAmount), 0) AS TotalAmount
    FROM Customers c
    LEFT JOIN Orders o ON c.CustomerID = o.CustomerID
    GROUP BY c.CustomerID;
END;

-- Compare performance
EXEC sp_BadCursorExample;  -- Slow cursor approach
EXEC sp_GoodSetBasedExample;  -- Fast set-based approach
```

**Results:**
```
-- Both return same data, but cursor is much slower:
CustomerID | TotalAmount
-----------|------------
101        | 1250.75
102        | 1800.00
103        | 450.50
```

### When to Use CURSORs
```sql
-- GOOD: When you need complex row-by-row logic
CREATE PROCEDURE sp_ComplexOrderProcessing
AS
BEGIN
    DECLARE @OrderID INT;
    DECLARE @CustomerID INT;
    DECLARE @OrderDate DATETIME2;
    DECLARE @TotalAmount DECIMAL(10,2);
    DECLARE @CustomerType NVARCHAR(20);
    DECLARE @Discount DECIMAL(5,2);
    DECLARE @FinalAmount DECIMAL(10,2);
    
    DECLARE order_cursor CURSOR FOR
    SELECT o.OrderID, o.CustomerID, o.OrderDate, o.TotalAmount
    FROM Orders o
    WHERE o.Status = 'Pending'
    ORDER BY o.OrderDate;
    
    OPEN order_cursor;
    FETCH NEXT FROM order_cursor INTO @OrderID, @CustomerID, @OrderDate, @TotalAmount;
    
    WHILE @@FETCH_STATUS = 0
    BEGIN
        -- Complex business logic for each order
        SELECT @CustomerType = 
            CASE 
                WHEN COUNT(*) < 5 THEN 'Regular'
                WHEN COUNT(*) < 10 THEN 'Frequent'
                ELSE 'VIP'
            END
        FROM Orders
        WHERE CustomerID = @CustomerID;
        
        -- Calculate discount based on customer type and order amount
        SET @Discount = 
            CASE @CustomerType
                WHEN 'Regular' THEN 
                    CASE WHEN @TotalAmount > 1000 THEN 5.0 ELSE 0.0 END
                WHEN 'Frequent' THEN 
                    CASE WHEN @TotalAmount > 500 THEN 10.0 ELSE 5.0 END
                WHEN 'VIP' THEN 
                    CASE WHEN @TotalAmount > 2000 THEN 20.0 
                         WHEN @TotalAmount > 1000 THEN 15.0 
                         ELSE 10.0 END
            END;
        
        SET @FinalAmount = @TotalAmount * (1 - @Discount / 100);
        
        -- Update order with calculated values
        UPDATE Orders
        SET CustomerType = @CustomerType,
            DiscountPercent = @Discount,
            FinalAmount = @FinalAmount,
            Status = 'Processed'
        WHERE OrderID = @OrderID;
        
        FETCH NEXT FROM order_cursor INTO @OrderID, @CustomerID, @OrderDate, @TotalAmount;
    END;
    
    CLOSE order_cursor;
    DEALLOCATE order_cursor;
END;
```

## 62. What is a transaction in the context of T-SQL?

### Transaction Basics
A transaction is a sequence of operations that are treated as a single unit of work.

### ACID Properties Example
```sql
CREATE PROCEDURE sp_TransferMoney
    @FromAccount INT,
    @ToAccount INT,
    @Amount DECIMAL(10,2)
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Start transaction
    BEGIN TRANSACTION TransferTransaction;
    
    BEGIN TRY
        -- Atomicity: All operations succeed or all fail
        -- Consistency: Database remains in valid state
        
        -- Check source account balance
        DECLARE @SourceBalance DECIMAL(10,2);
        SELECT @SourceBalance = Balance
        FROM Accounts
        WHERE AccountID = @FromAccount;
        
        IF @SourceBalance < @Amount
        BEGIN
            RAISERROR('Insufficient funds', 16, 1);
        END;
        
        -- Debit from source account
        UPDATE Accounts
        SET Balance = Balance - @Amount,
            LastTransactionDate = GETDATE()
        WHERE AccountID = @FromAccount;
        
        -- Credit to destination account
        UPDATE Accounts
        SET Balance = Balance + @Amount,
            LastTransactionDate = GETDATE()
        WHERE AccountID = @ToAccount;
        
        -- Record transaction
        INSERT INTO Transactions (FromAccount, ToAccount, Amount, TransactionDate, Status)
        VALUES (@FromAccount, @ToAccount, @Amount, GETDATE(), 'Completed');
        
        -- Isolation: Other transactions see consistent state
        -- Durability: Changes are permanent after commit
        
        -- Commit transaction
        COMMIT TRANSACTION TransferTransaction;
        
        SELECT 'Transfer completed successfully' AS Status;
        
    END TRY
    BEGIN CATCH
        -- Rollback on any error
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION TransferTransaction;
        
        -- Re-raise error
        DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
        RAISERROR(@ErrorMessage, 16, 1);
    END CATCH;
END;

-- Test successful transfer
EXEC sp_TransferMoney @FromAccount = 1001, @ToAccount = 1002, @Amount = 500.00;

-- Test insufficient funds
EXEC sp_TransferMoney @FromAccount = 1001, @ToAccount = 1002, @Amount = 10000.00;
```

**Results:**
```
-- Successful transfer:
Status
----------------------------
Transfer completed successfully

-- Insufficient funds:
Msg 50000, Level 16, State 1, Procedure sp_TransferMoney, Line 25
Insufficient funds
```

### Nested Transactions
```sql
CREATE PROCEDURE sp_ComplexOrderProcessing
    @CustomerID INT,
    @ProductID INT,
    @Quantity INT
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Outer transaction
    BEGIN TRANSACTION OrderProcessing;
    
    BEGIN TRY
        -- Create order
        INSERT INTO Orders (CustomerID, OrderDate, Status)
        VALUES (@CustomerID, GETDATE(), 'Pending');
        
        DECLARE @OrderID INT = SCOPE_IDENTITY();
        
        -- Inner transaction for inventory
        BEGIN TRANSACTION InventoryUpdate;
        
        BEGIN TRY
            -- Check and update inventory
            DECLARE @CurrentStock INT;
            SELECT @CurrentStock = StockQuantity
            FROM Products
            WHERE ProductID = @ProductID;
            
            IF @CurrentStock < @Quantity
            BEGIN
                RAISERROR('Insufficient stock', 16, 1);
            END;
            
            UPDATE Products
            SET StockQuantity = StockQuantity - @Quantity
            WHERE ProductID = @ProductID;
            
            -- Commit inner transaction
            COMMIT TRANSACTION InventoryUpdate;
            
        END TRY
        BEGIN CATCH
            -- Rollback inner transaction
            IF @@TRANCOUNT > 1
                ROLLBACK TRANSACTION InventoryUpdate;
            
            RAISERROR('Inventory update failed', 16, 1);
        END CATCH;
        
        -- Add order details
        INSERT INTO OrderDetails (OrderID, ProductID, Quantity, UnitPrice)
        SELECT @OrderID, @ProductID, @Quantity, UnitPrice
        FROM Products
        WHERE ProductID = @ProductID;
        
        -- Update order status
        UPDATE Orders
        SET Status = 'Completed',
            TotalAmount = @Quantity * (SELECT UnitPrice FROM Products WHERE ProductID = @ProductID)
        WHERE OrderID = @OrderID;
        
        -- Commit outer transaction
        COMMIT TRANSACTION OrderProcessing;
        
        SELECT @OrderID AS OrderID, 'Order processed successfully' AS Status;
        
    END TRY
    BEGIN CATCH
        -- Rollback outer transaction
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION OrderProcessing;
        
        DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE();
        RAISERROR(@ErrorMessage, 16, 1);
    END CATCH;
END;

-- Test order processing
EXEC sp_ComplexOrderProcessing @CustomerID = 101, @ProductID = 1001, @Quantity = 2;
```

**Result:**
```
OrderID | Status
--------|----------------------------
1001    | Order processed successfully
```

## 63. How do you use the BEGIN TRANSACTION, COMMIT, and ROLLBACK statements?

### Basic Transaction Control
```sql
-- Example 1: Simple transaction
BEGIN TRANSACTION;

BEGIN TRY
    -- Multiple operations
    INSERT INTO Customers (CustomerName, Email) VALUES ('John Doe', 'john@email.com');
    INSERT INTO Orders (CustomerID, OrderDate) VALUES (SCOPE_IDENTITY(), GETDATE());
    
    -- Commit if all operations succeed
    COMMIT TRANSACTION;
    PRINT 'Transaction committed successfully';
    
END TRY
BEGIN CATCH
    -- Rollback on any error
    IF @@TRANCOUNT > 0
        ROLLBACK TRANSACTION;
    
    PRINT 'Transaction rolled back: ' + ERROR_MESSAGE();
END CATCH;
```

### Named Transactions
```sql
-- Example 2: Named transaction
BEGIN TRANSACTION CustomerUpdate;

BEGIN TRY
    -- Update customer information
    UPDATE Customers
    SET LastOrderDate = GETDATE(),
        OrderCount = OrderCount + 1
    WHERE CustomerID = 101;
    
    -- Update related orders
    UPDATE Orders
    SET Status = 'Updated'
    WHERE CustomerID = 101 AND Status = 'Pending';
    
    -- Commit named transaction
    COMMIT TRANSACTION CustomerUpdate;
    
    SELECT 'Customer update completed' AS Status;
    
END TRY
BEGIN CATCH
    -- Rollback named transaction
    IF @@TRANCOUNT > 0
        ROLLBACK TRANSACTION CustomerUpdate;
    
    SELECT 'Customer update failed: ' + ERROR_MESSAGE() AS Status;
END CATCH;
```

### Savepoints in Transactions
A **savepoint** in T-SQL is a marker within a transaction that allows you to perform a **partial rollback**.  
Instead of rolling back the entire transaction, you can roll back only to the last defined savepoint, preserving earlier changes.

**Key points:**
- Created with `SAVE TRANSACTION SavepointName`.
- Roll back with `ROLLBACK TRANSACTION SavepointName`.
- The transaction remains active after rolling back to a savepoint, so you can continue work and later commit.

**Use cases:**
- Partial rollback in multi-step operations.
- Error handling that preserves successful steps.
- Complex batch processing where some failures should not cancel all work.
```sql
-- Example 3: Using savepoints
CREATE PROCEDURE sp_ComplexDataUpdate
    @CustomerID INT
AS
BEGIN
    SET NOCOUNT ON;
    
    BEGIN TRANSACTION ComplexUpdate;
    
    BEGIN TRY
        -- Savepoint 1: Update customer
        SAVE TRANSACTION CustomerUpdate;
        
        UPDATE Customers
        SET LastModified = GETDATE()
        WHERE CustomerID = @CustomerID;
        
        -- Savepoint 2: Update orders
        SAVE TRANSACTION OrderUpdate;
        
        UPDATE Orders
        SET Status = 'Processing'
        WHERE CustomerID = @CustomerID;
        
        -- Savepoint 3: Update products
        SAVE TRANSACTION ProductUpdate;
        
        -- This might fail
        UPDATE Products
        SET LastOrderDate = GETDATE()
        WHERE ProductID IN (
            SELECT ProductID FROM OrderDetails od
            INNER JOIN Orders o ON od.OrderID = o.OrderID
            WHERE o.CustomerID = @CustomerID
        );
        
        -- All operations successful
        COMMIT TRANSACTION ComplexUpdate;
        SELECT 'All updates completed successfully' AS Status;
        
    END TRY
    BEGIN CATCH
        -- Rollback to last savepoint
        IF @@TRANCOUNT > 0
        BEGIN
            -- Try to rollback to product update savepoint
            IF @@TRANCOUNT > 0
                ROLLBACK TRANSACTION ProductUpdate;
            
            -- Continue with order update
            UPDATE Orders
            SET Status = 'Completed'
            WHERE CustomerID = @CustomerID;
            
            -- Commit partial success
            COMMIT TRANSACTION ComplexUpdate;
            SELECT 'Partial update completed' AS Status;
        END;
    END CATCH;
END;

-- Test the procedure
EXEC sp_ComplexDataUpdate @CustomerID = 101;
```

**Result:**
```
Status
----------------------------
All updates completed successfully
```

### Transaction Nesting and @@TRANCOUNT
**Definition:** `@@TRANCOUNT` is a T-SQL system function that returns the **number of active transactions** in the current session.

**Key points:**
- Each `BEGIN TRANSACTION` **increments** `@@TRANCOUNT` by 1.
- Each `COMMIT TRANSACTION` **decrements** it by 1.
- Any `ROLLBACK TRANSACTION` **resets** it to 0 (rolls back the entire transaction stack).
- Commonly used to:
    - Detect whether a transaction is active.
    - Safely handle nested transactions.
    - Guard cleanup in `CATCH` blocks (e.g., `IF @@TRANCOUNT > 0 ROLLBACK;`).
```sql
-- Example 4: Transaction nesting
CREATE PROCEDURE sp_NestedTransactions
AS
BEGIN
    PRINT 'Initial @@TRANCOUNT: ' + CAST(@@TRANCOUNT AS VARCHAR(10));
    
    BEGIN TRANSACTION Level1;
    PRINT 'After Level1 BEGIN: ' + CAST(@@TRANCOUNT AS VARCHAR(10));
    
    BEGIN TRANSACTION Level2;
    PRINT 'After Level2 BEGIN: ' + CAST(@@TRANCOUNT AS VARCHAR(10));
    
    -- Only one COMMIT needed for nested transactions
    COMMIT TRANSACTION Level2;
    PRINT 'After Level2 COMMIT: ' + CAST(@@TRANCOUNT AS VARCHAR(10));
    
    COMMIT TRANSACTION Level1;
    PRINT 'After Level1 COMMIT: ' + CAST(@@TRANCOUNT AS VARCHAR(10));
    
    -- Test rollback scenario
    BEGIN TRANSACTION TestRollback;
    PRINT 'Before rollback: ' + CAST(@@TRANCOUNT AS VARCHAR(10));
    
    ROLLBACK TRANSACTION TestRollback;
    PRINT 'After rollback: ' + CAST(@@TRANCOUNT AS VARCHAR(10));
END;

EXEC sp_NestedTransactions;
```

**Result:**
```
Initial @@TRANCOUNT: 0
After Level1 BEGIN: 1
After Level2 BEGIN: 2
After Level2 COMMIT: 1
After Level1 COMMIT: 0
Before rollback: 1
After rollback: 0
```

## 64. What does it mean to set a transaction isolation level in T-SQL?

### Isolation Levels Overview
Transaction isolation levels control how transactions interact with each other and what data they can see.

### Setting Isolation Levels
```sql
-- Example 1: READ UNCOMMITTED (Lowest isolation)
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

BEGIN TRANSACTION;
    -- Can read uncommitted data (dirty reads)
    SELECT * FROM Orders WHERE Status = 'Pending';
    
    -- Other transactions can see uncommitted changes
    UPDATE Orders SET Status = 'Processing' WHERE OrderID = 1001;
    
    -- Rollback to demonstrate dirty read
    ROLLBACK TRANSACTION;
```

### READ COMMITTED (Default)
```sql
-- Example 2: READ COMMITTED (Default level)
SET TRANSACTION ISOLATION LEVEL READ COMMITTED;

-- Session 1
BEGIN TRANSACTION;
    UPDATE Customers SET CustomerName = 'Updated Name' WHERE CustomerID = 101;
    -- Don't commit yet

-- Session 2 (in another connection)
SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
SELECT CustomerName FROM Customers WHERE CustomerID = 101;
-- Will see old value until Session 1 commits

-- Session 1
COMMIT TRANSACTION;

-- Session 2
SELECT CustomerName FROM Customers WHERE CustomerID = 101;
-- Now sees updated value
```

**Results:**
```
-- Before commit (Session 2):
CustomerName
------------
John Smith

-- After commit (Session 2):
CustomerName
------------
Updated Name
```

### REPEATABLE READ
```sql
-- Example 3: REPEATABLE READ
SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;

BEGIN TRANSACTION;
    -- First read
    SELECT COUNT(*) AS InitialCount FROM Orders WHERE Status = 'Pending';
    
    -- Another transaction cannot modify these rows
    -- Wait for other session to try updating...
    
    -- Second read - same result guaranteed
    SELECT COUNT(*) AS FinalCount FROM Orders WHERE Status = 'Pending';
    
COMMIT TRANSACTION;
```

### SERIALIZABLE (Highest isolation)
```sql
-- Example 4: SERIALIZABLE
SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;

BEGIN TRANSACTION;
    -- Prevents phantom reads
    SELECT * FROM Orders WHERE TotalAmount > 1000;
    
    -- Other transactions cannot insert rows that would match this query
    -- until this transaction commits
    
    -- Insert new order (this would be blocked in another session)
    INSERT INTO Orders (CustomerID, OrderDate, TotalAmount, Status)
    VALUES (101, GETDATE(), 1500.00, 'Pending');
    
COMMIT TRANSACTION;
```

### Snapshot Isolation
```sql
-- Example 5: SNAPSHOT ISOLATION
-- First enable snapshot isolation on database
ALTER DATABASE YourDatabase SET ALLOW_SNAPSHOT_ISOLATION ON;

-- Use snapshot isolation
SET TRANSACTION ISOLATION LEVEL SNAPSHOT;

BEGIN TRANSACTION;
    -- Reads from snapshot at transaction start
    SELECT * FROM Orders WHERE CustomerID = 101;
    
    -- Other transactions can modify data
    -- but this transaction sees consistent snapshot
    
COMMIT TRANSACTION;
```

### Isolation Level Comparison
```sql
-- Example 6: Comparing isolation levels
CREATE PROCEDURE sp_IsolationLevelDemo
    @IsolationLevel NVARCHAR(50)
AS
BEGIN
    DECLARE @SQL NVARCHAR(MAX);
    
    SET @SQL = 'SET TRANSACTION ISOLATION LEVEL ' + @IsolationLevel;
    EXEC sp_executesql @SQL;
    
    BEGIN TRANSACTION;
    
    -- Read data
    SELECT 
        @IsolationLevel AS IsolationLevel,
        COUNT(*) AS OrderCount,
        GETDATE() AS ReadTime
    FROM Orders;
    
    -- Simulate some processing time
    WAITFOR DELAY '00:00:02';
    
    -- Read again
    SELECT 
        @IsolationLevel AS IsolationLevel,
        COUNT(*) AS OrderCount,
        GETDATE() AS ReadTime
    FROM Orders;
    
    COMMIT TRANSACTION;
END;

-- Test different isolation levels
EXEC sp_IsolationLevelDemo @IsolationLevel = 'READ UNCOMMITTED';
EXEC sp_IsolationLevelDemo @IsolationLevel = 'READ COMMITTED';
EXEC sp_IsolationLevelDemo @IsolationLevel = 'REPEATABLE READ';
EXEC sp_IsolationLevelDemo @IsolationLevel = 'SERIALIZABLE';
```

**Results:**
```
-- READ UNCOMMITTED (may see dirty reads):
IsolationLevel | OrderCount | ReadTime
---------------|------------|------------------
READ UNCOMMITTED | 25 | 2024-01-15 14:30:25
READ UNCOMMITTED | 26 | 2024-01-15 14:30:27

-- READ COMMITTED (sees committed data):
IsolationLevel | OrderCount | ReadTime
---------------|------------|------------------
READ COMMITTED | 25 | 2024-01-15 14:30:25
READ COMMITTED | 25 | 2024-01-15 14:30:27
```

## 65. Discuss the potential risks of transaction deadlocks

### Deadlock Scenarios
Deadlocks occur when two or more transactions are waiting for each other to release locks.

### Classic Deadlock Example
```sql
-- Session 1
BEGIN TRANSACTION;
    UPDATE Customers SET CustomerName = 'Session1' WHERE CustomerID = 101;
    -- Wait a moment
    WAITFOR DELAY '00:00:02';
    UPDATE Orders SET Status = 'Session1' WHERE CustomerID = 101;
COMMIT TRANSACTION;

-- Session 2 (run simultaneously)
BEGIN TRANSACTION;
    UPDATE Orders SET Status = 'Session2' WHERE CustomerID = 101;
    -- Wait a moment
    WAITFOR DELAY '00:00:02';
    UPDATE Customers SET CustomerName = 'Session2' WHERE CustomerID = 101;
COMMIT TRANSACTION;
```

**Result:**
```
Msg 1205, Level 13, State 45, Line 4
Transaction (Process ID 52) was deadlocked on lock resources with another process and has been chosen as the deadlock victim. Rerun the transaction.
```

### Deadlock Detection and Prevention
```sql
-- Example 1: Consistent lock ordering
CREATE PROCEDURE sp_SafeUpdateCustomer
    @CustomerID INT,
    @NewName NVARCHAR(100)
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Always lock in same order: Customers first, then Orders
    BEGIN TRANSACTION;
    
    BEGIN TRY
        -- Lock customer first
        UPDATE Customers 
        SET CustomerName = @NewName,
            LastModified = GETDATE()
        WHERE CustomerID = @CustomerID;
        
        -- Then lock related orders
        UPDATE Orders
        SET LastModified = GETDATE()
        WHERE CustomerID = @CustomerID;
        
        COMMIT TRANSACTION;
        SELECT 'Update completed successfully' AS Status;
        
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;
        
        -- Handle deadlock specifically
        IF ERROR_NUMBER() = 1205
        BEGIN
            SELECT 'Deadlock detected, please retry' AS Status;
        END
        ELSE
        BEGIN
            SELECT 'Error: ' + ERROR_MESSAGE() AS Status;
        END;
    END CATCH;
END;
```

### Deadlock Retry Logic
```sql
-- Example 2: Deadlock retry mechanism
CREATE PROCEDURE sp_DeadlockRetry
    @CustomerID INT,
    @MaxRetries INT = 3
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @RetryCount INT = 0;
    DECLARE @Success BIT = 0;
    DECLARE @ErrorMessage NVARCHAR(4000);
    
    WHILE @RetryCount < @MaxRetries AND @Success = 0
    BEGIN
        BEGIN TRY
            BEGIN TRANSACTION;
            
            -- Critical section
            UPDATE Customers 
            SET CustomerName = 'Updated_' + CAST(@RetryCount AS VARCHAR(10)),
                LastModified = GETDATE()
            WHERE CustomerID = @CustomerID;
            
            UPDATE Orders
            SET Status = 'Updated',
                LastModified = GETDATE()
            WHERE CustomerID = @CustomerID;
            
            COMMIT TRANSACTION;
            SET @Success = 1;
            
            SELECT 
                @RetryCount AS RetryCount,
                'Success after ' + CAST(@RetryCount AS VARCHAR(10)) + ' retries' AS Status;
            
        END TRY
        BEGIN CATCH
            IF @@TRANCOUNT > 0
                ROLLBACK TRANSACTION;
            
            SET @ErrorMessage = ERROR_MESSAGE();
            
            -- Check if it's a deadlock
            IF ERROR_NUMBER() = 1205
            BEGIN
                SET @RetryCount = @RetryCount + 1;
                
                -- Exponential backoff
                WAITFOR DELAY '00:00:' + RIGHT('0' + CAST(@RetryCount * 2 AS VARCHAR(2)), 2);
                
                PRINT 'Deadlock detected, retrying... Attempt ' + CAST(@RetryCount AS VARCHAR(10));
            END
            ELSE
            BEGIN
                -- Non-deadlock error, don't retry
                SELECT 'Non-deadlock error: ' + @ErrorMessage AS Status;
                BREAK;
            END;
        END CATCH;
    END;
    
    -- If we exhausted retries
    IF @Success = 0 AND @RetryCount >= @MaxRetries
    BEGIN
        SELECT 'Failed after ' + CAST(@MaxRetries AS VARCHAR(10)) + ' retries' AS Status;
    END;
END;

-- Test with potential deadlock
EXEC sp_DeadlockRetry @CustomerID = 101, @MaxRetries = 5;
```

**Result:**
```
RetryCount | Status
-----------|----------------------------
2          | Success after 2 retries
```

### Deadlock Monitoring
```sql
-- Example 3: Monitor deadlocks
CREATE PROCEDURE sp_MonitorDeadlocks
AS
BEGIN
    -- Query deadlock information from system views
    SELECT 
        'Deadlock Graph' AS InfoType,
        CAST(event_data AS XML) AS DeadlockInfo
    FROM sys.fn_xe_file_target_read_file('system_health*.xel', null, null, null)
    WHERE object_name = 'xml_deadlock_report';
    
    -- Alternative: Query current locks
    SELECT 
        t.request_session_id,
        t.resource_type,
        t.resource_description,
        t.request_mode,
        t.request_status,
        s.program_name,
        s.host_name
    FROM sys.dm_tran_locks t
    INNER JOIN sys.dm_exec_sessions s ON t.request_session_id = s.session_id
    WHERE t.request_status IN ('WAIT', 'CONVERT')
    ORDER BY t.request_session_id;
END;

-- Monitor current deadlock situation
EXEC sp_MonitorDeadlocks;
```

### Deadlock Prevention Strategies
```sql
-- Example 4: Deadlock prevention techniques
CREATE PROCEDURE sp_PreventDeadlocks
    @CustomerID INT
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Strategy 1: Use application locks
    DECLARE @LockResult INT;
    
    -- Acquire application lock
    EXEC @LockResult = sp_getapplock 
        @Resource = 'CustomerUpdate_' + CAST(@CustomerID AS VARCHAR(10)),
        @LockMode = 'Exclusive',
        @LockOwner = 'Transaction',
        @LockTimeout = 5000; -- 5 seconds timeout
    
    IF @LockResult < 0
    BEGIN
        SELECT 'Could not acquire lock, another process is updating this customer' AS Status;
        RETURN;
    END;
    
    BEGIN TRY
        BEGIN TRANSACTION;
        
        -- Critical section
        UPDATE Customers 
        SET CustomerName = 'Locked Update',
            LastModified = GETDATE()
        WHERE CustomerID = @CustomerID;
        
        UPDATE Orders
        SET Status = 'Locked Update',
            LastModified = GETDATE()
        WHERE CustomerID = @CustomerID;
        
        COMMIT TRANSACTION;
        
        -- Release application lock
        EXEC sp_releaseapplock 
            @Resource = 'CustomerUpdate_' + CAST(@CustomerID AS VARCHAR(10)),
            @LockOwner = 'Transaction';
        
        SELECT 'Update completed with lock protection' AS Status;
        
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;
        
        -- Release lock on error
        EXEC sp_releaseapplock 
            @Resource = 'CustomerUpdate_' + CAST(@CustomerID AS VARCHAR(10)),
            @LockOwner = 'Transaction';
        
        SELECT 'Error: ' + ERROR_MESSAGE() AS Status;
    END CATCH;
END;

-- Test deadlock prevention
EXEC sp_PreventDeadlocks @CustomerID = 101;
```

**Result:**
```
Status
----------------------------------------
Update completed with lock protection
```

## 66. What is an index in SQL Server and how is it implemented in T-SQL?

### Index Overview
An index is a database object that improves query performance by providing fast access to data.

### Basic Index Creation
```sql
-- Example 1: Create basic indexes
-- Clustered index (primary key)
CREATE TABLE Customers (
    CustomerID INT IDENTITY(1,1) PRIMARY KEY CLUSTERED,
    CustomerName NVARCHAR(100) NOT NULL,
    Email NVARCHAR(100),
    Phone NVARCHAR(20),
    CreatedDate DATETIME2 DEFAULT GETDATE()
);

-- Non-clustered index on email
CREATE NONCLUSTERED INDEX IX_Customers_Email 
ON Customers (Email);

-- Non-clustered index on created date
CREATE NONCLUSTERED INDEX IX_Customers_CreatedDate 
ON Customers (CreatedDate);

-- Composite index
CREATE NONCLUSTERED INDEX IX_Customers_Name_Email 
ON Customers (CustomerName, Email);
```

### Index Types and Examples
```sql
-- Example 2: Different index types
-- Unique index
CREATE UNIQUE NONCLUSTERED INDEX IX_Customers_Email_Unique 
ON Customers (Email);

-- Filtered index (SQL Server 2008+)
CREATE NONCLUSTERED INDEX IX_Customers_Active_Email 
ON Customers (Email)
WHERE Email IS NOT NULL;

-- Index with included columns
CREATE NONCLUSTERED INDEX IX_Orders_CustomerID_Covering 
ON Orders (CustomerID)
INCLUDE (OrderDate, TotalAmount, Status);

-- Test index usage
SELECT CustomerID, CustomerName, Email
FROM Customers
WHERE Email = 'john@example.com';
```

**Result:**
```
CustomerID | CustomerName | Email
-----------|--------------|------------------
101        | John Smith   | john@example.com
```

### Index Performance Comparison
```sql
-- Example 3: Performance comparison
-- Without index
SELECT CustomerName, Email
FROM Customers
WHERE CustomerName LIKE 'John%';

-- With index
CREATE NONCLUSTERED INDEX IX_Customers_CustomerName 
ON Customers (CustomerName);

-- Same query with index
SELECT CustomerName, Email
FROM Customers
WHERE CustomerName LIKE 'John%';

-- Check index usage
SELECT 
    i.name AS IndexName,
    s.user_seeks,
    s.user_scans,
    s.user_lookups,
    s.user_updates
FROM sys.dm_db_index_usage_stats s
INNER JOIN sys.indexes i ON s.object_id = i.object_id AND s.index_id = i.index_id
WHERE i.name = 'IX_Customers_CustomerName';
```

**Result:**
```
IndexName | user_seeks | user_scans | user_lookups | user_updates
----------|------------|------------|--------------|-------------
IX_Customers_CustomerName | 1 | 0 | 0 | 0
```

## 67. What are clustered and non-clustered indexes?

### Clustered Index
A clustered index determines the physical order of data in a table.

```sql
-- Example 1: Clustered index
CREATE TABLE Orders (
    OrderID INT IDENTITY(1,1),
    CustomerID INT NOT NULL,
    OrderDate DATETIME2 NOT NULL,
    TotalAmount DECIMAL(10,2) NOT NULL,
    Status NVARCHAR(20) NOT NULL,
    
    -- Clustered index (primary key)
    CONSTRAINT PK_Orders PRIMARY KEY CLUSTERED (OrderID)
);

-- Insert test data
INSERT INTO Orders (CustomerID, OrderDate, TotalAmount, Status)
VALUES 
    (101, '2024-01-15', 150.00, 'Completed'),
    (102, '2024-01-16', 200.00, 'Pending'),
    (101, '2024-01-17', 300.00, 'Completed');

-- Query using clustered index
SELECT OrderID, CustomerID, OrderDate, TotalAmount
FROM Orders
WHERE OrderID = 2;
```

**Result:**
```
OrderID | CustomerID | OrderDate  | TotalAmount
--------|------------|------------|------------
2       | 102        | 2024-01-16 | 200.00
```

### Non-Clustered Index
A non-clustered index is a separate structure that points to the actual data.

```sql
-- Example 2: Non-clustered indexes
-- Create non-clustered index on CustomerID
CREATE NONCLUSTERED INDEX IX_Orders_CustomerID 
ON Orders (CustomerID);

-- Create non-clustered index on OrderDate
CREATE NONCLUSTERED INDEX IX_Orders_OrderDate 
ON Orders (OrderDate);

-- Create composite non-clustered index
CREATE NONCLUSTERED INDEX IX_Orders_CustomerID_OrderDate 
ON Orders (CustomerID, OrderDate);

-- Query using non-clustered index
SELECT OrderID, CustomerID, OrderDate, TotalAmount
FROM Orders
WHERE CustomerID = 101;
```

**Result:**
```
OrderID | CustomerID | OrderDate  | TotalAmount
--------|------------|------------|------------
1       | 101        | 2024-01-15 | 150.00
3       | 101        | 2024-01-17 | 300.00
```

### Index Comparison
```sql
-- Example 3: Compare clustered vs non-clustered
-- Clustered index scan (seeks are very fast)
SELECT OrderID, CustomerID, TotalAmount
FROM Orders
WHERE OrderID BETWEEN 1 AND 3;

-- Non-clustered index seek + key lookup
SELECT OrderID, CustomerID, TotalAmount, Status
FROM Orders
WHERE CustomerID = 101;

-- Covering index (no key lookup needed)
CREATE NONCLUSTERED INDEX IX_Orders_CustomerID_Covering 
ON Orders (CustomerID)
INCLUDE (OrderDate, TotalAmount, Status);

-- Same query with covering index
SELECT CustomerID, OrderDate, TotalAmount, Status
FROM Orders
WHERE CustomerID = 101;
```

**Results:**
```
-- Clustered index result:
OrderID | CustomerID | TotalAmount
--------|------------|------------
1       | 101        | 150.00
2       | 102        | 200.00
3       | 101        | 300.00

-- Non-clustered index result:
CustomerID | OrderDate  | TotalAmount | Status
-----------|------------|-------------|----------
101        | 2024-01-15 | 150.00      | Completed
101        | 2024-01-17 | 300.00      | Completed
```

## 68. How can indexing impact the performance of T-SQL queries?

### Performance Impact Examples
```sql
-- Example 1: Query without proper indexing
-- Slow query without index
SELECT CustomerID, COUNT(*) AS OrderCount, SUM(TotalAmount) AS TotalSpent
FROM Orders
WHERE OrderDate >= '2024-01-01'
    AND Status = 'Completed'
GROUP BY CustomerID
ORDER BY TotalSpent DESC;

-- Add index to improve performance
CREATE NONCLUSTERED INDEX IX_Orders_Date_Status_Covering 
ON Orders (OrderDate, Status)
INCLUDE (CustomerID, TotalAmount);

-- Same query with index
SELECT CustomerID, COUNT(*) AS OrderCount, SUM(TotalAmount) AS TotalSpent
FROM Orders
WHERE OrderDate >= '2024-01-01'
    AND Status = 'Completed'
GROUP BY CustomerID
ORDER BY TotalSpent DESC;
```

**Result:**
```
CustomerID | OrderCount | TotalSpent
-----------|------------|------------
101        | 2          | 450.00
102        | 1          | 200.00
```

### Index Seek vs Scan
```sql
-- Example 2: Index seek vs scan
-- Index seek (fast)
SELECT OrderID, CustomerID, TotalAmount
FROM Orders
WHERE CustomerID = 101;

-- Index scan (slower)
SELECT OrderID, CustomerID, TotalAmount
FROM Orders
WHERE TotalAmount > 100;

-- Add index for better performance
CREATE NONCLUSTERED INDEX IX_Orders_TotalAmount 
ON Orders (TotalAmount);

-- Same query with index
SELECT OrderID, CustomerID, TotalAmount
FROM Orders
WHERE TotalAmount > 100;
```

### Index Maintenance Impact
```sql
-- Example 3: Index maintenance overhead
-- Insert performance test
DECLARE @StartTime DATETIME2 = GETDATE();

-- Insert 1000 records
DECLARE @Counter INT = 1;
WHILE @Counter <= 1000
BEGIN
    INSERT INTO Orders (CustomerID, OrderDate, TotalAmount, Status)
    VALUES (
        @Counter % 100 + 1,
        DATEADD(DAY, @Counter, '2024-01-01'),
        RAND() * 1000,
        CASE WHEN @Counter % 3 = 0 THEN 'Pending' ELSE 'Completed' END
    );
    
    SET @Counter = @Counter + 1;
END;

DECLARE @EndTime DATETIME2 = GETDATE();
SELECT DATEDIFF(MILLISECOND, @StartTime, @EndTime) AS InsertTimeMs;
```

**Result:**
```
InsertTimeMs
------------
1250
```

### Index Fragmentation
**Index fragmentation** is the deterioration of an index’s physical layout caused by frequent inserts, updates, and deletes. Over time, the logical key order no longer aligns well with how pages are stored, making I/O less efficient.

#### Types of Fragmentation
- **Internal fragmentation**  
  Wasted space *inside* index pages (low page density). Happens after deletes/updates or poorly chosen fill factors.
- **External fragmentation**  
  Pages are *out of order* for the logical key sequence (page splits, interleaving). Hurts range scans and read-ahead.

#### Common Causes
- Random inserts into the middle of the key space (e.g., GUIDs, non-sequential keys).
- Frequent deletes and updates that change row size.
- Page splits due to full pages and inadequate fill factor.
- Heavy churn in hot tables without periodic maintenance.

#### Why It’s a Problem
- **More I/O:** Extra page reads for scans and lookups.
- **Bigger indexes:** More pages to cache and maintain.
- **Slower queries:** Reduced read-ahead efficiency and increased latency.

#### How to Detect (conceptually)
- Measure **fragmentation percentage**, **page count**, and **page density** using system metadata/DMVs.
- Prioritize large indexes (higher page counts) where fragmentation materially impacts performance.

#### How to Fix
- **Reorganize** the index  
  Lightweight, online defragmentation that reorders pages and compacts them without full rebuild.
- **Rebuild** the index  
  Recreates the index from scratch, producing optimal structure and optionally updating statistics with a full scan.

#### Practical Thresholds (typical guidance)
- **< 10%**: Do nothing.
- **10–30%**: **Reorganize**.
- **> 30%**: **Rebuild**.
> Treat these as heuristics—consider page count, workload patterns, and maintenance windows.

#### Prevention & Best Practices
- Choose **sequential keys** for clustered indexes when possible (e.g., identity/bigint or monotonic keys).
- Set an appropriate **fill factor** to reduce page splits for high-write tables.
- Run **regular index maintenance** (reorg/rebuild) during low-traffic windows.
- Keep **statistics up to date** to help the optimizer choose efficient plans.
- Monitor **hot tables** and adjust maintenance cadence based on actual fragmentation trends.

```sql
-- Example 4: Check index fragmentation
SELECT 
    i.name AS IndexName,
    s.avg_fragmentation_in_percent,
    s.page_count,
    s.avg_page_space_used_in_percent
FROM sys.dm_db_index_physical_stats(DB_ID(), OBJECT_ID('Orders'), NULL, NULL, 'DETAILED') s
INNER JOIN sys.indexes i ON s.object_id = i.object_id AND s.index_id = i.index_id
WHERE s.index_id > 0;

-- Rebuild fragmented index
ALTER INDEX IX_Orders_CustomerID ON Orders REBUILD;

-- Reorganize slightly fragmented index
ALTER INDEX IX_Orders_OrderDate ON Orders REORGANIZE;
```

**Result:**
```
IndexName | avg_fragmentation_in_percent | page_count | avg_page_space_used_in_percent
----------|----------------------------|------------|-------------------------------
IX_Orders_CustomerID | 15.5 | 25 | 78.2
IX_Orders_OrderDate | 5.2 | 18 | 85.1
```

## 69. Discuss the process and reason for index maintenance

### Index Maintenance Overview
Index maintenance ensures optimal performance by managing fragmentation and statistics.

### Fragmentation Analysis
```sql
-- Example 1: Analyze index fragmentation
CREATE PROCEDURE sp_AnalyzeIndexFragmentation
AS
BEGIN
    SELECT 
        OBJECT_NAME(s.object_id) AS TableName,
        i.name AS IndexName,
        s.index_type_desc,
        s.avg_fragmentation_in_percent,
        s.page_count,
        s.avg_page_space_used_in_percent,
        CASE 
            WHEN s.avg_fragmentation_in_percent > 30 THEN 'REBUILD'
            WHEN s.avg_fragmentation_in_percent > 10 THEN 'REORGANIZE'
            ELSE 'OK'
        END AS RecommendedAction
    FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'DETAILED') s
    INNER JOIN sys.indexes i ON s.object_id = i.object_id AND s.index_id = i.index_id
    WHERE s.index_id > 0
        AND s.page_count > 100  -- Only indexes with significant page count
    ORDER BY s.avg_fragmentation_in_percent DESC;
END;

EXEC sp_AnalyzeIndexFragmentation;
```

**Result:**
```
TableName | IndexName | avg_fragmentation_in_percent | RecommendedAction
----------|-----------|----------------------------|------------------
Orders    | IX_Orders_CustomerID | 35.2 | REBUILD
Orders    | IX_Orders_OrderDate | 12.5 | REORGANIZE
Customers | IX_Customers_Email | 5.1 | OK
```

### Automated Index Maintenance
```sql
-- Example 2: Automated index maintenance
CREATE PROCEDURE sp_MaintainIndexes
    @FragmentationThreshold INT = 10,
    @ReorganizeThreshold INT = 30
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @TableName NVARCHAR(128);
    DECLARE @IndexName NVARCHAR(128);
    DECLARE @Fragmentation FLOAT;
    DECLARE @Action NVARCHAR(20);
    DECLARE @SQL NVARCHAR(MAX);
    
    DECLARE index_cursor CURSOR FOR
    SELECT 
        OBJECT_NAME(s.object_id) AS TableName,
        i.name AS IndexName,
        s.avg_fragmentation_in_percent,
        CASE 
            WHEN s.avg_fragmentation_in_percent > @ReorganizeThreshold THEN 'REBUILD'
            WHEN s.avg_fragmentation_in_percent > @FragmentationThreshold THEN 'REORGANIZE'
            ELSE 'SKIP'
        END AS Action
    FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'DETAILED') s
    INNER JOIN sys.indexes i ON s.object_id = i.object_id AND s.index_id = i.index_id
    WHERE s.index_id > 0
        AND s.page_count > 100
        AND s.avg_fragmentation_in_percent > @FragmentationThreshold;
    
    OPEN index_cursor;
    FETCH NEXT FROM index_cursor INTO @TableName, @IndexName, @Fragmentation, @Action;
    
    WHILE @@FETCH_STATUS = 0
    BEGIN
        IF @Action = 'REBUILD'
        BEGIN
            SET @SQL = 'ALTER INDEX ' + @IndexName + ' ON ' + @TableName + ' REBUILD';
            PRINT 'Rebuilding index: ' + @IndexName + ' (Fragmentation: ' + CAST(@Fragmentation AS VARCHAR(10)) + '%)';
        END
        ELSE IF @Action = 'REORGANIZE'
        BEGIN
            SET @SQL = 'ALTER INDEX ' + @IndexName + ' ON ' + @TableName + ' REORGANIZE';
            PRINT 'Reorganizing index: ' + @IndexName + ' (Fragmentation: ' + CAST(@Fragmentation AS VARCHAR(10)) + '%)';
        END;
        
        -- Execute maintenance command
        IF @Action IN ('REBUILD', 'REORGANIZE')
        BEGIN
            EXEC sp_executesql @SQL;
        END;
        
        FETCH NEXT FROM index_cursor INTO @TableName, @IndexName, @Fragmentation, @Action;
    END;
    
    CLOSE index_cursor;
    DEALLOCATE index_cursor;
    
    PRINT 'Index maintenance completed';
END;

-- Run maintenance
EXEC sp_MaintainIndexes @FragmentationThreshold = 5, @ReorganizeThreshold = 20;
```

**Result:**
```
Rebuilding index: IX_Orders_CustomerID (Fragmentation: 35.2%)
Reorganizing index: IX_Orders_OrderDate (Fragmentation: 12.5%)
Index maintenance completed
```

### Statistics Maintenance
```sql
-- Example 3: Statistics maintenance
CREATE PROCEDURE sp_UpdateStatistics
    @TableName NVARCHAR(128) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @SQL NVARCHAR(MAX);
    
    IF @TableName IS NULL
    BEGIN
        -- Update all statistics
        SET @SQL = 'EXEC sp_updatestats';
        PRINT 'Updating all statistics...';
    END
    ELSE
    BEGIN
        -- Update specific table statistics
        SET @SQL = 'UPDATE STATISTICS ' + @TableName;
        PRINT 'Updating statistics for table: ' + @TableName;
    END;
    
    EXEC sp_executesql @SQL;
    
    -- Check statistics age
    SELECT 
        OBJECT_NAME(object_id) AS TableName,
        name AS StatisticName,
        STATS_DATE(object_id, stats_id) AS LastUpdated,
        DATEDIFF(DAY, STATS_DATE(object_id, stats_id), GETDATE()) AS DaysOld
    FROM sys.stats
    WHERE OBJECT_NAME(object_id) = ISNULL(@TableName, OBJECT_NAME(object_id))
    ORDER BY DaysOld DESC;
END;

-- Update statistics for Orders table
EXEC sp_UpdateStatistics @TableName = 'Orders';
```

**Result:**
```
TableName | StatisticName | LastUpdated | DaysOld
----------|---------------|-------------|--------
Orders    | PK_Orders | 2024-01-15 14:30:25 | 0
Orders    | IX_Orders_CustomerID | 2024-01-15 14:30:25 | 0
Orders    | IX_Orders_OrderDate | 2024-01-15 14:30:25 | 0
```

## 70. What are included columns in an index and when would you use them?

### Included Columns Overview
Included columns are non-key columns added to an index to create covering indexes.

### Basic Included Columns
```sql
-- Example 1: Basic included columns
-- Without included columns (requires key lookup)
SELECT CustomerID, OrderDate, TotalAmount, Status
FROM Orders
WHERE CustomerID = 101;

-- Create index with included columns
CREATE NONCLUSTERED INDEX IX_Orders_CustomerID_Covering 
ON Orders (CustomerID)
INCLUDE (OrderDate, TotalAmount, Status);

-- Same query with covering index (no key lookup needed)
SELECT CustomerID, OrderDate, TotalAmount, Status
FROM Orders
WHERE CustomerID = 101;
```

**Result:**
```
CustomerID | OrderDate  | TotalAmount | Status
-----------|------------|-------------|----------
101        | 2024-01-15 | 150.00      | Completed
101        | 2024-01-17 | 300.00      | Completed
```

### Performance Comparison
```sql
-- Example 2: Performance comparison
-- Query that needs key lookup without included columns
SELECT CustomerID, COUNT(*) AS OrderCount, SUM(TotalAmount) AS TotalSpent
FROM Orders
WHERE CustomerID BETWEEN 100 AND 110
GROUP BY CustomerID;

-- Create covering index
CREATE NONCLUSTERED INDEX IX_Orders_CustomerID_Grouping 
ON Orders (CustomerID)
INCLUDE (TotalAmount);

-- Same query with covering index
SELECT CustomerID, COUNT(*) AS OrderCount, SUM(TotalAmount) AS TotalSpent
FROM Orders
WHERE CustomerID BETWEEN 100 AND 110
GROUP BY CustomerID;
```

**Result:**
```
CustomerID | OrderCount | TotalSpent
-----------|------------|------------
101        | 2          | 450.00
102        | 1          | 200.00
```

### Complex Included Columns
```sql
-- Example 3: Complex included columns
-- Create comprehensive covering index
CREATE NONCLUSTERED INDEX IX_Orders_Comprehensive 
ON Orders (CustomerID, OrderDate)
INCLUDE (TotalAmount, Status, CreatedBy, LastModified);

-- Query using all included columns
SELECT 
    CustomerID,
    OrderDate,
    TotalAmount,
    Status,
    CreatedBy,
    LastModified
FROM Orders
WHERE CustomerID = 101
    AND OrderDate >= '2024-01-01'
ORDER BY OrderDate DESC;
```

**Result:**
```
CustomerID | OrderDate  | TotalAmount | Status    | CreatedBy | LastModified
-----------|------------|-------------|-----------|-----------|-------------
101        | 2024-01-17 | 300.00      | Completed | System    | 2024-01-17 14:30:25
101        | 2024-01-15 | 150.00      | Completed | System    | 2024-01-15 10:15:30
```

### When to Use Included Columns
```sql
-- Example 4: When to use included columns
-- Scenario 1: Frequent SELECT with WHERE and ORDER BY
CREATE NONCLUSTERED INDEX IX_Orders_Customer_Date_Status 
ON Orders (CustomerID, OrderDate)
INCLUDE (TotalAmount, Status);

-- Query that benefits from included columns
SELECT CustomerID, OrderDate, TotalAmount, Status
FROM Orders
WHERE CustomerID = 101
    AND OrderDate >= '2024-01-01'
ORDER BY OrderDate DESC;

-- Scenario 2: Aggregation queries
CREATE NONCLUSTERED INDEX IX_Orders_Customer_Aggregation 
ON Orders (CustomerID)
INCLUDE (TotalAmount, Status);

-- Aggregation query
SELECT 
    CustomerID,
    COUNT(*) AS OrderCount,
    SUM(TotalAmount) AS TotalSpent,
    COUNT(CASE WHEN Status = 'Completed' THEN 1 END) AS CompletedOrders
FROM Orders
WHERE CustomerID = 101
GROUP BY CustomerID;
```

**Results:**
```
-- First query result:
CustomerID | OrderDate  | TotalAmount | Status
-----------|------------|-------------|----------
101        | 2024-01-17 | 300.00      | Completed
101        | 2024-01-15 | 150.00      | Completed

-- Second query result:
CustomerID | OrderCount | TotalSpent | CompletedOrders
-----------|------------|------------|----------------
101        | 2          | 450.00     | 2
```

### Index Size Impact
```sql
-- Example 5: Monitor index size impact
SELECT 
    i.name AS IndexName,
    s.page_count,
    s.avg_page_space_used_in_percent,
    s.avg_fragmentation_in_percent,
    (s.page_count * 8) / 1024.0 AS SizeMB
FROM sys.dm_db_index_physical_stats(DB_ID(), OBJECT_ID('Orders'), NULL, NULL, 'DETAILED') s
INNER JOIN sys.indexes i ON s.object_id = i.object_id AND s.index_id = i.index_id
WHERE s.index_id > 0
ORDER BY SizeMB DESC;
```

**Result:**
```
IndexName | page_count | avg_page_space_used_in_percent | SizeMB
----------|------------|----------------------------|-------
IX_Orders_Comprehensive | 45 | 78.5 | 0.35
IX_Orders_CustomerID_Covering | 32 | 82.1 | 0.25
IX_Orders_CustomerID | 18 | 85.2 | 0.14
```

## 71. What is SQL injection and how can it be prevented in T-SQL?

### SQL Injection Overview
SQL injection is a security vulnerability where malicious SQL code is inserted into application queries.

### Vulnerable Code Examples
```sql
-- BAD: Vulnerable to SQL injection
CREATE PROCEDURE sp_GetCustomer_Bad
    @CustomerName NVARCHAR(100)
AS
BEGIN
    DECLARE @SQL NVARCHAR(MAX);
    SET @SQL = 'SELECT * FROM Customers WHERE CustomerName = ''' + @CustomerName + '''';
    
    -- This is vulnerable!
    EXEC sp_executesql @SQL;
END;

-- Example of injection attack
EXEC sp_GetCustomer_Bad @CustomerName = 'John''; DROP TABLE Customers; --';
```

**Result:**
```
-- This would execute malicious code:
-- SELECT * FROM Customers WHERE CustomerName = 'John'; DROP TABLE Customers; --'
```

### Prevention Methods
```sql
-- GOOD: Using parameterized queries
CREATE PROCEDURE sp_GetCustomer_Safe
    @CustomerName NVARCHAR(100)
AS
BEGIN
    SELECT CustomerID, CustomerName, Email, Phone
    FROM Customers
    WHERE CustomerName = @CustomerName;
END;

-- Safe execution
EXEC sp_GetCustomer_Safe @CustomerName = 'John Smith';
```

**Result:**
```
CustomerID | CustomerName | Email           | Phone
-----------|--------------|-----------------|----------
101        | John Smith   | john@email.com  | 555-1234
```

### Dynamic SQL with Parameters
```sql
-- GOOD: Safe dynamic SQL with parameters
CREATE PROCEDURE sp_SearchCustomers_Safe
    @SearchColumn NVARCHAR(50),
    @SearchValue NVARCHAR(100)
AS
BEGIN
    DECLARE @SQL NVARCHAR(MAX);
    DECLARE @ParameterDefinition NVARCHAR(500);
    
    -- Validate column name to prevent injection
    IF @SearchColumn NOT IN ('CustomerName', 'Email', 'Phone')
    BEGIN
        RAISERROR('Invalid search column', 16, 1);
        RETURN;
    END;
    
    SET @SQL = 'SELECT CustomerID, CustomerName, Email, Phone FROM Customers WHERE ' + 
               QUOTENAME(@SearchColumn) + ' = @SearchValue';
    
    SET @ParameterDefinition = '@SearchValue NVARCHAR(100)';
    
    EXEC sp_executesql @SQL, @ParameterDefinition, @SearchValue = @SearchValue;
END;

-- Safe execution
EXEC sp_SearchCustomers_Safe @SearchColumn = 'CustomerName', @SearchValue = 'John Smith';
```

**Result:**
```
CustomerID | CustomerName | Email           | Phone
-----------|--------------|-----------------|----------
101        | John Smith   | john@email.com  | 555-1234
```

### Input Validation
```sql
-- Example: Input validation and sanitization
CREATE PROCEDURE sp_ValidateAndSearch
    @SearchTerm NVARCHAR(100)
AS
BEGIN
    -- Remove potentially dangerous characters
    SET @SearchTerm = REPLACE(REPLACE(REPLACE(@SearchTerm, '''', ''''''), ';', ''), '--', '');
    
    -- Validate input length
    IF LEN(@SearchTerm) > 100
    BEGIN
        RAISERROR('Search term too long', 16, 1);
        RETURN;
    END;
    
    -- Use parameterized query
    SELECT CustomerID, CustomerName, Email
    FROM Customers
    WHERE CustomerName LIKE '%' + @SearchTerm + '%'
        OR Email LIKE '%' + @SearchTerm + '%';
END;

-- Test with various inputs
EXEC sp_ValidateAndSearch @SearchTerm = 'John';
EXEC sp_ValidateAndSearch @SearchTerm = 'john@email.com';
```

**Results:**
```
-- First query:
CustomerID | CustomerName | Email
-----------|--------------|------------------
101        | John Smith   | john@email.com

-- Second query:
CustomerID | CustomerName | Email
-----------|--------------|------------------
101        | John Smith   | john@email.com
```

## 72. How do you implement row-level security in T-SQL?

### Row-Level Security Overview
Row-level security (RLS) allows you to control which rows users can access based on security predicates.

### Basic RLS Implementation
```sql
-- Example 1: Basic RLS setup
-- Create security function
CREATE FUNCTION dbo.fn_SecurityPredicate(@CustomerID INT)
RETURNS TABLE
WITH SCHEMABINDING
AS
RETURN SELECT 1 AS fn_securitypredicate_result
WHERE @CustomerID = CAST(SESSION_CONTEXT(N'CustomerID') AS INT);

-- Create security policy
CREATE SECURITY POLICY dbo.CustomerSecurityPolicy
ADD FILTER PREDICATE dbo.fn_SecurityPredicate(CustomerID) ON dbo.Orders;

-- Set session context
EXEC sp_set_session_context @key = 'CustomerID', @value = 101;

-- Test RLS
SELECT OrderID, CustomerID, TotalAmount, Status
FROM Orders;
```

**Result:**
```
OrderID | CustomerID | TotalAmount | Status
--------|------------|-------------|----------
1       | 101        | 150.00      | Completed
3       | 101        | 300.00      | Completed
```

### Multi-User RLS
```sql
-- Example 2: Multi-user RLS
CREATE FUNCTION dbo.fn_MultiUserSecurity(@CustomerID INT)
RETURNS TABLE
WITH SCHEMABINDING
AS
RETURN SELECT 1 AS fn_securitypredicate_result
WHERE @CustomerID IN (
    SELECT CustomerID 
    FROM UserCustomerAccess 
    WHERE UserName = SUSER_SNAME()
);

-- Create user access table
CREATE TABLE UserCustomerAccess (
    UserName NVARCHAR(128),
    CustomerID INT,
    AccessLevel NVARCHAR(20)
);

-- Insert access permissions
INSERT INTO UserCustomerAccess VALUES
('DOMAIN\user1', 101, 'Read'),
('DOMAIN\user1', 102, 'Read'),
('DOMAIN\user2', 102, 'Read'),
('DOMAIN\user2', 103, 'Read');

-- Apply security policy
CREATE SECURITY POLICY dbo.MultiUserSecurityPolicy
ADD FILTER PREDICATE dbo.fn_MultiUserSecurity(CustomerID) ON dbo.Orders;

-- Test with different users
EXEC sp_set_session_context @key = 'CustomerID', @value = 101;
SELECT COUNT(*) AS VisibleOrders FROM Orders;
```

**Result:**
```
VisibleOrders
-------------
2
```

### Time-Based RLS
```sql
-- Example 3: Time-based RLS
CREATE FUNCTION dbo.fn_TimeBasedSecurity(@OrderDate DATETIME2)
RETURNS TABLE
WITH SCHEMABINDING
AS
RETURN SELECT 1 AS fn_securitypredicate_result
WHERE @OrderDate >= DATEADD(MONTH, -6, GETDATE()); -- Only last 6 months

-- Apply time-based policy
CREATE SECURITY POLICY dbo.TimeBasedSecurityPolicy
ADD FILTER PREDICATE dbo.fn_TimeBasedSecurity(OrderDate) ON dbo.Orders;

-- Test time-based access
SELECT COUNT(*) AS RecentOrders FROM Orders;
SELECT COUNT(*) AS AllOrders FROM Orders WITH (NOLOCK); -- Bypass RLS
```

**Result:**
```
RecentOrders
------------
15
```

## 73. What are the best practices for writing secure T-SQL code?

### Security Best Practices
```sql
-- Example 1: Secure stored procedure
CREATE PROCEDURE sp_SecureCustomerUpdate
    @CustomerID INT,
    @CustomerName NVARCHAR(100),
    @Email NVARCHAR(100),
    @UpdatedBy NVARCHAR(128) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Input validation
    IF @CustomerID IS NULL OR @CustomerID <= 0
    BEGIN
        RAISERROR('Invalid CustomerID', 16, 1);
        RETURN;
    END;
    
    IF @CustomerName IS NULL OR LEN(TRIM(@CustomerName)) = 0
    BEGIN
        RAISERROR('Customer name is required', 16, 1);
        RETURN;
    END;
    
    -- Validate email format
    IF @Email IS NOT NULL AND @Email NOT LIKE '%@%.%'
    BEGIN
        RAISERROR('Invalid email format', 16, 1);
        RETURN;
    END;
    
    -- Set default updated by
    IF @UpdatedBy IS NULL
        SET @UpdatedBy = SUSER_SNAME();
    
    BEGIN TRY
        BEGIN TRANSACTION;
        
        -- Audit before update
        INSERT INTO CustomerAudit (CustomerID, OldName, NewName, UpdatedBy, UpdateDate)
        SELECT @CustomerID, CustomerName, @CustomerName, @UpdatedBy, GETDATE()
        FROM Customers
        WHERE CustomerID = @CustomerID;
        
        -- Perform update
        UPDATE Customers
        SET CustomerName = @CustomerName,
            Email = @Email,
            LastModified = GETDATE(),
            ModifiedBy = @UpdatedBy
        WHERE CustomerID = @CustomerID;
        
        IF @@ROWCOUNT = 0
        BEGIN
            RAISERROR('Customer not found', 16, 1);
        END;
        
        COMMIT TRANSACTION;
        
        SELECT 'Customer updated successfully' AS Status;
        
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;
        
        -- Log security event
        INSERT INTO SecurityLog (EventType, UserName, EventTime, Details)
        VALUES ('UPDATE_FAILED', SUSER_SNAME(), GETDATE(), ERROR_MESSAGE());
        
        RAISERROR('Update failed: %s', 16, 1, ERROR_MESSAGE());
    END CATCH;
END;

-- Test secure procedure
EXEC sp_SecureCustomerUpdate 
    @CustomerID = 101,
    @CustomerName = 'John Smith Updated',
    @Email = 'john.updated@email.com';
```

**Result:**
```
Status
----------------------------
Customer updated successfully
```

### Permission-Based Security
```sql
-- Example 2: Permission-based access control
CREATE PROCEDURE sp_CheckPermissions
    @RequiredPermission NVARCHAR(50)
AS
BEGIN
    -- Check if user has required permission
    IF NOT EXISTS (
        SELECT 1 FROM UserPermissions up
        INNER JOIN UserRoles ur ON up.RoleID = ur.RoleID
        WHERE ur.UserName = SUSER_SNAME()
            AND up.PermissionName = @RequiredPermission
    )
    BEGIN
        RAISERROR('Access denied: Insufficient permissions', 16, 1);
        RETURN;
    END;
END;

-- Secure data access procedure
CREATE PROCEDURE sp_GetSensitiveData
    @CustomerID INT
AS
BEGIN
    -- Check permissions
    EXEC sp_CheckPermissions @RequiredPermission = 'READ_CUSTOMER_DATA';
    
    -- Return data based on user role
    IF EXISTS (
        SELECT 1 FROM UserRoles 
        WHERE UserName = SUSER_SNAME() AND RoleName = 'ADMIN'
    )
    BEGIN
        -- Admin sees all data
        SELECT CustomerID, CustomerName, Email, Phone, SSN, CreditScore
        FROM Customers
        WHERE CustomerID = @CustomerID;
    END
    ELSE
    BEGIN
        -- Regular user sees limited data
        SELECT CustomerID, CustomerName, Email, Phone
        FROM Customers
        WHERE CustomerID = @CustomerID;
    END;
END;
```

### Data Encryption
```sql
-- Example 3: Data encryption
-- Create encrypted column
ALTER TABLE Customers
ADD EncryptedSSN VARBINARY(MAX);

-- Encrypt sensitive data
UPDATE Customers
SET EncryptedSSN = ENCRYPTBYKEY(KEY_GUID('CustomerDataKey'), SSN)
WHERE SSN IS NOT NULL;

-- Decrypt data for authorized users
CREATE PROCEDURE sp_GetDecryptedSSN
    @CustomerID INT
AS
BEGIN
    -- Check permissions
    EXEC sp_CheckPermissions @RequiredPermission = 'READ_SSN';
    
    SELECT 
        CustomerID,
        CustomerName,
        CONVERT(NVARCHAR(20), DECRYPTBYKEY(EncryptedSSN)) AS DecryptedSSN
    FROM Customers
    WHERE CustomerID = @CustomerID
        AND EncryptedSSN IS NOT NULL;
END;
```

## 74. How do you handle large datasets efficiently in T-SQL?

### Pagination Techniques
```sql
-- Example 1: Efficient pagination
CREATE PROCEDURE sp_GetCustomersPaged
    @PageNumber INT = 1,
    @PageSize INT = 50,
    @SortColumn NVARCHAR(50) = 'CustomerName',
    @SortDirection NVARCHAR(4) = 'ASC'
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Validate parameters
    IF @PageNumber < 1 SET @PageNumber = 1;
    IF @PageSize < 1 OR @PageSize > 1000 SET @PageSize = 50;
    IF @SortDirection NOT IN ('ASC', 'DESC') SET @SortDirection = 'ASC';
    
    DECLARE @Offset INT = (@PageNumber - 1) * @PageSize;
    DECLARE @SQL NVARCHAR(MAX);
    
    -- Build dynamic query for sorting
    SET @SQL = '
    SELECT CustomerID, CustomerName, Email, CreatedDate
    FROM Customers
    ORDER BY ' + QUOTENAME(@SortColumn) + ' ' + @SortDirection + '
    OFFSET ' + CAST(@Offset AS VARCHAR(10)) + ' ROWS
    FETCH NEXT ' + CAST(@PageSize AS VARCHAR(10)) + ' ROWS ONLY';
    
    EXEC sp_executesql @SQL;
    
    -- Return total count
    SELECT COUNT(*) AS TotalRecords FROM Customers;
END;

-- Test pagination
EXEC sp_GetCustomersPaged @PageNumber = 1, @PageSize = 10, @SortColumn = 'CustomerName';
```

**Result:**
```
CustomerID | CustomerName | Email           | CreatedDate
-----------|--------------|-----------------|------------------
101        | John Smith   | john@email.com  | 2024-01-15 10:00:00
102        | Jane Doe     | jane@email.com  | 2024-01-16 11:00:00
...

TotalRecords
------------
150
```

### Batch Processing
```sql
-- Example 2: Batch processing large datasets
CREATE PROCEDURE sp_ProcessLargeDataset
    @BatchSize INT = 1000
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @ProcessedCount INT = 0;
    DECLARE @TotalCount INT;
    DECLARE @StartTime DATETIME2 = GETDATE();
    
    -- Get total count
    SELECT @TotalCount = COUNT(*) FROM Orders WHERE Status = 'Pending';
    
    WHILE @ProcessedCount < @TotalCount
    BEGIN
        -- Process batch
        UPDATE TOP (@BatchSize) Orders
        SET Status = 'Processing',
            ProcessedDate = GETDATE()
        WHERE Status = 'Pending';
        
        SET @ProcessedCount = @ProcessedCount + @@ROWCOUNT;
        
        -- Log progress
        PRINT 'Processed ' + CAST(@ProcessedCount AS VARCHAR(10)) + ' of ' + CAST(@TotalCount AS VARCHAR(10)) + ' records';
        
        -- Small delay to prevent blocking
        WAITFOR DELAY '00:00:00.100';
    END;
    
    DECLARE @EndTime DATETIME2 = GETDATE();
    SELECT 
        @ProcessedCount AS ProcessedRecords,
        DATEDIFF(SECOND, @StartTime, @EndTime) AS ProcessingTimeSeconds;
END;

-- Execute batch processing
EXEC sp_ProcessLargeDataset @BatchSize = 500;
```

**Result:**
```
Processed 1000 of 5000 records
Processed 2000 of 5000 records
Processed 3000 of 5000 records
Processed 4000 of 5000 records
Processed 5000 of 5000 records

ProcessedRecords | ProcessingTimeSeconds
-----------------|----------------------
5000             | 15
```

### Memory-Efficient Queries
```sql
-- Example 3: Memory-efficient processing
CREATE PROCEDURE sp_ProcessOrdersEfficiently
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Use cursor for large dataset processing
    DECLARE @OrderID INT;
    DECLARE @CustomerID INT;
    DECLARE @TotalAmount DECIMAL(10,2);
    DECLARE @ProcessedCount INT = 0;
    
    DECLARE order_cursor CURSOR LOCAL FAST_FORWARD FOR
    SELECT OrderID, CustomerID, TotalAmount
    FROM Orders
    WHERE Status = 'Pending'
    ORDER BY OrderDate;
    
    OPEN order_cursor;
    FETCH NEXT FROM order_cursor INTO @OrderID, @CustomerID, @TotalAmount;
    
    WHILE @@FETCH_STATUS = 0
    BEGIN
        -- Process individual order
        BEGIN TRY
            BEGIN TRANSACTION;
            
            -- Update order status
            UPDATE Orders
            SET Status = 'Processing',
                ProcessedDate = GETDATE()
            WHERE OrderID = @OrderID;
            
            -- Update customer totals
            UPDATE Customers
            SET TotalSpent = TotalSpent + @TotalAmount,
                LastOrderDate = GETDATE()
            WHERE CustomerID = @CustomerID;
            
            COMMIT TRANSACTION;
            SET @ProcessedCount = @ProcessedCount + 1;
            
        END TRY
        BEGIN CATCH
            IF @@TRANCOUNT > 0
                ROLLBACK TRANSACTION;
            
            -- Log error but continue processing
            INSERT INTO ProcessingErrors (OrderID, ErrorMessage, ErrorTime)
            VALUES (@OrderID, ERROR_MESSAGE(), GETDATE());
        END CATCH;
        
        FETCH NEXT FROM order_cursor INTO @OrderID, @CustomerID, @TotalAmount;
    END;
    
    CLOSE order_cursor;
    DEALLOCATE order_cursor;
    
    SELECT @ProcessedCount AS ProcessedOrders;
END;
```

## 75. What are the considerations for T-SQL performance optimization?

### Query Optimization Techniques
```sql
-- Example 1: Query optimization
-- BAD: Inefficient query
SELECT c.CustomerName, o.OrderDate, o.TotalAmount
FROM Customers c
INNER JOIN Orders o ON c.CustomerID = o.CustomerID
WHERE o.OrderDate >= '2024-01-01'
    AND o.Status = 'Completed'
ORDER BY o.TotalAmount DESC;

-- GOOD: Optimized query with proper indexing
-- Create covering index
CREATE NONCLUSTERED INDEX IX_Orders_Date_Status_Covering
ON Orders (OrderDate, Status)
INCLUDE (CustomerID, TotalAmount);

-- Optimized query
SELECT c.CustomerName, o.OrderDate, o.TotalAmount
FROM Customers c
INNER JOIN Orders o ON c.CustomerID = o.CustomerID
WHERE o.OrderDate >= '2024-01-01'
    AND o.Status = 'Completed'
ORDER BY o.TotalAmount DESC;
```

### Execution Plan Analysis
```sql
-- Example 2: Analyze execution plans
-- Enable actual execution plan
SET STATISTICS IO ON;
SET STATISTICS TIME ON;

-- Query with execution plan
SELECT 
    c.CustomerName,
    COUNT(o.OrderID) AS OrderCount,
    SUM(o.TotalAmount) AS TotalSpent
FROM Customers c
INNER JOIN Orders o ON c.CustomerID = o.CustomerID
WHERE o.OrderDate >= '2024-01-01'
GROUP BY c.CustomerID, c.CustomerName
HAVING COUNT(o.OrderID) > 1
ORDER BY TotalSpent DESC;

-- Check statistics
SELECT 
    i.name AS IndexName,
    s.user_seeks,
    s.user_scans,
    s.user_lookups,
    s.user_updates,
    s.last_user_seek,
    s.last_user_scan
FROM sys.dm_db_index_usage_stats s
INNER JOIN sys.indexes i ON s.object_id = i.object_id AND s.index_id = i.index_id
WHERE i.name LIKE 'IX_Orders%';
```

**Result:**
```
IndexName | user_seeks | user_scans | user_lookups | user_updates
----------|------------|------------|--------------|-------------
IX_Orders_Date_Status_Covering | 1 | 0 | 0 | 0
```

### Memory and CPU Optimization
```sql
-- Example 3: Memory and CPU optimization
CREATE PROCEDURE sp_OptimizedDataProcessing
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Use table variable for small datasets
    DECLARE @Results TABLE (
        CustomerID INT,
        OrderCount INT,
        TotalSpent DECIMAL(10,2)
    );
    
    -- Efficient aggregation
    INSERT INTO @Results
    SELECT 
        CustomerID,
        COUNT(*) AS OrderCount,
        SUM(TotalAmount) AS TotalSpent
    FROM Orders
    WHERE OrderDate >= '2024-01-01'
    GROUP BY CustomerID;
    
    -- Join with customer data
    SELECT 
        c.CustomerName,
        r.OrderCount,
        r.TotalSpent
    FROM @Results r
    INNER JOIN Customers c ON r.CustomerID = c.CustomerID
    ORDER BY r.TotalSpent DESC;
    
    -- Monitor resource usage
    SELECT 
        'Memory Usage' AS Metric,
        (SELECT COUNT(*) * 8 / 1024.0 FROM @Results) AS ValueMB
    UNION ALL
    SELECT 
        'CPU Time',
        CAST(@@CPU_BUSY AS VARCHAR(10));
END;

-- Execute optimized procedure
EXEC sp_OptimizedDataProcessing;
```

**Result:**
```
CustomerName | OrderCount | TotalSpent
-------------|------------|------------
John Smith   | 5          | 1250.75
Jane Doe     | 3          | 800.50

Metric       | ValueMB
-------------|--------
Memory Usage | 0.01
CPU Time     | 15
```

## 81. What are window functions in T-SQL and how do you use them?

### Window Functions Overview
Window functions perform calculations across a set of rows related to the current row.

### Basic Window Functions
```sql
-- Example 1: Basic window functions
SELECT 
    CustomerID,
    OrderDate,
    TotalAmount,
    ROW_NUMBER() OVER (ORDER BY OrderDate) AS RowNumber,
    RANK() OVER (ORDER BY TotalAmount DESC) AS AmountRank,
    DENSE_RANK() OVER (ORDER BY TotalAmount DESC) AS DenseRank,
    NTILE(4) OVER (ORDER BY TotalAmount DESC) AS Quartile
FROM Orders
ORDER BY OrderDate;
```

**Result:**
```
CustomerID | OrderDate  | TotalAmount | RowNumber | AmountRank | DenseRank | Quartile
-----------|------------|-------------|-----------|------------|-----------|----------
101        | 2024-01-15 | 150.00      | 1         | 3          | 2         | 2
102        | 2024-01-16 | 200.00      | 2         | 2          | 1         | 1
101        | 2024-01-17 | 300.00      | 3         | 1          | 1         | 1
```

### Aggregate Window Functions
```sql
-- Example 2: Aggregate window functions
SELECT 
    CustomerID,
    OrderDate,
    TotalAmount,
    SUM(TotalAmount) OVER (PARTITION BY CustomerID) AS CustomerTotal,
    AVG(TotalAmount) OVER (PARTITION BY CustomerID) AS CustomerAverage,
    COUNT(*) OVER (PARTITION BY CustomerID) AS CustomerOrderCount,
    SUM(TotalAmount) OVER (ORDER BY OrderDate ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS RunningTotal
FROM Orders
ORDER BY CustomerID, OrderDate;
```

**Result:**
```
CustomerID | OrderDate  | TotalAmount | CustomerTotal | CustomerAverage | CustomerOrderCount | RunningTotal
-----------|------------|-------------|---------------|-----------------|-------------------|-------------
101        | 2024-01-15 | 150.00      | 450.00        | 225.00          | 2                  | 150.00
101        | 2024-01-17 | 300.00      | 450.00        | 225.00          | 2                  | 450.00
102        | 2024-01-16 | 200.00      | 200.00        | 200.00          | 1                  | 650.00
```

### Advanced Window Functions
```sql
-- Example 3: Advanced window functions
SELECT 
    CustomerID,
    OrderDate,
    TotalAmount,
    LAG(TotalAmount, 1) OVER (PARTITION BY CustomerID ORDER BY OrderDate) AS PreviousAmount,
    LEAD(TotalAmount, 1) OVER (PARTITION BY CustomerID ORDER BY OrderDate) AS NextAmount,
    FIRST_VALUE(TotalAmount) OVER (PARTITION BY CustomerID ORDER BY OrderDate) AS FirstAmount,
    LAST_VALUE(TotalAmount) OVER (PARTITION BY CustomerID ORDER BY OrderDate 
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS LastAmount
FROM Orders
ORDER BY CustomerID, OrderDate;
```

**Result:**
```
CustomerID | OrderDate  | TotalAmount | PreviousAmount | NextAmount | FirstAmount | LastAmount
-----------|------------|-------------|----------------|------------|-------------|------------
101        | 2024-01-15 | 150.00      | NULL           | 300.00     | 150.00      | 300.00
101        | 2024-01-17 | 300.00      | 150.00         | NULL       | 150.00      | 300.00
102        | 2024-01-16 | 200.00      | NULL           | NULL       | 200.00      | 200.00
```

## 82. How do you use Common Table Expressions (CTEs) effectively?

### Basic CTE Usage
```sql
-- Example 1: Basic CTE
WITH CustomerOrders AS (
    SELECT 
        CustomerID,
        COUNT(*) AS OrderCount,
        SUM(TotalAmount) AS TotalSpent
    FROM Orders
    GROUP BY CustomerID
)
SELECT 
    c.CustomerName,
    co.OrderCount,
    co.TotalSpent
FROM Customers c
INNER JOIN CustomerOrders co ON c.CustomerID = co.CustomerID
ORDER BY co.TotalSpent DESC;
```

**Result:**
```
CustomerName | OrderCount | TotalSpent
-------------|------------|------------
John Smith   | 2          | 450.00
Jane Doe     | 1          | 200.00
```

### Recursive CTE
```sql
-- Example 2: Recursive CTE for hierarchy
WITH EmployeeHierarchy AS (
    -- Anchor member
    SELECT 
        EmployeeID,
        EmployeeName,
        ManagerID,
        0 AS Level,
        CAST(EmployeeName AS NVARCHAR(MAX)) AS HierarchyPath
    FROM Employees
    WHERE ManagerID IS NULL
    
    UNION ALL
    
    -- Recursive member
    SELECT 
        e.EmployeeID,
        e.EmployeeName,
        e.ManagerID,
        eh.Level + 1,
        CAST(eh.HierarchyPath + ' -> ' + e.EmployeeName AS NVARCHAR(MAX))
    FROM Employees e
    INNER JOIN EmployeeHierarchy eh ON e.ManagerID = eh.EmployeeID
)
SELECT 
    EmployeeID,
    EmployeeName,
    Level,
    HierarchyPath
FROM EmployeeHierarchy
ORDER BY Level, EmployeeName;
```

**Result:**
```
EmployeeID | EmployeeName | Level | HierarchyPath
-----------|--------------|-------|----------------------------
100        | CEO          | 0     | CEO
101        | Manager A    | 1     | CEO -> Manager A
102        | Manager B    | 1     | CEO -> Manager B
103        | Employee 1   | 2     | CEO -> Manager A -> Employee 1
104        | Employee 2   | 2     | CEO -> Manager B -> Employee 2
```

### Multiple CTEs
```sql
-- Example 3: Multiple CTEs
WITH 
MonthlySales AS (
    SELECT 
        YEAR(OrderDate) AS OrderYear,
        MONTH(OrderDate) AS OrderMonth,
        SUM(TotalAmount) AS MonthlyTotal
    FROM Orders
    GROUP BY YEAR(OrderDate), MONTH(OrderDate)
),
CustomerRankings AS (
    SELECT 
        CustomerID,
        SUM(TotalAmount) AS CustomerTotal,
        RANK() OVER (ORDER BY SUM(TotalAmount) DESC) AS CustomerRank
    FROM Orders
    GROUP BY CustomerID
)
SELECT 
    ms.OrderYear,
    ms.OrderMonth,
    ms.MonthlyTotal,
    cr.CustomerRank,
    c.CustomerName
FROM MonthlySales ms
CROSS JOIN CustomerRankings cr
INNER JOIN Customers c ON cr.CustomerID = c.CustomerID
WHERE cr.CustomerRank <= 3
ORDER BY ms.OrderYear, ms.OrderMonth, cr.CustomerRank;
```

## 83. What are the different types of SQL Server backups and how do you implement them?

### Full Backup
```sql
-- Example 1: Full database backup
BACKUP DATABASE [YourDatabase] 
TO DISK = 'C:\Backups\YourDatabase_Full.bak'
WITH 
    FORMAT,
    INIT,
    NAME = 'YourDatabase-Full Database Backup',
    SKIP,
    NOREWIND,
    NOUNLOAD,
    STATS = 10;

-- Verify backup
RESTORE VERIFYONLY 
FROM DISK = 'C:\Backups\YourDatabase_Full.bak';
```

### Differential Backup
```sql
-- Example 2: Differential backup
BACKUP DATABASE [YourDatabase] 
TO DISK = 'C:\Backups\YourDatabase_Diff.bak'
WITH 
    DIFFERENTIAL,
    FORMAT,
    INIT,
    NAME = 'YourDatabase-Differential Database Backup',
    SKIP,
    NOREWIND,
    NOUNLOAD,
    STATS = 10;
```

### Transaction Log Backup
```sql
-- Example 3: Transaction log backup
BACKUP LOG [YourDatabase] 
TO DISK = 'C:\Backups\YourDatabase_Log.trn'
WITH 
    FORMAT,
    INIT,
    NAME = 'YourDatabase-Transaction Log Backup',
    SKIP,
    NOREWIND,
    NOUNLOAD,
    STATS = 10;
```

### Automated Backup Strategy
```sql
-- Example 4: Automated backup procedure
CREATE PROCEDURE sp_AutomatedBackup
    @DatabaseName NVARCHAR(128),
    @BackupType NVARCHAR(20) = 'FULL' -- FULL, DIFF, LOG
AS
BEGIN
    DECLARE @BackupPath NVARCHAR(500);
    DECLARE @BackupName NVARCHAR(200);
    DECLARE @SQL NVARCHAR(MAX);
    DECLARE @Timestamp NVARCHAR(20);
    
    SET @Timestamp = FORMAT(GETDATE(), 'yyyyMMdd_HHmmss');
    SET @BackupPath = 'C:\Backups\' + @DatabaseName + '_' + @BackupType + '_' + @Timestamp;
    
    IF @BackupType = 'FULL'
    BEGIN
        SET @BackupName = @DatabaseName + '-Full Database Backup';
        SET @SQL = 'BACKUP DATABASE [' + @DatabaseName + '] TO DISK = ''' + @BackupPath + '.bak'' WITH FORMAT, INIT, NAME = ''' + @BackupName + ''', SKIP, NOREWIND, NOUNLOAD, STATS = 10';
    END
    ELSE IF @BackupType = 'DIFF'
    BEGIN
        SET @BackupName = @DatabaseName + '-Differential Database Backup';
        SET @SQL = 'BACKUP DATABASE [' + @DatabaseName + '] TO DISK = ''' + @BackupPath + '.bak'' WITH DIFFERENTIAL, FORMAT, INIT, NAME = ''' + @BackupName + ''', SKIP, NOREWIND, NOUNLOAD, STATS = 10';
    END
    ELSE IF @BackupType = 'LOG'
    BEGIN
        SET @BackupName = @DatabaseName + '-Transaction Log Backup';
        SET @SQL = 'BACKUP LOG [' + @DatabaseName + '] TO DISK = ''' + @BackupPath + '.trn'' WITH FORMAT, INIT, NAME = ''' + @BackupName + ''', SKIP, NOREWIND, NOUNLOAD, STATS = 10';
    END;
    
    EXEC sp_executesql @SQL;
    
    -- Log backup information
    INSERT INTO BackupLog (DatabaseName, BackupType, BackupPath, BackupDate, BackupSize)
    SELECT 
        @DatabaseName,
        @BackupType,
        @BackupPath,
        GETDATE(),
        (SELECT size * 8.0 / 1024 FROM sys.database_files WHERE name = @DatabaseName);
END;

-- Execute automated backup
EXEC sp_AutomatedBackup @DatabaseName = 'YourDatabase', @BackupType = 'FULL';
```

## 84. How do you implement data archiving strategies in T-SQL?

### Archive Old Data
```sql
-- Example 1: Archive old orders
CREATE PROCEDURE sp_ArchiveOldOrders
    @ArchiveDate DATETIME2,
    @BatchSize INT = 1000
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @ProcessedCount INT = 0;
    DECLARE @TotalCount INT;
    DECLARE @StartTime DATETIME2 = GETDATE();
    
    -- Get total count of records to archive
    SELECT @TotalCount = COUNT(*)
    FROM Orders
    WHERE OrderDate < @ArchiveDate
        AND Status = 'Completed';
    
    PRINT 'Found ' + CAST(@TotalCount AS VARCHAR(10)) + ' records to archive';
    
    WHILE @ProcessedCount < @TotalCount
    BEGIN
        -- Archive batch of records
        INSERT INTO OrdersArchive (OrderID, CustomerID, OrderDate, TotalAmount, Status, ArchivedDate)
        SELECT TOP (@BatchSize)
            OrderID, CustomerID, OrderDate, TotalAmount, Status, GETDATE()
        FROM Orders
        WHERE OrderDate < @ArchiveDate
            AND Status = 'Completed'
            AND OrderID NOT IN (SELECT OrderID FROM OrdersArchive);
        
        SET @ProcessedCount = @ProcessedCount + @@ROWCOUNT;
        
        -- Delete archived records from main table
        DELETE TOP (@BatchSize)
        FROM Orders
        WHERE OrderDate < @ArchiveDate
            AND Status = 'Completed'
            AND OrderID IN (SELECT OrderID FROM OrdersArchive);
        
        PRINT 'Archived ' + CAST(@ProcessedCount AS VARCHAR(10)) + ' of ' + CAST(@TotalCount AS VARCHAR(10)) + ' records';
        
        -- Small delay to prevent blocking
        WAITFOR DELAY '00:00:00.100';
    END;
    
    DECLARE @EndTime DATETIME2 = GETDATE();
    SELECT 
        @ProcessedCount AS ArchivedRecords,
        DATEDIFF(SECOND, @StartTime, @EndTime) AS ProcessingTimeSeconds;
END;

-- Execute archiving
EXEC sp_ArchiveOldOrders @ArchiveDate = '2023-01-01', @BatchSize = 500;
```

**Result:**
```
Found 5000 records to archive
Archived 1000 of 5000 records
Archived 2000 of 5000 records
Archived 3000 of 5000 records
Archived 4000 of 5000 records
Archived 5000 of 5000 records

ArchivedRecords | ProcessingTimeSeconds
----------------|----------------------
5000            | 25
```

### Partitioned Archiving
```sql
-- Example 2: Partitioned table archiving
CREATE PROCEDURE sp_ArchivePartitionedData
    @PartitionDate DATETIME2
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Create partition function for date-based partitioning
    CREATE PARTITION FUNCTION pf_OrderDate (DATETIME2)
    AS RANGE RIGHT FOR VALUES ('2023-01-01', '2024-01-01', '2025-01-01');
    
    -- Create partition scheme
    CREATE PARTITION SCHEME ps_OrderDate
    AS PARTITION pf_OrderDate
    TO ([PRIMARY], [PRIMARY], [PRIMARY], [PRIMARY]);
    
    -- Archive old partition
    ALTER TABLE Orders
    SWITCH PARTITION 1 TO OrdersArchive PARTITION 1;
    
    -- Update statistics
    UPDATE STATISTICS Orders;
    
    SELECT 
        'Partition archived successfully' AS Status,
        @PartitionDate AS ArchiveDate;
END;
```

### Incremental Archiving
```sql
-- Example 3: Incremental archiving with change tracking
CREATE PROCEDURE sp_IncrementalArchive
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @LastArchiveDate DATETIME2;
    DECLARE @CurrentDate DATETIME2 = GETDATE();
    
    -- Get last archive date
    SELECT @LastArchiveDate = MAX(ArchivedDate)
    FROM OrdersArchive;
    
    IF @LastArchiveDate IS NULL
        SET @LastArchiveDate = DATEADD(MONTH, -12, @CurrentDate);
    
    -- Archive records since last archive
    INSERT INTO OrdersArchive (OrderID, CustomerID, OrderDate, TotalAmount, Status, ArchivedDate)
    SELECT 
        OrderID, CustomerID, OrderDate, TotalAmount, Status, @CurrentDate
    FROM Orders
    WHERE OrderDate BETWEEN @LastArchiveDate AND DATEADD(DAY, -30, @CurrentDate)
        AND Status = 'Completed';
    
    -- Delete archived records
    DELETE FROM Orders
    WHERE OrderID IN (SELECT OrderID FROM OrdersArchive WHERE ArchivedDate = @CurrentDate);
    
    -- Update archive metadata
    INSERT INTO ArchiveMetadata (LastArchiveDate, RecordsArchived, ArchiveDate)
    VALUES (@LastArchiveDate, @@ROWCOUNT, @CurrentDate);
    
    SELECT 
        @@ROWCOUNT AS RecordsArchived,
        @LastArchiveDate AS LastArchiveDate,
        @CurrentDate AS CurrentArchiveDate;
END;

-- Execute incremental archiving
EXEC sp_IncrementalArchive;
```

**Result:**
```
RecordsArchived | LastArchiveDate | CurrentArchiveDate
----------------|-----------------|-------------------
150             | 2024-01-01     | 2024-01-15 14:30:25
```

## 85. What are the best practices for T-SQL code documentation?

### Code Documentation Standards
```sql
-- Example 1: Well-documented stored procedure
/*
================================================================================
PROCEDURE: sp_CalculateCustomerLifetimeValue
PURPOSE:   Calculates the lifetime value of customers based on their order history
AUTHOR:    Database Team
CREATED:   2024-01-15
MODIFIED:  2024-01-15
VERSION:   1.0

PARAMETERS:
    @CustomerID INT - The ID of the customer to calculate LTV for
    @StartDate DATETIME2 - Start date for calculation period (optional)
    @EndDate DATETIME2 - End date for calculation period (optional)

RETURNS:
    CustomerID, CustomerName, TotalOrders, TotalSpent, AverageOrderValue, LTV

DEPENDENCIES:
    - Customers table
    - Orders table
    - OrderDetails table

EXAMPLE:
    EXEC sp_CalculateCustomerLifetimeValue @CustomerID = 101;

CHANGELOG:
    1.0 - Initial version
================================================================================
*/
CREATE PROCEDURE sp_CalculateCustomerLifetimeValue
    @CustomerID INT,
    @StartDate DATETIME2 = NULL,
    @EndDate DATETIME2 = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Input validation
    IF @CustomerID IS NULL OR @CustomerID <= 0
    BEGIN
        RAISERROR('Invalid CustomerID provided', 16, 1);
        RETURN;
    END;
    
    -- Set default date range if not provided
    IF @StartDate IS NULL
        SET @StartDate = '1900-01-01';
    IF @EndDate IS NULL
        SET @EndDate = GETDATE();
    
    -- Validate date range
    IF @StartDate >= @EndDate
    BEGIN
        RAISERROR('Start date must be before end date', 16, 1);
        RETURN;
    END;
    
    BEGIN TRY
        -- Calculate customer lifetime value
        SELECT 
            c.CustomerID,
            c.CustomerName,
            COUNT(o.OrderID) AS TotalOrders,
            SUM(o.TotalAmount) AS TotalSpent,
            AVG(o.TotalAmount) AS AverageOrderValue,
            -- LTV calculation: Total spent * 1.2 (assumed retention factor)
            SUM(o.TotalAmount) * 1.2 AS LifetimeValue
        FROM Customers c
        INNER JOIN Orders o ON c.CustomerID = o.CustomerID
        WHERE c.CustomerID = @CustomerID
            AND o.OrderDate BETWEEN @StartDate AND @EndDate
            AND o.Status = 'Completed'
        GROUP BY c.CustomerID, c.CustomerName;
        
    END TRY
    BEGIN CATCH
        -- Log error and re-raise
        INSERT INTO ErrorLog (ProcedureName, ErrorMessage, ErrorTime, UserName)
        VALUES ('sp_CalculateCustomerLifetimeValue', ERROR_MESSAGE(), GETDATE(), SUSER_SNAME());
        
        RAISERROR('Error calculating customer lifetime value: %s', 16, 1, ERROR_MESSAGE());
    END CATCH;
END;

-- Test the procedure
EXEC sp_CalculateCustomerLifetimeValue @CustomerID = 101;
```

**Result:**
```
CustomerID | CustomerName | TotalOrders | TotalSpent | AverageOrderValue | LifetimeValue
-----------|--------------|-------------|------------|-------------------|---------------
101        | John Smith   | 2           | 450.00     | 225.00            | 540.00
```

### Function Documentation
```sql
-- Example 2: Well-documented function
/*
================================================================================
FUNCTION: fn_CalculateAge
PURPOSE:   Calculates the age in years between two dates
AUTHOR:    Database Team
CREATED:   2024-01-15
VERSION:   1.0

PARAMETERS:
    @BirthDate DATE - The birth date
    @CurrentDate DATE - The current date (optional, defaults to GETDATE())

RETURNS:   INT - Age in years

EXAMPLE:
    SELECT dbo.fn_CalculateAge('1990-01-01', '2024-01-15') AS Age;
    -- Returns: 34

NOTES:
    - Handles leap years correctly
    - Returns 0 if birth date is in the future
================================================================================
*/
CREATE FUNCTION dbo.fn_CalculateAge(@BirthDate DATE, @CurrentDate DATE = NULL)
RETURNS INT
AS
BEGIN
    DECLARE @Age INT;
    
    -- Use current date if not provided
    IF @CurrentDate IS NULL
        SET @CurrentDate = CAST(GETDATE() AS DATE);
    
    -- Calculate age
    SET @Age = DATEDIFF(YEAR, @BirthDate, @CurrentDate);
    
    -- Adjust if birthday hasn't occurred this year
    IF DATEADD(YEAR, @Age, @BirthDate) > @CurrentDate
        SET @Age = @Age - 1;
    
    -- Return 0 for future dates
    IF @Age < 0
        SET @Age = 0;
    
    RETURN @Age;
END;

-- Test the function
SELECT dbo.fn_CalculateAge('1990-01-01', '2024-01-15') AS Age;
```

**Result:**
```
Age
---
34
```

### Table Documentation
```sql
-- Example 3: Table documentation with extended properties
-- Add table description
EXEC sp_addextendedproperty 
    @name = 'MS_Description',
    @value = 'Stores customer information including contact details and preferences',
    @level0type = 'SCHEMA', @level0name = 'dbo',
    @level1type = 'TABLE', @level1name = 'Customers';

-- Add column descriptions
EXEC sp_addextendedproperty 
    @name = 'MS_Description',
    @value = 'Unique identifier for the customer',
    @level0type = 'SCHEMA', @level0name = 'dbo',
    @level1type = 'TABLE', @level1name = 'Customers',
    @level2type = 'COLUMN', @level2name = 'CustomerID';

EXEC sp_addextendedproperty 
    @name = 'MS_Description',
    @value = 'Full name of the customer',
    @level0type = 'SCHEMA', @level0name = 'dbo',
    @level1type = 'TABLE', @level1name = 'Customers',
    @level2type = 'COLUMN', @level2name = 'CustomerName';

-- Query table documentation
SELECT 
    t.name AS TableName,
    c.name AS ColumnName,
    ep.value AS Description
FROM sys.tables t
INNER JOIN sys.columns c ON t.object_id = c.object_id
LEFT JOIN sys.extended_properties ep ON t.object_id = ep.major_id 
    AND c.column_id = ep.minor_id 
    AND ep.name = 'MS_Description'
WHERE t.name = 'Customers'
ORDER BY c.column_id;
```

**Result:**
```
TableName | ColumnName   | Description
----------|--------------|----------------------------------------
Customers | CustomerID   | Unique identifier for the customer
Customers | CustomerName | Full name of the customer
Customers | Email        | NULL
Customers | Phone        | NULL
```

## 86. How do you implement data validation and constraints in T-SQL?

### Check Constraints
```sql
-- Example 1: Check constraints for data validation
CREATE TABLE Products (
    ProductID INT IDENTITY(1,1) PRIMARY KEY,
    ProductName NVARCHAR(100) NOT NULL,
    UnitPrice DECIMAL(10,2) NOT NULL,
    StockQuantity INT NOT NULL,
    CategoryID INT NOT NULL,
    CreatedDate DATETIME2 DEFAULT GETDATE(),
    
    -- Check constraints
    CONSTRAINT CK_Products_UnitPrice CHECK (UnitPrice > 0),
    CONSTRAINT CK_Products_StockQuantity CHECK (StockQuantity >= 0),
    CONSTRAINT CK_Products_ProductName CHECK (LEN(TRIM(ProductName)) > 0)
);

-- Test constraint validation
INSERT INTO Products (ProductName, UnitPrice, StockQuantity, CategoryID)
VALUES ('Valid Product', 25.99, 100, 1);

-- This will fail due to constraint
INSERT INTO Products (ProductName, UnitPrice, StockQuantity, CategoryID)
VALUES ('Invalid Product', -10.00, 50, 1);
```

**Result:**
```
-- First insert succeeds
(1 row affected)

-- Second insert fails with:
Msg 547, Level 16, State 0, Line 1
The INSERT statement conflicted with the CHECK constraint "CK_Products_UnitPrice".
```

### Foreign Key Constraints
```sql
-- Example 2: Foreign key constraints
CREATE TABLE Categories (
    CategoryID INT IDENTITY(1,1) PRIMARY KEY,
    CategoryName NVARCHAR(50) NOT NULL UNIQUE
);

-- Add foreign key constraint
ALTER TABLE Products
ADD CONSTRAINT FK_Products_Categories 
FOREIGN KEY (CategoryID) REFERENCES Categories(CategoryID);

-- Test foreign key validation
INSERT INTO Categories (CategoryName) VALUES ('Electronics');

INSERT INTO Products (ProductName, UnitPrice, StockQuantity, CategoryID)
VALUES ('Laptop', 999.99, 10, 1);

-- This will fail due to foreign key constraint
INSERT INTO Products (ProductName, UnitPrice, StockQuantity, CategoryID)
VALUES ('Invalid Product', 25.99, 10, 999);
```

**Result:**
```
-- First insert succeeds
(1 row affected)

-- Second insert fails with:
Msg 547, Level 16, State 0, Line 1
The INSERT statement conflicted with the FOREIGN KEY constraint "FK_Products_Categories".
```

### Custom Validation Functions
```sql
-- Example 3: Custom validation functions
CREATE FUNCTION dbo.fn_ValidateEmail(@Email NVARCHAR(100))
RETURNS BIT
AS
BEGIN
    IF @Email IS NULL OR @Email = ''
        RETURN 1; -- NULL or empty is valid
    
    -- Basic email validation
    IF @Email NOT LIKE '%@%.%'
        RETURN 0;
    
    IF LEN(@Email) > 100
        RETURN 0;
    
    RETURN 1;
END;

-- Create table with custom validation
CREATE TABLE Customers (
    CustomerID INT IDENTITY(1,1) PRIMARY KEY,
    CustomerName NVARCHAR(100) NOT NULL,
    Email NVARCHAR(100),
    Phone NVARCHAR(20),
    CreatedDate DATETIME2 DEFAULT GETDATE(),
    
    CONSTRAINT CK_Customers_Email CHECK (dbo.fn_ValidateEmail(Email) = 1)
);

-- Test email validation
INSERT INTO Customers (CustomerName, Email) 
VALUES ('John Smith', 'john@example.com');

-- This will fail due to invalid email
INSERT INTO Customers (CustomerName, Email) 
VALUES ('Jane Doe', 'invalid-email');
```

**Result:**
```
-- First insert succeeds
(1 row affected)

-- Second insert fails with:
Msg 547, Level 16, State 0, Line 1
The INSERT statement conflicted with the CHECK constraint "CK_Customers_Email".
```

## 87. What are the different types of SQL Server data types and when to use them?

### Numeric Data Types
```sql
-- Example 1: Numeric data types
CREATE TABLE NumericTypes (
    -- Integer types
    TinyIntColumn TINYINT,           -- 0 to 255
    SmallIntColumn SMALLINT,         -- -32,768 to 32,767
    IntColumn INT,                   -- -2,147,483,648 to 2,147,483,647
    BigIntColumn BIGINT,             -- -9,223,372,036,854,775,808 to 9,223,372,036,854,775,807
    
    -- Decimal types
    DecimalColumn DECIMAL(10,2),     -- Fixed precision and scale
    NumericColumn NUMERIC(8,4),      -- Same as DECIMAL
    MoneyColumn MONEY,               -- -922,337,203,685,477.5808 to 922,337,203,685,477.5807
    SmallMoneyColumn SMALLMONEY,     -- -214,748.3648 to 214,748.3647
    
    -- Floating point types
    FloatColumn FLOAT(24),           -- Approximate numeric
    RealColumn REAL                  -- Approximate numeric (FLOAT(24))
);

-- Insert test data
INSERT INTO NumericTypes VALUES (
    255,                    -- TinyInt
    32767,                  -- SmallInt
    2147483647,             -- Int
    9223372036854775807,    -- BigInt
    123456.78,              -- Decimal
    1234.5678,              -- Numeric
    123456.78,              -- Money
    1234.56,                -- SmallMoney
    123.456789,             -- Float
    123.456                 -- Real
);
```

### String Data Types
```sql
-- Example 2: String data types
CREATE TABLE StringTypes (
    -- Fixed-length strings
    CharColumn CHAR(10),             -- Fixed-length, non-Unicode
    NCharColumn NCHAR(10),          -- Fixed-length, Unicode
    
    -- Variable-length strings
    VarCharColumn VARCHAR(50),      -- Variable-length, non-Unicode
    NVarCharColumn NVARCHAR(50),    -- Variable-length, Unicode (recommended)
    VarCharMaxColumn VARCHAR(MAX),  -- Variable-length, up to 2GB
    NVarCharMaxColumn NVARCHAR(MAX), -- Variable-length Unicode, up to 2GB
    
    -- Text types (deprecated)
    TextColumn TEXT,                -- Variable-length, non-Unicode, up to 2GB
    NTextColumn NTEXT               -- Variable-length, Unicode, up to 2GB
);

-- Insert test data
INSERT INTO StringTypes VALUES (
    'Fixed10',              -- CHAR(10)
    N'Unicode10',           -- NCHAR(10)
    'Variable length',      -- VARCHAR(50)
    N'Unicode variable',    -- NVARCHAR(50)
    'Very long text...',    -- VARCHAR(MAX)
    N'Unicode long text...', -- NVARCHAR(MAX)
    'Text data',            -- TEXT
    N'Unicode text'         -- NTEXT
);
```

### Date and Time Data Types
```sql
-- Example 3: Date and time data types
CREATE TABLE DateTimeTypes (
    DateColumn DATE,               -- Date only (0001-01-01 to 9999-12-31)
    TimeColumn TIME,                 -- Time only (00:00:00.0000000 to 23:59:59.9999999)
    DateTimeColumn DATETIME,         -- Date and time (1753-01-01 to 9999-12-31)
    DateTime2Column DATETIME2,       -- Date and time with higher precision
    SmallDateTimeColumn SMALLDATETIME, -- Date and time (1900-01-01 to 2079-06-06)
    DateTimeOffsetColumn DATETIMEOFFSET -- Date and time with timezone
);

-- Insert test data
INSERT INTO DateTimeTypes VALUES (
    '2024-01-15',                           -- DATE
    '14:30:25.1234567',                     -- TIME
    '2024-01-15 14:30:25.123',             -- DATETIME
    '2024-01-15 14:30:25.1234567',        -- DATETIME2
    '2024-01-15 14:30:25',                 -- SMALLDATETIME
    '2024-01-15 14:30:25.1234567 +05:00'   -- DATETIMEOFFSET
);
```

### Binary Data Types
```sql
-- Example 4: Binary data types
CREATE TABLE BinaryTypes (
    BinaryColumn BINARY(10),        -- Fixed-length binary data
    VarBinaryColumn VARBINARY(50),  -- Variable-length binary data
    VarBinaryMaxColumn VARBINARY(MAX), -- Variable-length binary data, up to 2GB
    ImageColumn IMAGE               -- Variable-length binary data, up to 2GB (deprecated)
);

-- Insert test data
INSERT INTO BinaryTypes VALUES (
    0x48656C6C6F,                   -- BINARY(10)
    0x576F726C64,                   -- VARBINARY(50)
    0x5468697349734C6F6E67,         -- VARBINARY(MAX)
    0x44617461                      -- IMAGE
);
```

### Special Data Types
```sql
-- Example 5: Special data types
CREATE TABLE SpecialTypes (
    UniqueIdentifierColumn UNIQUEIDENTIFIER, -- GUID
    XmlColumn XML,                           -- XML data
    JsonColumn NVARCHAR(MAX),               -- JSON data (stored as NVARCHAR)
    SqlVariantColumn SQL_VARIANT,           -- Can store any SQL Server data type
    HierarchyIdColumn HIERARCHYID,          -- Represents a position in a hierarchy
    GeographyColumn GEOGRAPHY,              -- Spatial data (geographic)
    GeometryColumn GEOMETRY                 -- Spatial data (geometric)
);

-- Insert test data
INSERT INTO SpecialTypes VALUES (
    NEWID(),                                -- UNIQUEIDENTIFIER
    '<root><item>value</item></root>',     -- XML
    '{"name": "John", "age": 30}',         -- JSON
    'Text value',                          -- SQL_VARIANT
    '/1/2/3/',                             -- HIERARCHYID
    GEOGRAPHY::Point(47.6062, -122.3321, 4326), -- GEOGRAPHY
    GEOMETRY::Point(47.6062, -122.3321)   -- GEOMETRY
);
```

## 88. How do you implement data warehousing concepts in T-SQL?

### Star Schema Design
```sql
-- Example 1: Star schema implementation
-- Dimension table: Customers
CREATE TABLE DimCustomers (
    CustomerKey INT IDENTITY(1,1) PRIMARY KEY,
    CustomerID INT NOT NULL,
    CustomerName NVARCHAR(100) NOT NULL,
    Email NVARCHAR(100),
    City NVARCHAR(50),
    State NVARCHAR(50),
    Country NVARCHAR(50),
    CustomerSegment NVARCHAR(20),
    StartDate DATETIME2 NOT NULL,
    EndDate DATETIME2 NULL,
    IsCurrent BIT DEFAULT 1
);

-- Dimension table: Products
CREATE TABLE DimProducts (
    ProductKey INT IDENTITY(1,1) PRIMARY KEY,
    ProductID INT NOT NULL,
    ProductName NVARCHAR(100) NOT NULL,
    CategoryName NVARCHAR(50),
    BrandName NVARCHAR(50),
    UnitPrice DECIMAL(10,2),
    StartDate DATETIME2 NOT NULL,
    EndDate DATETIME2 NULL,
    IsCurrent BIT DEFAULT 1
);

-- Dimension table: Date
CREATE TABLE DimDate (
    DateKey INT PRIMARY KEY, -- YYYYMMDD format
    FullDate DATE NOT NULL,
    Year INT NOT NULL,
    Quarter INT NOT NULL,
    Month INT NOT NULL,
    MonthName NVARCHAR(20) NOT NULL,
    Day INT NOT NULL,
    DayOfWeek INT NOT NULL,
    DayName NVARCHAR(20) NOT NULL,
    IsWeekend BIT NOT NULL,
    IsHoliday BIT DEFAULT 0
);

-- Fact table: Sales
CREATE TABLE FactSales (
    SalesKey INT IDENTITY(1,1) PRIMARY KEY,
    CustomerKey INT NOT NULL,
    ProductKey INT NOT NULL,
    DateKey INT NOT NULL,
    OrderID INT NOT NULL,
    Quantity INT NOT NULL,
    UnitPrice DECIMAL(10,2) NOT NULL,
    TotalAmount DECIMAL(10,2) NOT NULL,
    DiscountAmount DECIMAL(10,2) DEFAULT 0,
    
    FOREIGN KEY (CustomerKey) REFERENCES DimCustomers(CustomerKey),
    FOREIGN KEY (ProductKey) REFERENCES DimProducts(ProductKey),
    FOREIGN KEY (DateKey) REFERENCES DimDate(DateKey)
);

-- Create indexes for performance
CREATE INDEX IX_FactSales_CustomerKey ON FactSales(CustomerKey);
CREATE INDEX IX_FactSales_ProductKey ON FactSales(ProductKey);
CREATE INDEX IX_FactSales_DateKey ON FactSales(DateKey);
```

### ETL Process Implementation
```sql
-- Example 2: ETL process for data warehouse
CREATE PROCEDURE sp_ETL_SalesData
    @LoadDate DATETIME2 = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    IF @LoadDate IS NULL
        SET @LoadDate = GETDATE();
    
    BEGIN TRY
        BEGIN TRANSACTION;
        
        -- Step 1: Extract and Load Customers
        INSERT INTO DimCustomers (CustomerID, CustomerName, Email, City, State, Country, CustomerSegment, StartDate)
        SELECT 
            c.CustomerID,
            c.CustomerName,
            c.Email,
            c.City,
            c.State,
            c.Country,
            CASE 
                WHEN SUM(o.TotalAmount) > 10000 THEN 'Premium'
                WHEN SUM(o.TotalAmount) > 5000 THEN 'Gold'
                WHEN SUM(o.TotalAmount) > 1000 THEN 'Silver'
                ELSE 'Bronze'
            END AS CustomerSegment,
            @LoadDate
        FROM Customers c
        LEFT JOIN Orders o ON c.CustomerID = o.CustomerID
        WHERE c.CustomerID NOT IN (SELECT CustomerID FROM DimCustomers WHERE IsCurrent = 1)
        GROUP BY c.CustomerID, c.CustomerName, c.Email, c.City, c.State, c.Country;
        
        -- Step 2: Extract and Load Products
        INSERT INTO DimProducts (ProductID, ProductName, CategoryName, BrandName, UnitPrice, StartDate)
        SELECT 
            p.ProductID,
            p.ProductName,
            c.CategoryName,
            p.BrandName,
            p.UnitPrice,
            @LoadDate
        FROM Products p
        INNER JOIN Categories c ON p.CategoryID = c.CategoryID
        WHERE p.ProductID NOT IN (SELECT ProductID FROM DimProducts WHERE IsCurrent = 1);
        
        -- Step 3: Extract and Load Sales Facts
        INSERT INTO FactSales (CustomerKey, ProductKey, DateKey, OrderID, Quantity, UnitPrice, TotalAmount, DiscountAmount)
        SELECT 
            dc.CustomerKey,
            dp.ProductKey,
            dd.DateKey,
            o.OrderID,
            od.Quantity,
            od.UnitPrice,
            od.Quantity * od.UnitPrice AS TotalAmount,
            ISNULL(od.DiscountAmount, 0) AS DiscountAmount
        FROM Orders o
        INNER JOIN OrderDetails od ON o.OrderID = od.OrderID
        INNER JOIN DimCustomers dc ON o.CustomerID = dc.CustomerID AND dc.IsCurrent = 1
        INNER JOIN DimProducts dp ON od.ProductID = dp.ProductID AND dp.IsCurrent = 1
        INNER JOIN DimDate dd ON CAST(o.OrderDate AS DATE) = dd.FullDate
        WHERE o.OrderDate >= @LoadDate;
        
        COMMIT TRANSACTION;
        
        SELECT 'ETL process completed successfully' AS Status;
        
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;
        
        RAISERROR('ETL process failed: %s', 16, 1, ERROR_MESSAGE());
    END CATCH;
END;

-- Execute ETL process
EXEC sp_ETL_SalesData @LoadDate = '2024-01-01';
```

**Result:**
```
Status
--------------------------------
ETL process completed successfully
```

### Data Warehouse Queries
```sql
-- Example 3: Data warehouse analytical queries
-- Sales by customer segment
SELECT 
    dc.CustomerSegment,
    COUNT(DISTINCT fs.CustomerKey) AS CustomerCount,
    SUM(fs.TotalAmount) AS TotalSales,
    AVG(fs.TotalAmount) AS AverageSale
FROM FactSales fs
INNER JOIN DimCustomers dc ON fs.CustomerKey = dc.CustomerKey
INNER JOIN DimDate dd ON fs.DateKey = dd.DateKey
WHERE dd.Year = 2024
GROUP BY dc.CustomerSegment
ORDER BY TotalSales DESC;
```

**Result:**
```
CustomerSegment | CustomerCount | TotalSales | AverageSale
----------------|---------------|------------|------------
Premium         | 15            | 125000.00  | 8333.33
Gold            | 25            | 87500.00   | 3500.00
Silver          | 40            | 45000.00   | 1125.00
Bronze          | 60            | 15000.00   | 250.00
```

## 89. What are the considerations for T-SQL performance monitoring?

### Performance Monitoring Queries
```sql
-- Example 1: Monitor query performance
SELECT 
    qs.sql_handle,
    qt.text AS QueryText,
    qs.execution_count,
    qs.total_elapsed_time / 1000 AS TotalElapsedTimeMs,
    qs.total_elapsed_time / qs.execution_count / 1000 AS AvgElapsedTimeMs,
    qs.total_worker_time / 1000 AS TotalWorkerTimeMs,
    qs.total_logical_reads,
    qs.total_logical_writes,
    qs.total_physical_reads,
    qs.creation_time,
    qs.last_execution_time
FROM sys.dm_exec_query_stats qs
CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) qt
WHERE qs.execution_count > 1
ORDER BY qs.total_elapsed_time DESC;
```

### Index Usage Monitoring
```sql
-- Example 2: Monitor index usage
SELECT 
    OBJECT_NAME(i.object_id) AS TableName,
    i.name AS IndexName,
    i.type_desc AS IndexType,
    s.user_seeks,
    s.user_scans,
    s.user_lookups,
    s.user_updates,
    s.last_user_seek,
    s.last_user_scan,
    s.last_user_lookup,
    s.last_user_update
FROM sys.dm_db_index_usage_stats s
INNER JOIN sys.indexes i ON s.object_id = i.object_id AND s.index_id = i.index_id
WHERE s.database_id = DB_ID()
    AND i.name IS NOT NULL
ORDER BY s.user_seeks + s.user_scans + s.user_lookups DESC;
```

### Wait Statistics
```sql
-- Example 3: Monitor wait statistics
SELECT 
    wait_type,
    waiting_tasks_count,
    wait_time_ms,
    max_wait_time_ms,
    signal_wait_time_ms,
    wait_time_ms - signal_wait_time_ms AS resource_wait_time_ms
FROM sys.dm_os_wait_stats
WHERE waiting_tasks_count > 0
ORDER BY wait_time_ms DESC;
```

### Memory Usage Monitoring
```sql
-- Example 4: Monitor memory usage
SELECT 
    'Buffer Pool' AS MemoryType,
    (SELECT COUNT(*) * 8 / 1024.0 FROM sys.dm_os_buffer_descriptors) AS SizeMB
UNION ALL
SELECT 
    'Plan Cache',
    (SELECT SUM(size_in_bytes) / 1024.0 / 1024.0 FROM sys.dm_exec_cached_plans)
UNION ALL
SELECT 
    'Total Memory',
    (SELECT total_physical_memory_kb / 1024.0 FROM sys.dm_os_sys_info);
```

## 90. How do you implement data quality checks in T-SQL?

### Data Quality Validation
```sql
-- Example 1: Data quality validation procedure
CREATE PROCEDURE sp_ValidateDataQuality
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @ValidationResults TABLE (
        TableName NVARCHAR(128),
        ColumnName NVARCHAR(128),
        ValidationType NVARCHAR(50),
        IssueCount INT,
        IssueDescription NVARCHAR(500)
    );
    
    -- Check for NULL values in required fields
    INSERT INTO @ValidationResults
    SELECT 
        'Customers',
        'CustomerName',
        'NULL_CHECK',
        COUNT(*),
        'NULL values found in CustomerName'
    FROM Customers
    WHERE CustomerName IS NULL;
    
    -- Check for duplicate emails
    INSERT INTO @ValidationResults
    SELECT 
        'Customers',
        'Email',
        'DUPLICATE_CHECK',
        COUNT(*) - COUNT(DISTINCT Email),
        'Duplicate email addresses found'
    FROM Customers
    WHERE Email IS NOT NULL;
    
    -- Check for invalid email format
    INSERT INTO @ValidationResults
    SELECT 
        'Customers',
        'Email',
        'FORMAT_CHECK',
        COUNT(*),
        'Invalid email format found'
    FROM Customers
    WHERE Email IS NOT NULL 
        AND Email NOT LIKE '%@%.%';
    
    -- Check for negative prices
    INSERT INTO @ValidationResults
    SELECT 
        'Products',
        'UnitPrice',
        'RANGE_CHECK',
        COUNT(*),
        'Negative prices found'
    FROM Products
    WHERE UnitPrice < 0;
    
    -- Check for future dates
    INSERT INTO @ValidationResults
    SELECT 
        'Orders',
        'OrderDate',
        'DATE_CHECK',
        COUNT(*),
        'Future order dates found'
    FROM Orders
    WHERE OrderDate > GETDATE();
    
    -- Return validation results
    SELECT * FROM @ValidationResults
    ORDER BY IssueCount DESC;
    
    -- Summary
    SELECT 
        COUNT(*) AS TotalIssues,
        SUM(IssueCount) AS TotalRecordsWithIssues
    FROM @ValidationResults;
END;

-- Execute data quality validation
EXEC sp_ValidateDataQuality;
```

**Result:**
```
TableName | ColumnName | ValidationType | IssueCount | IssueDescription
----------|------------|----------------|------------|------------------
Customers | Email      | FORMAT_CHECK   | 5          | Invalid email format found
Orders    | OrderDate  | DATE_CHECK     | 2          | Future order dates found
Products  | UnitPrice  | RANGE_CHECK    | 1          | Negative prices found

TotalIssues | TotalRecordsWithIssues
------------|-----------------------
3           | 8
```

### Data Completeness Checks
```sql
-- Example 2: Data completeness validation
CREATE PROCEDURE sp_CheckDataCompleteness
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Check for missing customer data
    SELECT 
        'Customers' AS TableName,
        'Missing Email' AS Issue,
        COUNT(*) AS RecordCount
    FROM Customers
    WHERE Email IS NULL OR Email = ''
    
    UNION ALL
    
    -- Check for missing product data
    SELECT 
        'Products' AS TableName,
        'Missing Category' AS Issue,
        COUNT(*) AS RecordCount
    FROM Products p
    LEFT JOIN Categories c ON p.CategoryID = c.CategoryID
    WHERE c.CategoryID IS NULL
    
    UNION ALL
    
    -- Check for orphaned order details
    SELECT 
        'OrderDetails' AS TableName,
        'Orphaned Records' AS Issue,
        COUNT(*) AS RecordCount
    FROM OrderDetails od
    LEFT JOIN Orders o ON od.OrderID = o.OrderID
    WHERE o.OrderID IS NULL;
END;

-- Execute completeness check
EXEC sp_CheckDataCompleteness;
```

### Data Consistency Checks
```sql
-- Example 3: Data consistency validation
CREATE PROCEDURE sp_CheckDataConsistency
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Check for inconsistent order totals
    SELECT 
        'Orders' AS TableName,
        'Inconsistent Totals' AS Issue,
        COUNT(*) AS RecordCount
    FROM Orders o
    WHERE o.TotalAmount != (
        SELECT SUM(od.Quantity * od.UnitPrice)
        FROM OrderDetails od
        WHERE od.OrderID = o.OrderID
    );
    
    -- Check for invalid customer references
    SELECT 
        'Orders' AS TableName,
        'Invalid Customer References' AS Issue,
        COUNT(*) AS RecordCount
    FROM Orders o
    LEFT JOIN Customers c ON o.CustomerID = c.CustomerID
    WHERE c.CustomerID IS NULL;
    
    -- Check for invalid product references
    SELECT 
        'OrderDetails' AS TableName,
        'Invalid Product References' AS Issue,
        COUNT(*) AS RecordCount
    FROM OrderDetails od
    LEFT JOIN Products p ON od.ProductID = p.ProductID
    WHERE p.ProductID IS NULL;
END;

-- Execute consistency check
EXEC sp_CheckDataConsistency;
```

## 91. How do you implement automated testing for T-SQL code?

### Unit Testing Framework
```sql
-- Example 1: T-SQL unit testing framework
CREATE TABLE TestResults (
    TestID INT IDENTITY(1,1) PRIMARY KEY,
    TestName NVARCHAR(100) NOT NULL,
    TestResult NVARCHAR(20) NOT NULL,
    ErrorMessage NVARCHAR(MAX),
    TestDate DATETIME2 DEFAULT GETDATE()
);

-- Test procedure
CREATE PROCEDURE sp_RunTest
    @TestName NVARCHAR(100),
    @TestCondition BIT,
    @ErrorMessage NVARCHAR(MAX) = NULL
AS
BEGIN
    DECLARE @Result NVARCHAR(20);
    
    IF @TestCondition = 1
        SET @Result = 'PASS';
    ELSE
        SET @Result = 'FAIL';
    
    INSERT INTO TestResults (TestName, TestResult, ErrorMessage)
    VALUES (@TestName, @Result, @ErrorMessage);
    
    SELECT @TestName AS TestName, @Result AS Result;
END;

-- Test customer validation function
CREATE PROCEDURE sp_TestCustomerValidation
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Test 1: Valid customer name
    DECLARE @ValidName BIT = 0;
    IF LEN('John Smith') > 0
        SET @ValidName = 1;
    
    EXEC sp_RunTest 'Valid Customer Name', @ValidName, 'Customer name should not be empty';
    
    -- Test 2: Invalid customer name
    DECLARE @InvalidName BIT = 0;
    IF LEN('') = 0
        SET @InvalidName = 1;
    
    EXEC sp_RunTest 'Invalid Customer Name', @InvalidName, 'Empty customer name should be invalid';
    
    -- Test 3: Email validation
    DECLARE @ValidEmail BIT = 0;
    IF 'john@example.com' LIKE '%@%.%'
        SET @ValidEmail = 1;
    
    EXEC sp_RunTest 'Valid Email Format', @ValidEmail, 'Email should contain @ and .';
    
    -- Test 4: Invalid email
    DECLARE @InvalidEmail BIT = 0;
    IF 'invalid-email' NOT LIKE '%@%.%'
        SET @InvalidEmail = 1;
    
    EXEC sp_RunTest 'Invalid Email Format', @InvalidEmail, 'Invalid email should be rejected';
    
    -- Summary
    SELECT 
        TestResult,
        COUNT(*) AS TestCount
    FROM TestResults
    WHERE TestDate >= CAST(GETDATE() AS DATE)
    GROUP BY TestResult;
END;

-- Execute tests
EXEC sp_TestCustomerValidation;
```

**Result:**
```
TestName | Result
---------|-------
Valid Customer Name | PASS
Invalid Customer Name | PASS
Valid Email Format | PASS
Invalid Email Format | PASS

TestResult | TestCount
-----------|----------
PASS       | 4
```

### Integration Testing
```sql
-- Example 2: Integration testing for stored procedures
CREATE PROCEDURE sp_TestOrderProcessing
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Setup test data
    DECLARE @TestCustomerID INT;
    DECLARE @TestProductID INT;
    DECLARE @TestOrderID INT;
    
    -- Create test customer
    INSERT INTO Customers (CustomerName, Email)
    VALUES ('Test Customer', 'test@example.com');
    SET @TestCustomerID = SCOPE_IDENTITY();
    
    -- Create test product
    INSERT INTO Products (ProductName, UnitPrice, StockQuantity, CategoryID)
    VALUES ('Test Product', 25.99, 100, 1);
    SET @TestProductID = SCOPE_IDENTITY();
    
    -- Test order creation
    BEGIN TRY
        INSERT INTO Orders (CustomerID, OrderDate, TotalAmount, Status)
        VALUES (@TestCustomerID, GETDATE(), 25.99, 'Pending');
        SET @TestOrderID = SCOPE_IDENTITY();
        
        EXEC sp_RunTest 'Order Creation', 1, 'Order should be created successfully';
        
        -- Test order processing
        UPDATE Orders
        SET Status = 'Completed'
        WHERE OrderID = @TestOrderID;
        
        EXEC sp_RunTest 'Order Processing', 1, 'Order should be processed successfully';
        
        -- Test data consistency
        DECLARE @OrderExists BIT = 0;
        IF EXISTS (SELECT 1 FROM Orders WHERE OrderID = @TestOrderID AND Status = 'Completed')
            SET @OrderExists = 1;
        
        EXEC sp_RunTest 'Data Consistency', @OrderExists, 'Order should exist and be completed';
        
    END TRY
    BEGIN CATCH
        EXEC sp_RunTest 'Order Processing', 0, ERROR_MESSAGE();
    END CATCH;
    
    -- Cleanup test data
    DELETE FROM Orders WHERE CustomerID = @TestCustomerID;
    DELETE FROM Products WHERE ProductID = @TestProductID;
    DELETE FROM Customers WHERE CustomerID = @TestCustomerID;
END;

-- Execute integration test
EXEC sp_TestOrderProcessing;
```

### Performance Testing
```sql
-- Example 3: Performance testing
CREATE PROCEDURE sp_TestPerformance
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @StartTime DATETIME2;
    DECLARE @EndTime DATETIME2;
    DECLARE @ExecutionTime INT;
    
    -- Test query performance
    SET @StartTime = GETDATE();
    
    SELECT COUNT(*) FROM Orders
    WHERE OrderDate >= '2024-01-01';
    
    SET @EndTime = GETDATE();
    SET @ExecutionTime = DATEDIFF(MILLISECOND, @StartTime, @EndTime);
    
    -- Performance threshold: 100ms
    DECLARE @PerformanceTest BIT = 0;
    IF @ExecutionTime <= 100
        SET @PerformanceTest = 1;
    
    EXEC sp_RunTest 'Query Performance', @PerformanceTest, 
        'Query execution time: ' + CAST(@ExecutionTime AS VARCHAR(10)) + 'ms (threshold: 100ms)';
    
    -- Test index usage
    DECLARE @IndexUsed BIT = 0;
    IF EXISTS (
        SELECT 1 FROM sys.dm_db_index_usage_stats s
        INNER JOIN sys.indexes i ON s.object_id = i.object_id AND s.index_id = i.index_id
        WHERE i.name LIKE 'IX_Orders%' AND s.user_seeks > 0
    )
        SET @IndexUsed = 1;
    
    EXEC sp_RunTest 'Index Usage', @IndexUsed, 'Indexes should be used for query optimization';
END;

-- Execute performance test
EXEC sp_TestPerformance;
```

## 92. What are the best practices for T-SQL code version control?

### Version Control Integration
```sql
-- Example 1: Version control metadata
CREATE TABLE CodeVersions (
    VersionID INT IDENTITY(1,1) PRIMARY KEY,
    ObjectName NVARCHAR(128) NOT NULL,
    ObjectType NVARCHAR(50) NOT NULL,
    VersionNumber NVARCHAR(20) NOT NULL,
    ChangeDescription NVARCHAR(500),
    Author NVARCHAR(100),
    CreatedDate DATETIME2 DEFAULT GETDATE(),
    GitCommitHash NVARCHAR(40),
    DeploymentDate DATETIME2
);

-- Track procedure changes
CREATE PROCEDURE sp_TrackCodeVersion
    @ObjectName NVARCHAR(128),
    @ObjectType NVARCHAR(50),
    @VersionNumber NVARCHAR(20),
    @ChangeDescription NVARCHAR(500),
    @Author NVARCHAR(100),
    @GitCommitHash NVARCHAR(40) = NULL
AS
BEGIN
    INSERT INTO CodeVersions (
        ObjectName, ObjectType, VersionNumber, 
        ChangeDescription, Author, GitCommitHash
    )
    VALUES (
        @ObjectName, @ObjectType, @VersionNumber,
        @ChangeDescription, @Author, @GitCommitHash
    );
    
    SELECT 'Version tracked successfully' AS Status;
END;

-- Track a procedure change
EXEC sp_TrackCodeVersion 
    @ObjectName = 'sp_CalculateCustomerLifetimeValue',
    @ObjectType = 'PROCEDURE',
    @VersionNumber = '1.1',
    @ChangeDescription = 'Added error handling and improved performance',
    @Author = 'John Developer',
    @GitCommitHash = 'a1b2c3d4e5f6';
```

**Result:**
```
Status
--------------------------------
Version tracked successfully
```

### Code Review Process
```sql
-- Example 2: Code review tracking
CREATE TABLE CodeReviews (
    ReviewID INT IDENTITY(1,1) PRIMARY KEY,
    ObjectName NVARCHAR(128) NOT NULL,
    Reviewer NVARCHAR(100) NOT NULL,
    ReviewDate DATETIME2 DEFAULT GETDATE(),
    ReviewStatus NVARCHAR(20) NOT NULL,
    Comments NVARCHAR(MAX),
    ApprovedBy NVARCHAR(100)
);

-- Code review procedure
CREATE PROCEDURE sp_SubmitForReview
    @ObjectName NVARCHAR(128),
    @Reviewer NVARCHAR(100),
    @Comments NVARCHAR(MAX) = NULL
AS
BEGIN
    INSERT INTO CodeReviews (ObjectName, Reviewer, ReviewStatus, Comments)
    VALUES (@ObjectName, @Reviewer, 'PENDING', @Comments);
    
    SELECT 'Code submitted for review' AS Status;
END;

-- Approve code
CREATE PROCEDURE sp_ApproveCode
    @ObjectName NVARCHAR(128),
    @ApprovedBy NVARCHAR(100),
    @Comments NVARCHAR(MAX) = NULL
AS
BEGIN
    UPDATE CodeReviews
    SET ReviewStatus = 'APPROVED',
        ApprovedBy = @ApprovedBy,
        Comments = ISNULL(@Comments, Comments)
    WHERE ObjectName = @ObjectName
        AND ReviewStatus = 'PENDING';
    
    SELECT 'Code approved successfully' AS Status;
END;

-- Submit for review
EXEC sp_SubmitForReview 
    @ObjectName = 'sp_CalculateCustomerLifetimeValue',
    @Reviewer = 'Senior Developer',
    @Comments = 'Please review the error handling logic';

-- Approve code
EXEC sp_ApproveCode 
    @ObjectName = 'sp_CalculateCustomerLifetimeValue',
    @ApprovedBy = 'Senior Developer',
    @Comments = 'Approved with minor suggestions';
```

### Deployment Tracking
```sql
-- Example 3: Deployment tracking
CREATE TABLE Deployments (
    DeploymentID INT IDENTITY(1,1) PRIMARY KEY,
    ObjectName NVARCHAR(128) NOT NULL,
    VersionNumber NVARCHAR(20) NOT NULL,
    Environment NVARCHAR(50) NOT NULL,
    DeployedBy NVARCHAR(100) NOT NULL,
    DeploymentDate DATETIME2 DEFAULT GETDATE(),
    DeploymentStatus NVARCHAR(20) NOT NULL,
    RollbackDate DATETIME2 NULL
);

-- Deployment procedure
CREATE PROCEDURE sp_DeployObject
    @ObjectName NVARCHAR(128),
    @VersionNumber NVARCHAR(20),
    @Environment NVARCHAR(50),
    @DeployedBy NVARCHAR(100)
AS
BEGIN
    BEGIN TRY
        BEGIN TRANSACTION;
        
        -- Record deployment
        INSERT INTO Deployments (ObjectName, VersionNumber, Environment, DeployedBy, DeploymentStatus)
        VALUES (@ObjectName, @VersionNumber, @Environment, @DeployedBy, 'SUCCESS');
        
        -- Update version tracking
        UPDATE CodeVersions
        SET DeploymentDate = GETDATE()
        WHERE ObjectName = @ObjectName AND VersionNumber = @VersionNumber;
        
        COMMIT TRANSACTION;
        
        SELECT 'Deployment recorded successfully' AS Status;
        
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;
        
        -- Record failed deployment
        INSERT INTO Deployments (ObjectName, VersionNumber, Environment, DeployedBy, DeploymentStatus)
        VALUES (@ObjectName, @VersionNumber, @Environment, @DeployedBy, 'FAILED');
        
        RAISERROR('Deployment failed: %s', 16, 1, ERROR_MESSAGE());
    END CATCH;
END;

-- Deploy object
EXEC sp_DeployObject 
    @ObjectName = 'sp_CalculateCustomerLifetimeValue',
    @VersionNumber = '1.1',
    @Environment = 'PRODUCTION',
    @DeployedBy = 'DevOps Team';
```

## 93. How do you implement data migration strategies in T-SQL?

### Schema Migration
```sql
-- Example 1: Schema migration tracking
CREATE TABLE SchemaMigrations (
    MigrationID INT IDENTITY(1,1) PRIMARY KEY,
    MigrationName NVARCHAR(100) NOT NULL,
    AppliedDate DATETIME2 DEFAULT GETDATE(),
    AppliedBy NVARCHAR(100) NOT NULL,
    RollbackScript NVARCHAR(MAX)
);

-- Migration procedure
CREATE PROCEDURE sp_ApplyMigration
    @MigrationName NVARCHAR(100),
    @MigrationScript NVARCHAR(MAX),
    @RollbackScript NVARCHAR(MAX) = NULL,
    @AppliedBy NVARCHAR(100)
AS
BEGIN
    BEGIN TRY
        BEGIN TRANSACTION;
        
        -- Check if migration already applied
        IF EXISTS (SELECT 1 FROM SchemaMigrations WHERE MigrationName = @MigrationName)
        BEGIN
            RAISERROR('Migration %s already applied', 16, 1, @MigrationName);
            RETURN;
        END;
        
        -- Execute migration script
        EXEC sp_executesql @MigrationScript;
        
        -- Record migration
        INSERT INTO SchemaMigrations (MigrationName, AppliedBy, RollbackScript)
        VALUES (@MigrationName, @AppliedBy, @RollbackScript);
        
        COMMIT TRANSACTION;
        
        SELECT 'Migration applied successfully' AS Status;
        
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;
        
        RAISERROR('Migration failed: %s', 16, 1, ERROR_MESSAGE());
    END CATCH;
END;

-- Apply migration
DECLARE @MigrationScript NVARCHAR(MAX) = '
CREATE TABLE NewCustomers (
    CustomerID INT IDENTITY(1,1) PRIMARY KEY,
    CustomerName NVARCHAR(100) NOT NULL,
    Email NVARCHAR(100),
    Phone NVARCHAR(20),
    CreatedDate DATETIME2 DEFAULT GETDATE()
);';

DECLARE @RollbackScript NVARCHAR(MAX) = 'DROP TABLE NewCustomers;';

EXEC sp_ApplyMigration 
    @MigrationName = 'CreateNewCustomersTable',
    @MigrationScript = @MigrationScript,
    @RollbackScript = @RollbackScript,
    @AppliedBy = 'Database Admin';
```

### Data Migration
```sql
-- Example 2: Data migration procedure
CREATE PROCEDURE sp_MigrateCustomerData
    @BatchSize INT = 1000,
    @MaxRetries INT = 3
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @ProcessedCount INT = 0;
    DECLARE @TotalCount INT;
    DECLARE @RetryCount INT = 0;
    DECLARE @Success BIT = 0;
    
    -- Get total count
    SELECT @TotalCount = COUNT(*) FROM Customers;
    
    WHILE @ProcessedCount < @TotalCount AND @RetryCount < @MaxRetries
    BEGIN
        BEGIN TRY
            BEGIN TRANSACTION;
            
            -- Migrate batch of customers
            INSERT INTO NewCustomers (CustomerName, Email, Phone, CreatedDate)
            SELECT TOP (@BatchSize)
                CustomerName, Email, Phone, CreatedDate
            FROM Customers
            WHERE CustomerID NOT IN (SELECT CustomerID FROM NewCustomers)
            ORDER BY CustomerID;
            
            SET @ProcessedCount = @ProcessedCount + @@ROWCOUNT;
            SET @Success = 1;
            
            COMMIT TRANSACTION;
            
            PRINT 'Migrated ' + CAST(@ProcessedCount AS VARCHAR(10)) + ' of ' + CAST(@TotalCount AS VARCHAR(10)) + ' customers';
            
        END TRY
        BEGIN CATCH
            IF @@TRANCOUNT > 0
                ROLLBACK TRANSACTION;
            
            SET @RetryCount = @RetryCount + 1;
            SET @Success = 0;
            
            PRINT 'Migration failed, retry ' + CAST(@RetryCount AS VARCHAR(10)) + ': ' + ERROR_MESSAGE();
            
            -- Wait before retry
            WAITFOR DELAY '00:00:05';
        END CATCH;
    END;
    
    -- Final status
    IF @Success = 1
        SELECT 'Data migration completed successfully' AS Status;
    ELSE
        SELECT 'Data migration failed after ' + CAST(@MaxRetries AS VARCHAR(10)) + ' retries' AS Status;
END;

-- Execute data migration
EXEC sp_MigrateCustomerData @BatchSize = 500, @MaxRetries = 3;
```

### Validation After Migration
```sql
-- Example 3: Post-migration validation
CREATE PROCEDURE sp_ValidateMigration
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @SourceCount INT;
    DECLARE @TargetCount INT;
    DECLARE @ValidationPassed BIT = 1;
    
    -- Count source records
    SELECT @SourceCount = COUNT(*) FROM Customers;
    
    -- Count target records
    SELECT @TargetCount = COUNT(*) FROM NewCustomers;
    
    -- Validate record counts
    IF @SourceCount != @TargetCount
    BEGIN
        SET @ValidationPassed = 0;
        SELECT 'Record count mismatch: Source=' + CAST(@SourceCount AS VARCHAR(10)) + 
               ', Target=' + CAST(@TargetCount AS VARCHAR(10)) AS ValidationError;
    END;
    
    -- Validate data integrity
    IF EXISTS (
        SELECT 1 FROM Customers c
        LEFT JOIN NewCustomers nc ON c.CustomerName = nc.CustomerName
        WHERE nc.CustomerName IS NULL
    )
    BEGIN
        SET @ValidationPassed = 0;
        SELECT 'Data integrity check failed: Missing records in target' AS ValidationError;
    END;
    
    -- Final validation result
    IF @ValidationPassed = 1
        SELECT 'Migration validation passed' AS Status;
    ELSE
        SELECT 'Migration validation failed' AS Status;
END;

-- Execute validation
EXEC sp_ValidateMigration;
```

## 94. What are the considerations for T-SQL in cloud environments?

### Azure SQL Database Considerations
```sql
-- Example 1: Azure SQL Database specific features
-- Use Azure-specific data types
CREATE TABLE CloudData (
    ID INT IDENTITY(1,1) PRIMARY KEY,
    Data NVARCHAR(MAX),
    CreatedDate DATETIME2 DEFAULT GETDATE(),
    -- Azure-specific features
    RowVersion ROWVERSION,
    -- JSON data support
    JsonData NVARCHAR(MAX) CHECK (ISJSON(JsonData) = 1)
);

-- Azure-specific functions
SELECT 
    GETDATE() AS CurrentTime,
    SYSUTCDATETIME() AS UTCTime,
    -- Azure region information
    @@SERVERNAME AS ServerName,
    DB_NAME() AS DatabaseName;
```

### Elastic Query Implementation
```sql
-- Example 2: Elastic query for cross-database queries
-- Create external data source
CREATE EXTERNAL DATA SOURCE RemoteDataSource
WITH (
    TYPE = RDBMS,
    LOCATION = 'your-server.database.windows.net',
    DATABASE_NAME = 'RemoteDatabase',
    CREDENTIAL = RemoteCredential
);

-- Create external table
CREATE EXTERNAL TABLE RemoteCustomers (
    CustomerID INT,
    CustomerName NVARCHAR(100),
    Email NVARCHAR(100)
)
WITH (
    DATA_SOURCE = RemoteDataSource,
    SCHEMA_NAME = 'dbo',
    OBJECT_NAME = 'Customers'
);

-- Query across databases
SELECT 
    lc.CustomerName AS LocalCustomer,
    rc.CustomerName AS RemoteCustomer
FROM Customers lc
FULL OUTER JOIN RemoteCustomers rc ON lc.CustomerID = rc.CustomerID;
```

### Cloud Performance Optimization
```sql
-- Example 3: Cloud performance optimization
-- Use table-valued parameters for batch operations
CREATE TYPE CustomerTableType AS TABLE (
    CustomerName NVARCHAR(100),
    Email NVARCHAR(100),
    Phone NVARCHAR(20)
);

CREATE PROCEDURE sp_BatchInsertCustomers
    @Customers CustomerTableType READONLY
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Batch insert with error handling
    INSERT INTO Customers (CustomerName, Email, Phone)
    SELECT CustomerName, Email, Phone
    FROM @Customers
    WHERE CustomerName IS NOT NULL;
    
    SELECT @@ROWCOUNT AS InsertedRecords;
END;

-- Use the procedure
DECLARE @CustomerData CustomerTableType;
INSERT INTO @CustomerData VALUES 
    ('John Smith', 'john@example.com', '555-1234'),
    ('Jane Doe', 'jane@example.com', '555-5678');

EXEC sp_BatchInsertCustomers @Customers = @CustomerData;
```

### Cloud Security Implementation
```sql
-- Example 4: Cloud security features
-- Row-level security for multi-tenant data
CREATE FUNCTION dbo.fn_TenantSecurity(@TenantID INT)
RETURNS TABLE
WITH SCHEMABINDING
AS
RETURN SELECT 1 AS fn_securitypredicate_result
WHERE @TenantID = CAST(SESSION_CONTEXT(N'TenantID') AS INT);

-- Apply security policy
CREATE SECURITY POLICY dbo.TenantSecurityPolicy
ADD FILTER PREDICATE dbo.fn_TenantSecurity(TenantID) ON dbo.Orders;

-- Set tenant context
EXEC sp_set_session_context @key = 'TenantID', @value = 1;

-- Query with tenant isolation
SELECT * FROM Orders; -- Only returns orders for tenant 1
```

## 95. How do you implement real-time data processing in T-SQL?

### Change Data Capture (CDC)
```sql
-- Example 1: Enable CDC for real-time tracking
-- Enable CDC on database
EXEC sys.sp_cdc_enable_db;

-- Enable CDC on table
EXEC sys.sp_cdc_enable_table
    @source_schema = 'dbo',
    @source_name = 'Orders',
    @role_name = 'cdc_admin';

-- Query CDC data
SELECT 
    __$start_lsn,
    __$end_lsn,
    __$seqval,
    __$operation,
    __$update_mask,
    OrderID,
    CustomerID,
    OrderDate,
    TotalAmount,
    Status
FROM cdc.dbo_Orders_CT
WHERE __$operation IN (1, 2, 4); -- Insert, Update, Delete
```

### Temporal Tables
```sql
-- Example 2: Temporal tables for automatic history tracking
CREATE TABLE Products (
    ProductID INT IDENTITY(1,1) PRIMARY KEY,
    ProductName NVARCHAR(100) NOT NULL,
    UnitPrice DECIMAL(10,2) NOT NULL,
    StockQuantity INT NOT NULL,
    -- Temporal columns
    ValidFrom DATETIME2 GENERATED ALWAYS AS ROW START,
    ValidTo DATETIME2 GENERATED ALWAYS AS ROW END,
    PERIOD FOR SYSTEM_TIME (ValidFrom, ValidTo)
)
WITH (SYSTEM_VERSIONING = ON);

-- Insert test data
INSERT INTO Products (ProductName, UnitPrice, StockQuantity)
VALUES ('Laptop', 999.99, 10);

-- Update to create history
UPDATE Products
SET UnitPrice = 899.99
WHERE ProductID = 1;

-- Query current data
SELECT * FROM Products;

-- Query historical data
SELECT * FROM Products
FOR SYSTEM_TIME AS OF '2024-01-15 10:00:00';
```

### Real-time Monitoring
```sql
-- Example 3: Real-time monitoring procedure
CREATE PROCEDURE sp_RealTimeMonitoring
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Monitor active connections
    SELECT 
        session_id,
        login_name,
        host_name,
        program_name,
        status,
        cpu_time,
        memory_usage,
        last_request_start_time
    FROM sys.dm_exec_sessions
    WHERE is_user_process = 1
    ORDER BY cpu_time DESC;
    
    -- Monitor blocking
    SELECT 
        blocking_session_id,
        session_id,
        wait_type,
        wait_time,
        wait_resource
    FROM sys.dm_exec_requests
    WHERE blocking_session_id > 0;
    
    -- Monitor performance
    SELECT 
        'CPU Usage' AS Metric,
        (SELECT cpu_percent FROM sys.dm_os_ring_buffers WHERE ring_buffer_type = 'RING_BUFFER_SCHEDULER_MONITOR') AS Value
    UNION ALL
    SELECT 
        'Memory Usage',
        (SELECT (total_physical_memory_kb - available_physical_memory_kb) * 100.0 / total_physical_memory_kb FROM sys.dm_os_sys_info);
END;

-- Execute monitoring
EXEC sp_RealTimeMonitoring;
```

### Event-driven Processing
```sql
-- Example 4: Event-driven processing with triggers
CREATE TABLE OrderEvents (
    EventID INT IDENTITY(1,1) PRIMARY KEY,
    OrderID INT NOT NULL,
    EventType NVARCHAR(50) NOT NULL,
    EventData NVARCHAR(MAX),
    EventTime DATETIME2 DEFAULT GETDATE()
);

-- Trigger for order events
CREATE TRIGGER tr_OrderEvents
ON Orders
AFTER INSERT, UPDATE, DELETE
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Insert events
    IF EXISTS (SELECT 1 FROM inserted)
    BEGIN
        INSERT INTO OrderEvents (OrderID, EventType, EventData)
        SELECT 
            OrderID,
            'ORDER_CREATED',
            JSON_OBJECT('CustomerID', CustomerID, 'TotalAmount', TotalAmount)
        FROM inserted;
    END;
    
    -- Update events
    IF EXISTS (SELECT 1 FROM inserted) AND EXISTS (SELECT 1 FROM deleted)
    BEGIN
        INSERT INTO OrderEvents (OrderID, EventType, EventData)
        SELECT 
            i.OrderID,
            'ORDER_UPDATED',
            JSON_OBJECT('OldStatus', d.Status, 'NewStatus', i.Status)
        FROM inserted i
        INNER JOIN deleted d ON i.OrderID = d.OrderID
        WHERE i.Status != d.Status;
    END;
    
    -- Delete events
    IF EXISTS (SELECT 1 FROM deleted) AND NOT EXISTS (SELECT 1 FROM inserted)
    BEGIN
        INSERT INTO OrderEvents (OrderID, EventType, EventData)
        SELECT 
            OrderID,
            'ORDER_DELETED',
            JSON_OBJECT('CustomerID', CustomerID, 'TotalAmount', TotalAmount)
        FROM deleted;
    END;
END;

-- Test event processing
INSERT INTO Orders (CustomerID, OrderDate, TotalAmount, Status)
VALUES (101, GETDATE(), 150.00, 'Pending');

-- Check events
SELECT * FROM OrderEvents ORDER BY EventTime DESC;
```

## 96. What are the advanced T-SQL techniques for data analysis?

### Advanced Analytics Functions
```sql
-- Example 1: Advanced analytics with window functions
SELECT 
    CustomerID,
    OrderDate,
    TotalAmount,
    -- Running totals
    SUM(TotalAmount) OVER (PARTITION BY CustomerID ORDER BY OrderDate) AS CustomerRunningTotal,
    -- Percentile calculations
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY TotalAmount) OVER (PARTITION BY CustomerID) AS CustomerMedian,
    -- Statistical functions
    AVG(TotalAmount) OVER (PARTITION BY CustomerID) AS CustomerAverage,
    STDEV(TotalAmount) OVER (PARTITION BY CustomerID) AS CustomerStdDev,
    -- Ranking functions
    ROW_NUMBER() OVER (PARTITION BY CustomerID ORDER BY TotalAmount DESC) AS CustomerOrderRank,
    -- Lead/Lag analysis
    LAG(TotalAmount, 1) OVER (PARTITION BY CustomerID ORDER BY OrderDate) AS PreviousOrderAmount,
    LEAD(TotalAmount, 1) OVER (PARTITION BY CustomerID ORDER BY OrderDate) AS NextOrderAmount
FROM Orders
ORDER BY CustomerID, OrderDate;
```

### Time Series Analysis
```sql
-- Example 2: Time series analysis
WITH TimeSeriesData AS (
    SELECT 
        CAST(OrderDate AS DATE) AS OrderDate,
        SUM(TotalAmount) AS DailyTotal,
        COUNT(*) AS DailyOrders
    FROM Orders
    WHERE OrderDate >= DATEADD(MONTH, -12, GETDATE())
    GROUP BY CAST(OrderDate AS DATE)
),
MovingAverages AS (
    SELECT 
        OrderDate,
        DailyTotal,
        DailyOrders,
        -- 7-day moving average
        AVG(DailyTotal) OVER (ORDER BY OrderDate ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS SevenDayAverage,
        -- 30-day moving average
        AVG(DailyTotal) OVER (ORDER BY OrderDate ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) AS ThirtyDayAverage,
        -- Growth rate
        LAG(DailyTotal, 1) OVER (ORDER BY OrderDate) AS PreviousDayTotal
    FROM TimeSeriesData
)
SELECT 
    OrderDate,
    DailyTotal,
    DailyOrders,
    SevenDayAverage,
    ThirtyDayAverage,
    CASE 
        WHEN PreviousDayTotal > 0 THEN (DailyTotal - PreviousDayTotal) * 100.0 / PreviousDayTotal
        ELSE NULL
    END AS GrowthRate
FROM MovingAverages
ORDER BY OrderDate;
```

### Customer Segmentation
```sql
-- Example 3: Customer segmentation analysis
WITH CustomerMetrics AS (
    SELECT 
        c.CustomerID,
        c.CustomerName,
        COUNT(o.OrderID) AS OrderCount,
        SUM(o.TotalAmount) AS TotalSpent,
        AVG(o.TotalAmount) AS AverageOrderValue,
        DATEDIFF(DAY, MIN(o.OrderDate), MAX(o.OrderDate)) AS CustomerLifespan,
        DATEDIFF(DAY, MAX(o.OrderDate), GETDATE()) AS DaysSinceLastOrder
    FROM Customers c
    LEFT JOIN Orders o ON c.CustomerID = o.CustomerID
    GROUP BY c.CustomerID, c.CustomerName
),
SegmentedCustomers AS (
    SELECT 
        CustomerID,
        CustomerName,
        OrderCount,
        TotalSpent,
        AverageOrderValue,
        CustomerLifespan,
        DaysSinceLastOrder,
        -- RFM Analysis
        NTILE(5) OVER (ORDER BY TotalSpent DESC) AS RecencyScore,
        NTILE(5) OVER (ORDER BY OrderCount DESC) AS FrequencyScore,
        NTILE(5) OVER (ORDER BY AverageOrderValue DESC) AS MonetaryScore,
        -- Customer segments
        CASE 
            WHEN TotalSpent > 10000 AND OrderCount > 20 THEN 'Champions'
            WHEN TotalSpent > 5000 AND OrderCount > 10 THEN 'Loyal Customers'
            WHEN TotalSpent > 1000 AND OrderCount > 5 THEN 'Potential Loyalists'
            WHEN DaysSinceLastOrder > 90 THEN 'At Risk'
            WHEN DaysSinceLastOrder > 180 THEN 'Cannot Lose Them'
            ELSE 'New Customers'
        END AS CustomerSegment
    FROM CustomerMetrics
)
SELECT 
    CustomerSegment,
    COUNT(*) AS CustomerCount,
    AVG(TotalSpent) AS AvgTotalSpent,
    AVG(OrderCount) AS AvgOrderCount,
    AVG(AverageOrderValue) AS AvgOrderValue
FROM SegmentedCustomers
GROUP BY CustomerSegment
ORDER BY AvgTotalSpent DESC;
```

### Predictive Analytics
```sql
-- Example 4: Predictive analytics with T-SQL
WITH CustomerBehavior AS (
    SELECT 
        CustomerID,
        OrderDate,
        TotalAmount,
        -- Calculate order frequency
        DATEDIFF(DAY, LAG(OrderDate, 1) OVER (PARTITION BY CustomerID ORDER BY OrderDate), OrderDate) AS DaysBetweenOrders,
        -- Calculate spending trend
        TotalAmount - LAG(TotalAmount, 1) OVER (PARTITION BY CustomerID ORDER BY OrderDate) AS SpendingChange,
        -- Calculate order velocity
        COUNT(*) OVER (PARTITION BY CustomerID ORDER BY OrderDate ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS OrdersLast7Days
    FROM Orders
    WHERE OrderDate >= DATEADD(MONTH, -6, GETDATE())
),
PredictiveMetrics AS (
    SELECT 
        CustomerID,
        AVG(DaysBetweenOrders) AS AvgDaysBetweenOrders,
        AVG(SpendingChange) AS AvgSpendingChange,
        MAX(OrdersLast7Days) AS MaxOrdersLast7Days,
        -- Predict next order date
        MAX(OrderDate) + AVG(DaysBetweenOrders) AS PredictedNextOrderDate,
        -- Predict next order amount
        AVG(TotalAmount) + AVG(SpendingChange) AS PredictedNextOrderAmount
    FROM CustomerBehavior
    GROUP BY CustomerID
)
SELECT 
    CustomerID,
    AvgDaysBetweenOrders,
    AvgSpendingChange,
    MaxOrdersLast7Days,
    PredictedNextOrderDate,
    PredictedNextOrderAmount,
    -- Risk assessment
    CASE 
        WHEN PredictedNextOrderDate < GETDATE() THEN 'High Risk - Overdue'
        WHEN PredictedNextOrderDate < DATEADD(DAY, 7, GETDATE()) THEN 'Medium Risk - Due Soon'
        ELSE 'Low Risk - On Track'
    END AS RiskLevel
FROM PredictiveMetrics
ORDER BY PredictedNextOrderDate;
```

## 97. How do you implement data compression in T-SQL?

### Row Compression
```sql
-- Example 1: Row compression implementation
-- Create table with row compression
CREATE TABLE CompressedOrders (
    OrderID INT IDENTITY(1,1) PRIMARY KEY,
    CustomerID INT NOT NULL,
    OrderDate DATETIME2 NOT NULL,
    TotalAmount DECIMAL(10,2) NOT NULL,
    Status NVARCHAR(20) NOT NULL,
    OrderDetails NVARCHAR(MAX)
) WITH (DATA_COMPRESSION = ROW);

-- Insert test data
INSERT INTO CompressedOrders (CustomerID, OrderDate, TotalAmount, Status, OrderDetails)
VALUES 
    (101, '2024-01-15', 150.00, 'Completed', 'Laptop, Mouse, Keyboard'),
    (102, '2024-01-16', 200.00, 'Pending', 'Tablet, Case'),
    (103, '2024-01-17', 300.00, 'Completed', 'Smartphone, Charger');

-- Check compression ratio
SELECT 
    OBJECT_NAME(object_id) AS TableName,
    data_compression_desc,
    page_count,
    compressed_page_count,
    (page_count - compressed_page_count) * 100.0 / page_count AS CompressionRatio
FROM sys.dm_db_index_physical_stats(DB_ID(), OBJECT_ID('CompressedOrders'), NULL, NULL, 'DETAILED')
WHERE index_id = 1;
```

**Result:**
```
TableName | data_compression_desc | page_count | compressed_page_count | CompressionRatio
----------|----------------------|------------|----------------------|-----------------
CompressedOrders | ROW | 1 | 1 | 0.00
```

### Page Compression
```sql
-- Example 2: Page compression implementation
-- Create table with page compression
CREATE TABLE PageCompressedProducts (
    ProductID INT IDENTITY(1,1) PRIMARY KEY,
    ProductName NVARCHAR(100) NOT NULL,
    CategoryName NVARCHAR(50) NOT NULL,
    BrandName NVARCHAR(50) NOT NULL,
    UnitPrice DECIMAL(10,2) NOT NULL,
    Description NVARCHAR(MAX)
) WITH (DATA_COMPRESSION = PAGE);

-- Insert test data with repetitive values
INSERT INTO PageCompressedProducts (ProductName, CategoryName, BrandName, UnitPrice, Description)
VALUES 
    ('Laptop Pro', 'Electronics', 'TechBrand', 999.99, 'High-performance laptop'),
    ('Laptop Air', 'Electronics', 'TechBrand', 799.99, 'Lightweight laptop'),
    ('Gaming Mouse', 'Electronics', 'TechBrand', 49.99, 'Precision gaming mouse'),
    ('Office Chair', 'Furniture', 'ComfortBrand', 299.99, 'Ergonomic office chair'),
    ('Desk Lamp', 'Furniture', 'ComfortBrand', 59.99, 'LED desk lamp');

-- Check compression effectiveness
SELECT 
    OBJECT_NAME(object_id) AS TableName,
    data_compression_desc,
    page_count,
    compressed_page_count,
    (page_count - compressed_page_count) * 100.0 / page_count AS CompressionRatio
FROM sys.dm_db_index_physical_stats(DB_ID(), OBJECT_ID('PageCompressedProducts'), NULL, NULL, 'DETAILED')
WHERE index_id = 1;
```

### Columnstore Compression
```sql
-- Example 3: Columnstore compression for analytics
-- Create columnstore index
CREATE NONCLUSTERED COLUMNSTORE INDEX IX_Orders_Columnstore
ON Orders (CustomerID, OrderDate, TotalAmount, Status);

-- Query with columnstore
SELECT 
    CustomerID,
    COUNT(*) AS OrderCount,
    SUM(TotalAmount) AS TotalSpent,
    AVG(TotalAmount) AS AverageOrderValue
FROM Orders
WHERE OrderDate >= '2024-01-01'
GROUP BY CustomerID
ORDER BY TotalSpent DESC;

-- Check columnstore compression
SELECT 
    OBJECT_NAME(object_id) AS TableName,
    index_type_desc,
    data_compression_desc,
    page_count,
    compressed_page_count,
    (page_count - compressed_page_count) * 100.0 / page_count AS CompressionRatio
FROM sys.dm_db_index_physical_stats(DB_ID(), OBJECT_ID('Orders'), NULL, NULL, 'DETAILED')
WHERE index_id > 1;
```

## 98. What are the considerations for T-SQL in high-availability environments?

### Always On Availability Groups
```sql
-- Example 1: Always On monitoring
-- Check availability group status
SELECT 
    ag.name AS AvailabilityGroupName,
    ar.replica_server_name,
    ar.availability_mode_desc,
    ar.failover_mode_desc,
    ar.primary_role_allow_connections_desc,
    ar.secondary_role_allow_connections_desc,
    ar.backup_priority,
    ar.read_only_routing_url
FROM sys.availability_groups ag
INNER JOIN sys.availability_replicas ar ON ag.group_id = ar.group_id;

-- Check database synchronization status
SELECT 
    database_name,
    synchronization_state_desc,
    synchronization_health_desc,
    is_suspended,
    suspend_reason_desc
FROM sys.dm_hadr_database_replica_states
WHERE is_local = 1;
```

### Failover Procedures
```sql
-- Example 2: Automated failover procedures
CREATE PROCEDURE sp_CheckFailoverReadiness
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @IsPrimary BIT = 0;
    DECLARE @IsSecondary BIT = 0;
    
    -- Check if current instance is primary
    IF EXISTS (
        SELECT 1 FROM sys.dm_hadr_availability_replica_states
        WHERE is_local = 1 AND role_desc = 'PRIMARY'
    )
        SET @IsPrimary = 1;
    
    -- Check if current instance is secondary
    IF EXISTS (
        SELECT 1 FROM sys.dm_hadr_availability_replica_states
        WHERE is_local = 1 AND role_desc = 'SECONDARY'
    )
        SET @IsSecondary = 1;
    
    -- Return failover readiness
    SELECT 
        CASE 
            WHEN @IsPrimary = 1 THEN 'PRIMARY'
            WHEN @IsSecondary = 1 THEN 'SECONDARY'
            ELSE 'STANDALONE'
        END AS InstanceRole,
        CASE 
            WHEN @IsPrimary = 1 THEN 'Ready for failover'
            WHEN @IsSecondary = 1 THEN 'Ready to become primary'
            ELSE 'Not in availability group'
        END AS FailoverStatus;
END;

-- Execute failover check
EXEC sp_CheckFailoverReadiness;
```

### Load Balancing
```sql
-- Example 3: Read-only routing for load balancing
-- Configure read-only routing
ALTER AVAILABILITY GROUP [YourAvailabilityGroup]
MODIFY REPLICA ON 'SecondaryServer' 
WITH (SECONDARY_ROLE (READ_ONLY_ROUTING_URL = 'TCP://SecondaryServer:1433'));

-- Query with read-only routing
SELECT 
    CustomerID,
    CustomerName,
    Email,
    Phone
FROM Customers
WHERE CustomerID = 101;

-- Check which replica handled the query
SELECT 
    @@SERVERNAME AS ServerName,
    DB_NAME() AS DatabaseName,
    GETDATE() AS QueryTime;
```

## 99. How do you implement data partitioning strategies in T-SQL?

### Table Partitioning
```sql
-- Example 1: Range partitioning by date
-- Create partition function
CREATE PARTITION FUNCTION pf_OrderDate (DATETIME2)
AS RANGE RIGHT FOR VALUES (
    '2023-01-01',
    '2024-01-01',
    '2025-01-01'
);

-- Create partition scheme
CREATE PARTITION SCHEME ps_OrderDate
AS PARTITION pf_OrderDate
TO ([PRIMARY], [PRIMARY], [PRIMARY], [PRIMARY]);

-- Create partitioned table
CREATE TABLE PartitionedOrders (
    OrderID INT IDENTITY(1,1),
    CustomerID INT NOT NULL,
    OrderDate DATETIME2 NOT NULL,
    TotalAmount DECIMAL(10,2) NOT NULL,
    Status NVARCHAR(20) NOT NULL,
    CONSTRAINT PK_PartitionedOrders PRIMARY KEY (OrderID, OrderDate)
) ON ps_OrderDate(OrderDate);

-- Insert test data
INSERT INTO PartitionedOrders (CustomerID, OrderDate, TotalAmount, Status)
VALUES 
    (101, '2023-06-15', 150.00, 'Completed'),
    (102, '2024-01-15', 200.00, 'Pending'),
    (103, '2024-06-15', 300.00, 'Completed'),
    (104, '2025-01-15', 400.00, 'Pending');

-- Query specific partition
SELECT * FROM PartitionedOrders
WHERE OrderDate >= '2024-01-01' AND OrderDate < '2025-01-01';
```

### Partition Maintenance
```sql
-- Example 2: Partition maintenance procedures
CREATE PROCEDURE sp_MaintainPartitions
    @PartitionDate DATETIME2
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Add new partition
    ALTER PARTITION SCHEME ps_OrderDate
    NEXT USED [PRIMARY];
    
    ALTER PARTITION FUNCTION pf_OrderDate()
    SPLIT RANGE (@PartitionDate);
    
    -- Archive old partition
    ALTER TABLE PartitionedOrders
    SWITCH PARTITION 1 TO OrdersArchive PARTITION 1;
    
    -- Update statistics
    UPDATE STATISTICS PartitionedOrders;
    
    SELECT 'Partition maintenance completed' AS Status;
END;

-- Execute partition maintenance
EXEC sp_MaintainPartitions @PartitionDate = '2026-01-01';
```

### Partitioned Indexes
```sql
-- Example 3: Partitioned indexes
-- Create partitioned index
CREATE NONCLUSTERED INDEX IX_PartitionedOrders_CustomerID
ON PartitionedOrders (CustomerID, OrderDate)
ON ps_OrderDate(OrderDate);

-- Query with partition elimination
SELECT 
    CustomerID,
    COUNT(*) AS OrderCount,
    SUM(TotalAmount) AS TotalSpent
FROM PartitionedOrders
WHERE OrderDate >= '2024-01-01'
    AND OrderDate < '2025-01-01'
GROUP BY CustomerID;

-- Check partition usage
SELECT 
    partition_number,
    rows,
    data_compression_desc
FROM sys.dm_db_partition_stats
WHERE object_id = OBJECT_ID('PartitionedOrders')
ORDER BY partition_number;
```

## 100. What are the best practices for T-SQL performance tuning?

### Query Optimization
```sql
-- Example 1: Query optimization techniques
-- Use proper indexing
CREATE NONCLUSTERED INDEX IX_Orders_Customer_Date_Covering
ON Orders (CustomerID, OrderDate)
INCLUDE (TotalAmount, Status);

-- Optimize query with proper WHERE clause
SELECT 
    c.CustomerName,
    o.OrderDate,
    o.TotalAmount,
    o.Status
FROM Customers c
INNER JOIN Orders o ON c.CustomerID = o.CustomerID
WHERE o.OrderDate >= '2024-01-01'
    AND o.Status = 'Completed'
ORDER BY o.OrderDate DESC;

-- Use EXISTS instead of IN for better performance
SELECT CustomerID, CustomerName
FROM Customers c
WHERE EXISTS (
    SELECT 1 FROM Orders o
    WHERE o.CustomerID = c.CustomerID
        AND o.OrderDate >= '2024-01-01'
);
```

### Performance Monitoring
```sql
-- Example 2: Performance monitoring queries
-- Monitor query performance
SELECT 
    qs.sql_handle,
    qt.text AS QueryText,
    qs.execution_count,
    qs.total_elapsed_time / 1000 AS TotalElapsedTimeMs,
    qs.total_elapsed_time / qs.execution_count / 1000 AS AvgElapsedTimeMs,
    qs.total_logical_reads,
    qs.total_physical_reads,
    qs.creation_time,
    qs.last_execution_time
FROM sys.dm_exec_query_stats qs
CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) qt
WHERE qs.total_elapsed_time > 1000000 -- Queries taking more than 1 second
ORDER BY qs.total_elapsed_time DESC;

-- Monitor index usage
SELECT 
    OBJECT_NAME(i.object_id) AS TableName,
    i.name AS IndexName,
    s.user_seeks,
    s.user_scans,
    s.user_lookups,
    s.user_updates,
    s.last_user_seek,
    s.last_user_scan
FROM sys.dm_db_index_usage_stats s
INNER JOIN sys.indexes i ON s.object_id = i.object_id AND s.index_id = i.index_id
WHERE s.database_id = DB_ID()
    AND i.name IS NOT NULL
ORDER BY s.user_seeks + s.user_scans + s.user_lookups DESC;
```

### Resource Optimization
```sql
-- Example 3: Resource optimization
-- Monitor memory usage
SELECT 
    'Buffer Pool' AS MemoryType,
    (SELECT COUNT(*) * 8 / 1024.0 FROM sys.dm_os_buffer_descriptors) AS SizeMB
UNION ALL
SELECT 
    'Plan Cache',
    (SELECT SUM(size_in_bytes) / 1024.0 / 1024.0 FROM sys.dm_exec_cached_plans)
UNION ALL
SELECT 
    'Total Memory',
    (SELECT total_physical_memory_kb / 1024.0 FROM sys.dm_os_sys_info);

-- Monitor CPU usage
SELECT 
    'CPU Usage' AS Metric,
    (SELECT cpu_percent FROM sys.dm_os_ring_buffers 
     WHERE ring_buffer_type = 'RING_BUFFER_SCHEDULER_MONITOR') AS Value
UNION ALL
SELECT 
    'Memory Usage',
    (SELECT (total_physical_memory_kb - available_physical_memory_kb) * 100.0 / total_physical_memory_kb 
     FROM sys.dm_os_sys_info);
```

### Best Practices Summary
```sql
-- Example 4: Performance best practices implementation
-- 1. Use appropriate data types
CREATE TABLE OptimizedTable (
    ID INT IDENTITY(1,1) PRIMARY KEY,
    Name NVARCHAR(100) NOT NULL, -- Use NVARCHAR for Unicode
    Email NVARCHAR(100),
    CreatedDate DATETIME2 DEFAULT GETDATE(), -- Use DATETIME2 for better precision
    IsActive BIT DEFAULT 1 -- Use BIT for boolean values
);

-- 2. Create proper indexes
CREATE NONCLUSTERED INDEX IX_OptimizedTable_Email
ON OptimizedTable (Email)
WHERE Email IS NOT NULL; -- Filtered index for better performance

-- 3. Use parameterized queries
CREATE PROCEDURE sp_GetCustomerByEmail
    @Email NVARCHAR(100)
AS
BEGIN
    SET NOCOUNT ON;
    
    SELECT CustomerID, CustomerName, Email
    FROM Customers
    WHERE Email = @Email; -- Parameterized query prevents SQL injection
END;

-- 4. Implement proper error handling
CREATE PROCEDURE sp_SafeDataUpdate
    @CustomerID INT,
    @NewName NVARCHAR(100)
AS
BEGIN
    SET NOCOUNT ON;
    
    BEGIN TRY
        BEGIN TRANSACTION;
        
        UPDATE Customers
        SET CustomerName = @NewName,
            LastModified = GETDATE()
        WHERE CustomerID = @CustomerID;
        
        IF @@ROWCOUNT = 0
            RAISERROR('Customer not found', 16, 1);
        
        COMMIT TRANSACTION;
        
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;
        
        RAISERROR('Update failed: %s', 16, 1, ERROR_MESSAGE());
    END CATCH;
END;
```

## 101. How do you implement Azure SQL Database specific features in T-SQL?

### Azure SQL Database Features
```sql
-- Example 1: Azure SQL Database specific functions
-- Use Azure-specific system functions
SELECT 
    @@SERVERNAME AS ServerName,
    DB_NAME() AS DatabaseName,
    SYSUTCDATETIME() AS UTCTime,
    GETDATE() AS LocalTime,
    -- Azure region information
    SERVERPROPERTY('Edition') AS Edition,
    SERVERPROPERTY('EngineEdition') AS EngineEdition;

-- Use Azure-specific data types
CREATE TABLE AzureFeatures (
    ID INT IDENTITY(1,1) PRIMARY KEY,
    Data NVARCHAR(MAX),
    JsonData NVARCHAR(MAX) CHECK (ISJSON(JsonData) = 1),
    CreatedDate DATETIME2 DEFAULT GETDATE(),
    RowVersion ROWVERSION
);

-- Insert JSON data
INSERT INTO AzureFeatures (Data, JsonData)
VALUES 
    ('Sample Data', '{"name": "John", "age": 30, "city": "New York"}'),
    ('Another Data', '{"name": "Jane", "age": 25, "city": "Los Angeles"}');

-- Query JSON data
SELECT 
    ID,
    Data,
    JSON_VALUE(JsonData, '$.name') AS Name,
    JSON_VALUE(JsonData, '$.age') AS Age,
    JSON_VALUE(JsonData, '$.city') AS City
FROM AzureFeatures;
```

**Result:**
```
ID | Data         | Name | Age | City
---|--------------|------|-----|----------
1  | Sample Data  | John | 30  | New York
2  | Another Data | Jane | 25  | Los Angeles
```

### Elastic Query Implementation
```sql
-- Example 2: Elastic query for cross-database operations
-- Create external data source
CREATE EXTERNAL DATA SOURCE RemoteDataSource
WITH (
    TYPE = RDBMS,
    LOCATION = 'your-server.database.windows.net',
    DATABASE_NAME = 'RemoteDatabase',
    CREDENTIAL = RemoteCredential
);

-- Create external table
CREATE EXTERNAL TABLE RemoteCustomers (
    CustomerID INT,
    CustomerName NVARCHAR(100),
    Email NVARCHAR(100),
    Region NVARCHAR(50)
)
WITH (
    DATA_SOURCE = RemoteDataSource,
    SCHEMA_NAME = 'dbo',
    OBJECT_NAME = 'Customers'
);

-- Query across databases
SELECT 
    'Local' AS Source,
    CustomerID,
    CustomerName,
    Email
FROM Customers
UNION ALL
SELECT 
    'Remote' AS Source,
    CustomerID,
    CustomerName,
    Email
FROM RemoteCustomers;
```

### Azure Security Features
```sql
-- Example 3: Azure security implementation
-- Row-level security for multi-tenant
CREATE FUNCTION dbo.fn_TenantSecurity(@TenantID INT)
RETURNS TABLE
WITH SCHEMABINDING
AS
RETURN SELECT 1 AS fn_securitypredicate_result
WHERE @TenantID = CAST(SESSION_CONTEXT(N'TenantID') AS INT);

-- Apply security policy
CREATE SECURITY POLICY dbo.TenantSecurityPolicy
ADD FILTER PREDICATE dbo.fn_TenantSecurity(TenantID) ON dbo.Orders;

-- Set tenant context
EXEC sp_set_session_context @key = 'TenantID', @value = 1;

-- Query with tenant isolation
SELECT * FROM Orders; -- Only returns orders for tenant 1
```

## 102. How do you implement database monitoring and alerting in T-SQL?

### Performance Monitoring
```sql
-- Example 1: Comprehensive performance monitoring
CREATE PROCEDURE sp_MonitorDatabasePerformance
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Monitor active sessions
    SELECT 
        'Active Sessions' AS Metric,
        COUNT(*) AS Value
    FROM sys.dm_exec_sessions
    WHERE is_user_process = 1
    
    UNION ALL
    
    -- Monitor blocking
    SELECT 
        'Blocking Sessions' AS Metric,
        COUNT(*) AS Value
    FROM sys.dm_exec_requests
    WHERE blocking_session_id > 0
    
    UNION ALL
    
    -- Monitor memory usage
    SELECT 
        'Memory Usage %' AS Metric,
        CAST((total_physical_memory_kb - available_physical_memory_kb) * 100.0 / total_physical_memory_kb AS DECIMAL(5,2)) AS Value
    FROM sys.dm_os_sys_info
    
    UNION ALL
    
    -- Monitor CPU usage
    SELECT 
        'CPU Usage %' AS Metric,
        CAST(cpu_percent AS DECIMAL(5,2)) AS Value
    FROM sys.dm_os_ring_buffers
    WHERE ring_buffer_type = 'RING_BUFFER_SCHEDULER_MONITOR';
END;

-- Execute monitoring
EXEC sp_MonitorDatabasePerformance;
```

**Result:**
```
Metric          | Value
----------------|-------
Active Sessions | 15
Blocking Sessions | 2
Memory Usage %  | 75.50
CPU Usage %     | 45.20
```

### Alert System
```sql
-- Example 2: Database alerting system
CREATE TABLE DatabaseAlerts (
    AlertID INT IDENTITY(1,1) PRIMARY KEY,
    AlertType NVARCHAR(50) NOT NULL,
    AlertMessage NVARCHAR(MAX) NOT NULL,
    AlertLevel NVARCHAR(20) NOT NULL,
    CreatedDate DATETIME2 DEFAULT GETDATE(),
    IsResolved BIT DEFAULT 0
);

CREATE PROCEDURE sp_CheckDatabaseAlerts
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @MemoryUsage DECIMAL(5,2);
    DECLARE @CPUUsage DECIMAL(5,2);
    DECLARE @BlockingCount INT;
    DECLARE @ActiveSessions INT;
    
    -- Get current metrics
    SELECT @MemoryUsage = (total_physical_memory_kb - available_physical_memory_kb) * 100.0 / total_physical_memory_kb
    FROM sys.dm_os_sys_info;
    
    SELECT @CPUUsage = cpu_percent
    FROM sys.dm_os_ring_buffers
    WHERE ring_buffer_type = 'RING_BUFFER_SCHEDULER_MONITOR';
    
    SELECT @BlockingCount = COUNT(*)
    FROM sys.dm_exec_requests
    WHERE blocking_session_id > 0;
    
    SELECT @ActiveSessions = COUNT(*)
    FROM sys.dm_exec_sessions
    WHERE is_user_process = 1;
    
    -- Check for alerts
    IF @MemoryUsage > 90
    BEGIN
        INSERT INTO DatabaseAlerts (AlertType, AlertMessage, AlertLevel)
        VALUES ('Memory', 'Memory usage is above 90%: ' + CAST(@MemoryUsage AS VARCHAR(10)) + '%', 'CRITICAL');
    END;
    
    IF @CPUUsage > 80
    BEGIN
        INSERT INTO DatabaseAlerts (AlertType, AlertMessage, AlertLevel)
        VALUES ('CPU', 'CPU usage is above 80%: ' + CAST(@CPUUsage AS VARCHAR(10)) + '%', 'WARNING');
    END;
    
    IF @BlockingCount > 5
    BEGIN
        INSERT INTO DatabaseAlerts (AlertType, AlertMessage, AlertLevel)
        VALUES ('Blocking', 'High number of blocking sessions: ' + CAST(@BlockingCount AS VARCHAR(10)), 'WARNING');
    END;
    
    IF @ActiveSessions > 100
    BEGIN
        INSERT INTO DatabaseAlerts (AlertType, AlertMessage, AlertLevel)
        VALUES ('Sessions', 'High number of active sessions: ' + CAST(@ActiveSessions AS VARCHAR(10)), 'INFO');
    END;
    
    -- Return current alerts
    SELECT * FROM DatabaseAlerts
    WHERE IsResolved = 0
    ORDER BY CreatedDate DESC;
END;

-- Execute alert check
EXEC sp_CheckDatabaseAlerts;
```

### Automated Monitoring
```sql
-- Example 3: Automated monitoring with thresholds
CREATE PROCEDURE sp_AutomatedMonitoring
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @Thresholds TABLE (
        MetricName NVARCHAR(50),
        WarningThreshold DECIMAL(10,2),
        CriticalThreshold DECIMAL(10,2),
        CurrentValue DECIMAL(10,2)
    );
    
    -- Define thresholds and get current values
    INSERT INTO @Thresholds (MetricName, WarningThreshold, CriticalThreshold, CurrentValue)
    VALUES 
        ('MemoryUsage', 80.0, 90.0, (SELECT (total_physical_memory_kb - available_physical_memory_kb) * 100.0 / total_physical_memory_kb FROM sys.dm_os_sys_info)),
        ('CPUUsage', 70.0, 85.0, (SELECT cpu_percent FROM sys.dm_os_ring_buffers WHERE ring_buffer_type = 'RING_BUFFER_SCHEDULER_MONITOR')),
        ('BlockingSessions', 3.0, 10.0, (SELECT COUNT(*) FROM sys.dm_exec_requests WHERE blocking_session_id > 0));
    
    -- Check thresholds and create alerts
    SELECT 
        MetricName,
        CurrentValue,
        CASE 
            WHEN CurrentValue >= CriticalThreshold THEN 'CRITICAL'
            WHEN CurrentValue >= WarningThreshold THEN 'WARNING'
            ELSE 'OK'
        END AS AlertLevel,
        CASE 
            WHEN CurrentValue >= CriticalThreshold THEN 'Critical threshold exceeded'
            WHEN CurrentValue >= WarningThreshold THEN 'Warning threshold exceeded'
            ELSE 'All metrics within normal range'
        END AS AlertMessage
    FROM @Thresholds;
END;

-- Execute automated monitoring
EXEC sp_AutomatedMonitoring;
```

## 103. How do you implement data archiving and retention policies in T-SQL?

### Automated Archiving
```sql
-- Example 1: Automated data archiving
CREATE PROCEDURE sp_ArchiveOldData
    @RetentionDays INT = 365,
    @BatchSize INT = 1000
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @CutoffDate DATETIME2 = DATEADD(DAY, -@RetentionDays, GETDATE());
    DECLARE @ProcessedCount INT = 0;
    DECLARE @TotalCount INT;
    DECLARE @StartTime DATETIME2 = GETDATE();
    
    -- Get total count of records to archive
    SELECT @TotalCount = COUNT(*)
    FROM Orders
    WHERE OrderDate < @CutoffDate
        AND Status = 'Completed';
    
    PRINT 'Found ' + CAST(@TotalCount AS VARCHAR(10)) + ' records to archive';
    
    WHILE @ProcessedCount < @TotalCount
    BEGIN
        BEGIN TRY
            BEGIN TRANSACTION;
            
            -- Archive batch of records
            INSERT INTO OrdersArchive (OrderID, CustomerID, OrderDate, TotalAmount, Status, ArchivedDate)
            SELECT TOP (@BatchSize)
                OrderID, CustomerID, OrderDate, TotalAmount, Status, GETDATE()
            FROM Orders
            WHERE OrderDate < @CutoffDate
                AND Status = 'Completed'
                AND OrderID NOT IN (SELECT OrderID FROM OrdersArchive);
            
            SET @ProcessedCount = @ProcessedCount + @@ROWCOUNT;
            
            -- Delete archived records from main table
            DELETE TOP (@BatchSize)
            FROM Orders
            WHERE OrderDate < @CutoffDate
                AND Status = 'Completed'
                AND OrderID IN (SELECT OrderID FROM OrdersArchive);
            
            COMMIT TRANSACTION;
            
            PRINT 'Archived ' + CAST(@ProcessedCount AS VARCHAR(10)) + ' of ' + CAST(@TotalCount AS VARCHAR(10)) + ' records';
            
            -- Small delay to prevent blocking
            WAITFOR DELAY '00:00:00.100';
            
        END TRY
        BEGIN CATCH
            IF @@TRANCOUNT > 0
                ROLLBACK TRANSACTION;
            
            PRINT 'Archive failed: ' + ERROR_MESSAGE();
            BREAK;
        END CATCH;
    END;
    
    DECLARE @EndTime DATETIME2 = GETDATE();
    SELECT 
        @ProcessedCount AS ArchivedRecords,
        DATEDIFF(SECOND, @StartTime, @EndTime) AS ProcessingTimeSeconds;
END;

-- Execute archiving
EXEC sp_ArchiveOldData @RetentionDays = 365, @BatchSize = 500;
```

**Result:**
```
Found 5000 records to archive
Archived 1000 of 5000 records
Archived 2000 of 5000 records
Archived 3000 of 5000 records
Archived 4000 of 5000 records
Archived 5000 of 5000 records

ArchivedRecords | ProcessingTimeSeconds
----------------|----------------------
5000            | 25
```

### Retention Policy Management
```sql
-- Example 2: Retention policy management
CREATE TABLE RetentionPolicies (
    PolicyID INT IDENTITY(1,1) PRIMARY KEY,
    TableName NVARCHAR(128) NOT NULL,
    RetentionDays INT NOT NULL,
    IsActive BIT DEFAULT 1,
    CreatedDate DATETIME2 DEFAULT GETDATE(),
    LastExecuted DATETIME2 NULL
);

-- Insert retention policies
INSERT INTO RetentionPolicies (TableName, RetentionDays)
VALUES 
    ('Orders', 365),
    ('OrderDetails', 365),
    ('CustomerLogs', 90),
    ('SystemLogs', 30);

-- Retention policy execution
CREATE PROCEDURE sp_ExecuteRetentionPolicies
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @TableName NVARCHAR(128);
    DECLARE @RetentionDays INT;
    DECLARE @CutoffDate DATETIME2;
    DECLARE @SQL NVARCHAR(MAX);
    
    DECLARE policy_cursor CURSOR FOR
    SELECT TableName, RetentionDays
    FROM RetentionPolicies
    WHERE IsActive = 1;
    
    OPEN policy_cursor;
    FETCH NEXT FROM policy_cursor INTO @TableName, @RetentionDays;
    
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @CutoffDate = DATEADD(DAY, -@RetentionDays, GETDATE());
        
        -- Build dynamic SQL for deletion
        SET @SQL = 'DELETE FROM ' + @TableName + 
                   ' WHERE CreatedDate < ''' + CAST(@CutoffDate AS VARCHAR(20)) + '''';
        
        -- Execute deletion
        EXEC sp_executesql @SQL;
        
        -- Update last executed
        UPDATE RetentionPolicies
        SET LastExecuted = GETDATE()
        WHERE TableName = @TableName;
        
        PRINT 'Executed retention policy for ' + @TableName + ' (cutoff: ' + CAST(@CutoffDate AS VARCHAR(20)) + ')';
        
        FETCH NEXT FROM policy_cursor INTO @TableName, @RetentionDays;
    END;
    
    CLOSE policy_cursor;
    DEALLOCATE policy_cursor;
END;

-- Execute retention policies
EXEC sp_ExecuteRetentionPolicies;
```

### Data Lifecycle Management
```sql
-- Example 3: Data lifecycle management
CREATE PROCEDURE sp_ManageDataLifecycle
    @TableName NVARCHAR(128),
    @LifecycleStage NVARCHAR(20) -- 'ACTIVE', 'ARCHIVE', 'DELETE'
AS
BEGIN
    SET NOCOUNT ON;
    
    IF @LifecycleStage = 'ARCHIVE'
    BEGIN
        -- Move data to archive
        INSERT INTO OrdersArchive (OrderID, CustomerID, OrderDate, TotalAmount, Status, ArchivedDate)
        SELECT OrderID, CustomerID, OrderDate, TotalAmount, Status, GETDATE()
        FROM Orders
        WHERE OrderDate < DATEADD(YEAR, -2, GETDATE());
        
        DELETE FROM Orders
        WHERE OrderDate < DATEADD(YEAR, -2, GETDATE());
        
        SELECT 'Data archived successfully' AS Status;
    END
    ELSE IF @LifecycleStage = 'DELETE'
    BEGIN
        -- Delete old archived data
        DELETE FROM OrdersArchive
        WHERE ArchivedDate < DATEADD(YEAR, -5, GETDATE());
        
        SELECT 'Old archived data deleted successfully' AS Status;
    END
    ELSE
    BEGIN
        -- Analyze data age
        SELECT 
            'Active Data' AS DataType,
            COUNT(*) AS RecordCount,
            MIN(OrderDate) AS OldestRecord,
            MAX(OrderDate) AS NewestRecord
        FROM Orders
        WHERE OrderDate >= DATEADD(YEAR, -2, GETDATE())
        
        UNION ALL
        
        SELECT 
            'Archived Data' AS DataType,
            COUNT(*) AS RecordCount,
            MIN(ArchivedDate) AS OldestRecord,
            MAX(ArchivedDate) AS NewestRecord
        FROM OrdersArchive;
    END;
END;

-- Execute lifecycle management
EXEC sp_ManageDataLifecycle @TableName = 'Orders', @LifecycleStage = 'ACTIVE';
```

## 104. How do you implement database disaster recovery procedures in T-SQL?

### Backup Verification
```sql
-- Example 1: Backup verification procedures
CREATE PROCEDURE sp_VerifyBackups
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Check backup history
    SELECT 
        database_name,
        backup_start_date,
        backup_finish_date,
        backup_size,
        backup_type,
        CASE 
            WHEN backup_type = 'D' THEN 'Full'
            WHEN backup_type = 'I' THEN 'Differential'
            WHEN backup_type = 'L' THEN 'Log'
        END AS BackupType,
        DATEDIFF(MINUTE, backup_start_date, backup_finish_date) AS DurationMinutes
    FROM msdb.dbo.backupset
    WHERE database_name = DB_NAME()
    ORDER BY backup_start_date DESC;
    
    -- Check for recent backups
    DECLARE @LastFullBackup DATETIME2;
    DECLARE @LastLogBackup DATETIME2;
    
    SELECT @LastFullBackup = MAX(backup_start_date)
    FROM msdb.dbo.backupset
    WHERE database_name = DB_NAME() AND backup_type = 'D';
    
    SELECT @LastLogBackup = MAX(backup_start_date)
    FROM msdb.dbo.backupset
    WHERE database_name = DB_NAME() AND backup_type = 'L';
    
    -- Alert if backups are too old
    IF @LastFullBackup < DATEADD(DAY, -1, GETDATE())
    BEGIN
        SELECT 'WARNING: No full backup in the last 24 hours' AS Alert;
    END;
    
    IF @LastLogBackup < DATEADD(HOUR, -1, GETDATE())
    BEGIN
        SELECT 'WARNING: No log backup in the last hour' AS Alert;
    END;
    
    SELECT 
        @LastFullBackup AS LastFullBackup,
        @LastLogBackup AS LastLogBackup,
        DATEDIFF(HOUR, @LastFullBackup, GETDATE()) AS HoursSinceFullBackup,
        DATEDIFF(MINUTE, @LastLogBackup, GETDATE()) AS MinutesSinceLogBackup;
END;

-- Execute backup verification
EXEC sp_VerifyBackups;
```

### Recovery Testing
```sql
-- Example 2: Recovery testing procedures
CREATE PROCEDURE sp_TestRecovery
    @BackupPath NVARCHAR(500)
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @TestDatabaseName NVARCHAR(128) = 'TestRecovery_' + FORMAT(GETDATE(), 'yyyyMMdd_HHmmss');
    DECLARE @SQL NVARCHAR(MAX);
    
    BEGIN TRY
        -- Restore database to test recovery
        SET @SQL = 'RESTORE DATABASE [' + @TestDatabaseName + '] FROM DISK = ''' + @BackupPath + '''';
        EXEC sp_executesql @SQL;
        
        -- Test database connectivity
        SET @SQL = 'SELECT COUNT(*) AS TableCount FROM [' + @TestDatabaseName + '].sys.tables';
        EXEC sp_executesql @SQL;
        
        -- Test data integrity
        SET @SQL = 'SELECT COUNT(*) AS RecordCount FROM [' + @TestDatabaseName + '].dbo.Orders';
        EXEC sp_executesql @SQL;
        
        -- Clean up test database
        SET @SQL = 'DROP DATABASE [' + @TestDatabaseName + ']';
        EXEC sp_executesql @SQL;
        
        SELECT 'Recovery test completed successfully' AS Status;
        
    END TRY
    BEGIN CATCH
        -- Clean up on error
        IF EXISTS (SELECT 1 FROM sys.databases WHERE name = @TestDatabaseName)
        BEGIN
            SET @SQL = 'DROP DATABASE [' + @TestDatabaseName + ']';
            EXEC sp_executesql @SQL;
        END;
        
        SELECT 'Recovery test failed: ' + ERROR_MESSAGE() AS Status;
    END CATCH;
END;

-- Execute recovery test
EXEC sp_TestRecovery @BackupPath = 'C:\Backups\YourDatabase_Full.bak';
```

### Disaster Recovery Planning
```sql
-- Example 3: Disaster recovery planning
CREATE PROCEDURE sp_DisasterRecoveryPlan
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Check database status
    SELECT 
        name AS DatabaseName,
        state_desc AS DatabaseState,
        recovery_model_desc AS RecoveryModel,
        collation_name AS Collation
    FROM sys.databases
    WHERE name = DB_NAME();
    
    -- Check log file status
    SELECT 
        name AS LogFileName,
        size * 8 / 1024 AS SizeMB,
        max_size * 8 / 1024 AS MaxSizeMB,
        growth AS GrowthMB
    FROM sys.database_files
    WHERE type_desc = 'LOG';
    
    -- Check backup chain
    SELECT 
        'Full Backup' AS BackupType,
        MAX(backup_start_date) AS LastBackup,
        DATEDIFF(HOUR, MAX(backup_start_date), GETDATE()) AS HoursAgo
    FROM msdb.dbo.backupset
    WHERE database_name = DB_NAME() AND backup_type = 'D'
    
    UNION ALL
    
    SELECT 
        'Log Backup' AS BackupType,
        MAX(backup_start_date) AS LastBackup,
        DATEDIFF(MINUTE, MAX(backup_start_date), GETDATE()) AS MinutesAgo
    FROM msdb.dbo.backupset
    WHERE database_name = DB_NAME() AND backup_type = 'L';
    
    -- Recovery time estimation
    SELECT 
        'Estimated Recovery Time' AS Metric,
        '15-30 minutes' AS Value
    UNION ALL
    SELECT 
        'RTO (Recovery Time Objective)',
        '1 hour'
    UNION ALL
    SELECT 
        'RPO (Recovery Point Objective)',
        '15 minutes';
END;

-- Execute disaster recovery plan
EXEC sp_DisasterRecoveryPlan;
```

## 105. How do you implement database automation and scheduling in T-SQL?

### Automated Maintenance
```sql
-- Example 1: Automated database maintenance
CREATE PROCEDURE sp_AutomatedMaintenance
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @StartTime DATETIME2 = GETDATE();
    DECLARE @Results TABLE (
        TaskName NVARCHAR(100),
        Status NVARCHAR(20),
        Duration INT,
        Details NVARCHAR(MAX)
    );
    
    -- Task 1: Update statistics
    BEGIN TRY
        DECLARE @StatsStart DATETIME2 = GETDATE();
        EXEC sp_updatestats;
        INSERT INTO @Results VALUES ('Update Statistics', 'SUCCESS', DATEDIFF(SECOND, @StatsStart, GETDATE()), 'All statistics updated');
    END TRY
    BEGIN CATCH
        INSERT INTO @Results VALUES ('Update Statistics', 'FAILED', 0, ERROR_MESSAGE());
    END CATCH;
    
    -- Task 2: Rebuild fragmented indexes
    BEGIN TRY
        DECLARE @IndexStart DATETIME2 = GETDATE();
        DECLARE @SQL NVARCHAR(MAX);
        
        SET @SQL = 'ALTER INDEX ALL ON Orders REBUILD';
        EXEC sp_executesql @SQL;
        
        INSERT INTO @Results VALUES ('Rebuild Indexes', 'SUCCESS', DATEDIFF(SECOND, @IndexStart, GETDATE()), 'All indexes rebuilt');
    END TRY
    BEGIN CATCH
        INSERT INTO @Results VALUES ('Rebuild Indexes', 'FAILED', 0, ERROR_MESSAGE());
    END CATCH;
    
    -- Task 3: Clean up old data
    BEGIN TRY
        DECLARE @CleanupStart DATETIME2 = GETDATE();
        DECLARE @DeletedCount INT;
        
        DELETE FROM SystemLogs
        WHERE LogDate < DATEADD(MONTH, -3, GETDATE());
        
        SET @DeletedCount = @@ROWCOUNT;
        INSERT INTO @Results VALUES ('Data Cleanup', 'SUCCESS', DATEDIFF(SECOND, @CleanupStart, GETDATE()), 'Deleted ' + CAST(@DeletedCount AS VARCHAR(10)) + ' old records');
    END TRY
    BEGIN CATCH
        INSERT INTO @Results VALUES ('Data Cleanup', 'FAILED', 0, ERROR_MESSAGE());
    END CATCH;
    
    -- Return results
    SELECT 
        TaskName,
        Status,
        Duration,
        Details,
        DATEDIFF(SECOND, @StartTime, GETDATE()) AS TotalDuration
    FROM @Results;
END;

-- Execute automated maintenance
EXEC sp_AutomatedMaintenance;
```

### Scheduled Tasks
```sql
-- Example 2: Scheduled task management
CREATE TABLE ScheduledTasks (
    TaskID INT IDENTITY(1,1) PRIMARY KEY,
    TaskName NVARCHAR(100) NOT NULL,
    TaskType NVARCHAR(50) NOT NULL,
    SchedulePattern NVARCHAR(100) NOT NULL,
    IsActive BIT DEFAULT 1,
    LastExecuted DATETIME2 NULL,
    NextExecution DATETIME2 NULL,
    CreatedDate DATETIME2 DEFAULT GETDATE()
);

-- Insert scheduled tasks
INSERT INTO ScheduledTasks (TaskName, TaskType, SchedulePattern, NextExecution)
VALUES 
    ('Daily Statistics Update', 'MAINTENANCE', 'DAILY', DATEADD(DAY, 1, GETDATE())),
    ('Weekly Index Rebuild', 'MAINTENANCE', 'WEEKLY', DATEADD(WEEK, 1, GETDATE())),
    ('Monthly Data Archive', 'ARCHIVE', 'MONTHLY', DATEADD(MONTH, 1, GETDATE()));

-- Task execution scheduler
CREATE PROCEDURE sp_ExecuteScheduledTasks
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @TaskID INT;
    DECLARE @TaskName NVARCHAR(100);
    DECLARE @TaskType NVARCHAR(50);
    DECLARE @NextExecution DATETIME2;
    
    DECLARE task_cursor CURSOR FOR
    SELECT TaskID, TaskName, TaskType, NextExecution
    FROM ScheduledTasks
    WHERE IsActive = 1
        AND NextExecution <= GETDATE();
    
    OPEN task_cursor;
    FETCH NEXT FROM task_cursor INTO @TaskID, @TaskName, @TaskType, @NextExecution;
    
    WHILE @@FETCH_STATUS = 0
    BEGIN
        BEGIN TRY
            -- Execute task based on type
            IF @TaskType = 'MAINTENANCE'
            BEGIN
                EXEC sp_AutomatedMaintenance;
            END
            ELSE IF @TaskType = 'ARCHIVE'
            BEGIN
                EXEC sp_ArchiveOldData @RetentionDays = 365, @BatchSize = 1000;
            END;
            
            -- Update task execution time
            UPDATE ScheduledTasks
            SET LastExecuted = GETDATE(),
                NextExecution = CASE 
                    WHEN SchedulePattern = 'DAILY' THEN DATEADD(DAY, 1, GETDATE())
                    WHEN SchedulePattern = 'WEEKLY' THEN DATEADD(WEEK, 1, GETDATE())
                    WHEN SchedulePattern = 'MONTHLY' THEN DATEADD(MONTH, 1, GETDATE())
                END
            WHERE TaskID = @TaskID;
            
            PRINT 'Executed task: ' + @TaskName;
            
        END TRY
        BEGIN CATCH
            PRINT 'Task execution failed: ' + @TaskName + ' - ' + ERROR_MESSAGE();
        END CATCH;
        
        FETCH NEXT FROM task_cursor INTO @TaskID, @TaskName, @TaskType, @NextExecution;
    END;
    
    CLOSE task_cursor;
    DEALLOCATE task_cursor;
END;

-- Execute scheduled tasks
EXEC sp_ExecuteScheduledTasks;
```

### Automation Monitoring
```sql
-- Example 3: Automation monitoring and reporting
CREATE PROCEDURE sp_MonitorAutomation
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Check task execution history
    SELECT 
        TaskName,
        LastExecuted,
        DATEDIFF(HOUR, LastExecuted, GETDATE()) AS HoursSinceLastExecution,
        NextExecution,
        DATEDIFF(HOUR, GETDATE(), NextExecution) AS HoursUntilNextExecution,
        CASE 
            WHEN LastExecuted IS NULL THEN 'NEVER_EXECUTED'
            WHEN DATEDIFF(HOUR, LastExecuted, GETDATE()) > 25 THEN 'OVERDUE'
            WHEN DATEDIFF(HOUR, LastExecuted, GETDATE()) > 1 THEN 'LATE'
            ELSE 'ON_TIME'
        END AS ExecutionStatus
    FROM ScheduledTasks
    WHERE IsActive = 1
    ORDER BY LastExecuted DESC;
    
    -- Check for failed tasks
    SELECT 
        'Failed Tasks' AS AlertType,
        COUNT(*) AS Count
    FROM ScheduledTasks
    WHERE IsActive = 1
        AND LastExecuted IS NULL
        AND NextExecution < GETDATE()
    
    UNION ALL
    
    SELECT 
        'Overdue Tasks' AS AlertType,
        COUNT(*) AS Count
    FROM ScheduledTasks
    WHERE IsActive = 1
        AND LastExecuted IS NOT NULL
        AND DATEDIFF(HOUR, LastExecuted, GETDATE()) > 25;
END;

-- Execute automation monitoring
EXEC sp_MonitorAutomation;
```

## 106. How do you implement database security and compliance in T-SQL?

### Security Audit Implementation
```sql
-- Example 1: Comprehensive security audit
CREATE PROCEDURE sp_SecurityAudit
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Check user permissions
    SELECT 
        'User Permissions' AS AuditCategory,
        p.name AS PrincipalName,
        p.type_desc AS PrincipalType,
        r.name AS RoleName,
        r.type_desc AS RoleType
    FROM sys.database_principals p
    LEFT JOIN sys.database_role_members rm ON p.principal_id = rm.member_principal_id
    LEFT JOIN sys.database_principals r ON rm.role_principal_id = r.principal_id
    WHERE p.type IN ('S', 'U', 'G')
    
    UNION ALL
    
    -- Check object permissions
    SELECT 
        'Object Permissions' AS AuditCategory,
        p.name AS PrincipalName,
        o.name AS ObjectName,
        pr.permission_name AS Permission,
        pr.state_desc AS PermissionState
    FROM sys.database_principals p
    CROSS JOIN sys.objects o
    INNER JOIN sys.database_permissions pr ON p.principal_id = pr.grantee_principal_id
    WHERE o.type IN ('U', 'V', 'P', 'FN', 'IF', 'TF');
    
    -- Check for excessive permissions
    SELECT 
        'Excessive Permissions' AS AlertType,
        COUNT(*) AS Count
    FROM sys.database_permissions
    WHERE permission_name IN ('CONTROL', 'ALTER', 'TAKE OWNERSHIP')
    
    UNION ALL
    
    SELECT 
        'Public Role Members' AS AlertType,
        COUNT(*) AS Count
    FROM sys.database_role_members rm
    INNER JOIN sys.database_principals r ON rm.role_principal_id = r.principal_id
    WHERE r.name = 'public';
END;

-- Execute security audit
EXEC sp_SecurityAudit;
```

**Result:**
```
AuditCategory      | PrincipalName | PrincipalType | RoleName | RoleType
-------------------|---------------|---------------|----------|----------
User Permissions   | dbo           | SQL_USER      | db_owner | DATABASE_ROLE
User Permissions   | guest         | SQL_USER      | NULL     | NULL
Object Permissions | dbo           | Orders        | SELECT   | GRANT
```

### Data Encryption Implementation
```sql
-- Example 2: Data encryption and decryption
-- Create encryption key
CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'YourStrongPassword123!';

-- Create certificate
CREATE CERTIFICATE CustomerDataCert
WITH SUBJECT = 'Customer Data Encryption Certificate';

-- Create symmetric key
CREATE SYMMETRIC KEY CustomerDataKey
WITH ALGORITHM = AES_256
ENCRYPTION BY CERTIFICATE CustomerDataCert;

-- Create table with encrypted columns
CREATE TABLE EncryptedCustomers (
    CustomerID INT IDENTITY(1,1) PRIMARY KEY,
    CustomerName NVARCHAR(100),
    EncryptedSSN VARBINARY(MAX),
    EncryptedEmail VARBINARY(MAX),
    CreatedDate DATETIME2 DEFAULT GETDATE()
);

-- Insert encrypted data
OPEN SYMMETRIC KEY CustomerDataKey DECRYPTION BY CERTIFICATE CustomerDataCert;

INSERT INTO EncryptedCustomers (CustomerName, EncryptedSSN, EncryptedEmail)
VALUES 
    ('John Smith', 
     EncryptByKey(Key_GUID('CustomerDataKey'), '123-45-6789'),
     EncryptByKey(Key_GUID('CustomerDataKey'), 'john@example.com')),
    ('Jane Doe', 
     EncryptByKey(Key_GUID('CustomerDataKey'), '987-65-4321'),
     EncryptByKey(Key_GUID('CustomerDataKey'), 'jane@example.com'));

CLOSE SYMMETRIC KEY CustomerDataKey;

-- Query decrypted data
OPEN SYMMETRIC KEY CustomerDataKey DECRYPTION BY CERTIFICATE CustomerDataCert;

SELECT 
    CustomerID,
    CustomerName,
    CONVERT(NVARCHAR(20), DecryptByKey(EncryptedSSN)) AS DecryptedSSN,
    CONVERT(NVARCHAR(100), DecryptByKey(EncryptedEmail)) AS DecryptedEmail
FROM EncryptedCustomers;

CLOSE SYMMETRIC KEY CustomerDataKey;
```

**Result:**
```
CustomerID | CustomerName | DecryptedSSN | DecryptedEmail
-----------|--------------|--------------|----------------
1          | John Smith   | 123-45-6789  | john@example.com
2          | Jane Doe     | 987-65-4321  | jane@example.com
```

### Compliance Reporting
```sql
-- Example 3: Compliance reporting and monitoring
CREATE PROCEDURE sp_ComplianceReport
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Data access audit
    SELECT 
        'Data Access Audit' AS ReportSection,
        session_id,
        login_name,
        host_name,
        program_name,
        login_time,
        last_request_start_time
    FROM sys.dm_exec_sessions
    WHERE is_user_process = 1
        AND login_time >= DATEADD(DAY, -7, GETDATE())
    ORDER BY login_time DESC;
    
    -- Permission changes audit
    SELECT 
        'Permission Changes' AS ReportSection,
        event_time,
        server_principal_name,
        target_server_principal_name,
        permission_name,
        action_id
    FROM sys.fn_get_audit_file('C:\Audit\*.sqlaudit', DEFAULT, DEFAULT)
    WHERE action_id IN ('AUP', 'AUD', 'AUR')
        AND event_time >= DATEADD(DAY, -7, GETDATE())
    ORDER BY event_time DESC;
    
    -- Data modification audit
    SELECT 
        'Data Modifications' AS ReportSection,
        COUNT(*) AS ModificationCount,
        MAX(event_time) AS LastModification
    FROM sys.fn_get_audit_file('C:\Audit\*.sqlaudit', DEFAULT, DEFAULT)
    WHERE action_id IN ('IN', 'UP', 'DL')
        AND event_time >= DATEADD(DAY, -7, GETDATE());
    
    -- Compliance status
    SELECT 
        'Compliance Status' AS ReportSection,
        'Data Encryption' AS ComplianceItem,
        CASE 
            WHEN EXISTS (SELECT 1 FROM sys.symmetric_keys WHERE name = 'CustomerDataKey') 
            THEN 'COMPLIANT' 
            ELSE 'NON_COMPLIANT' 
        END AS Status
    UNION ALL
    SELECT 
        'Compliance Status',
        'Audit Logging',
        CASE 
            WHEN EXISTS (SELECT 1 FROM sys.server_audits WHERE is_state_enabled = 1) 
            THEN 'COMPLIANT' 
            ELSE 'NON_COMPLIANT' 
        END
    UNION ALL
    SELECT 
        'Compliance Status',
        'Row Level Security',
        CASE 
            WHEN EXISTS (SELECT 1 FROM sys.security_policies) 
            THEN 'COMPLIANT' 
            ELSE 'NON_COMPLIANT' 
        END;
END;

-- Execute compliance report
EXEC sp_ComplianceReport;
```

## 107. How do you implement database performance tuning and optimization in T-SQL?

### Query Performance Analysis
```sql
-- Example 1: Query performance analysis
CREATE PROCEDURE sp_AnalyzeQueryPerformance
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Top 10 slowest queries
    SELECT TOP 10
        'Slowest Queries' AS AnalysisType,
        query_hash,
        query_plan_hash,
        total_elapsed_time,
        execution_count,
        total_elapsed_time / execution_count AS avg_elapsed_time,
        total_logical_reads,
        total_logical_writes,
        last_execution_time
    FROM sys.dm_exec_query_stats
    ORDER BY total_elapsed_time DESC;
    
    -- Most expensive queries by CPU
    SELECT TOP 10
        'CPU Intensive Queries' AS AnalysisType,
        query_hash,
        total_worker_time,
        execution_count,
        total_worker_time / execution_count AS avg_worker_time,
        total_logical_reads,
        last_execution_time
    FROM sys.dm_exec_query_stats
    ORDER BY total_worker_time DESC;
    
    -- Missing index recommendations
    SELECT 
        'Missing Indexes' AS AnalysisType,
        migs.avg_total_user_cost * (migs.avg_user_impact / 100.0) * (migs.user_seeks + migs.user_scans) AS improvement_measure,
        'CREATE INDEX IX_' + OBJECT_NAME(mid.object_id) + '_' + 
        REPLACE(REPLACE(REPLACE(ISNULL(mid.equality_columns, ''), ', ', '_'), '[', ''), ']', '') + 
        ' ON ' + mid.statement + ' (' + ISNULL(mid.equality_columns, '') + 
        CASE WHEN mid.equality_columns IS NOT NULL AND mid.inequality_columns IS NOT NULL 
             THEN ',' ELSE '' END + ISNULL(mid.inequality_columns, '') + ')' +
        CASE WHEN mid.included_columns IS NOT NULL 
             THEN ' INCLUDE (' + mid.included_columns + ')' ELSE '' END AS create_index_statement
    FROM sys.dm_db_missing_index_groups mig
    INNER JOIN sys.dm_db_missing_index_group_stats migs ON migs.group_handle = mig.index_group_handle
    INNER JOIN sys.dm_db_missing_index_details mid ON mig.index_handle = mid.index_handle
    WHERE migs.avg_total_user_cost * (migs.avg_user_impact / 100.0) * (migs.user_seeks + migs.user_scans) > 10
    ORDER BY improvement_measure DESC;
END;

-- Execute performance analysis
EXEC sp_AnalyzeQueryPerformance;
```

### Index Optimization
```sql
-- Example 2: Index optimization and maintenance
CREATE PROCEDURE sp_OptimizeIndexes
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @Results TABLE (
        TableName NVARCHAR(128),
        IndexName NVARCHAR(128),
        FragmentationPercent DECIMAL(5,2),
        RecommendedAction NVARCHAR(20),
        EstimatedTime INT
    );
    
    -- Analyze index fragmentation
    INSERT INTO @Results (TableName, IndexName, FragmentationPercent, RecommendedAction, EstimatedTime)
    SELECT 
        OBJECT_NAME(ips.object_id) AS TableName,
        i.name AS IndexName,
        ips.avg_fragmentation_in_percent AS FragmentationPercent,
        CASE 
            WHEN ips.avg_fragmentation_in_percent > 30 THEN 'REBUILD'
            WHEN ips.avg_fragmentation_in_percent > 10 THEN 'REORGANIZE'
            ELSE 'NO_ACTION'
        END AS RecommendedAction,
        CASE 
            WHEN ips.avg_fragmentation_in_percent > 30 THEN ips.page_count / 1000
            WHEN ips.avg_fragmentation_in_percent > 10 THEN ips.page_count / 2000
            ELSE 0
        END AS EstimatedTime
    FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'LIMITED') ips
    INNER JOIN sys.indexes i ON ips.object_id = i.object_id AND ips.index_id = i.index_id
    WHERE ips.avg_fragmentation_in_percent > 10
        AND i.name IS NOT NULL;
    
    -- Execute recommended actions
    DECLARE @TableName NVARCHAR(128);
    DECLARE @IndexName NVARCHAR(128);
    DECLARE @Action NVARCHAR(20);
    DECLARE @SQL NVARCHAR(MAX);
    
    DECLARE action_cursor CURSOR FOR
    SELECT TableName, IndexName, RecommendedAction
    FROM @Results
    WHERE RecommendedAction != 'NO_ACTION';
    
    OPEN action_cursor;
    FETCH NEXT FROM action_cursor INTO @TableName, @IndexName, @Action;
    
    WHILE @@FETCH_STATUS = 0
    BEGIN
        IF @Action = 'REBUILD'
        BEGIN
            SET @SQL = 'ALTER INDEX [' + @IndexName + '] ON [' + @TableName + '] REBUILD';
        END
        ELSE IF @Action = 'REORGANIZE'
        BEGIN
            SET @SQL = 'ALTER INDEX [' + @IndexName + '] ON [' + @TableName + '] REORGANIZE';
        END;
        
        BEGIN TRY
            EXEC sp_executesql @SQL;
            PRINT 'Executed: ' + @SQL;
        END TRY
        BEGIN CATCH
            PRINT 'Failed: ' + @SQL + ' - ' + ERROR_MESSAGE();
        END CATCH;
        
        FETCH NEXT FROM action_cursor INTO @TableName, @IndexName, @Action;
    END;
    
    CLOSE action_cursor;
    DEALLOCATE action_cursor;
    
    -- Return results
    SELECT * FROM @Results ORDER BY FragmentationPercent DESC;
END;

-- Execute index optimization
EXEC sp_OptimizeIndexes;
```

### Memory and Resource Optimization
```sql
-- Example 3: Memory and resource optimization
CREATE PROCEDURE sp_OptimizeResources
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Memory usage analysis
    SELECT 
        'Memory Usage' AS ResourceType,
        (total_physical_memory_kb / 1024.0) AS TotalMemoryGB,
        (available_physical_memory_kb / 1024.0) AS AvailableMemoryGB,
        ((total_physical_memory_kb - available_physical_memory_kb) / 1024.0) AS UsedMemoryGB,
        CAST((total_physical_memory_kb - available_physical_memory_kb) * 100.0 / total_physical_memory_kb AS DECIMAL(5,2)) AS MemoryUsagePercent
    FROM sys.dm_os_sys_info;
    
    -- Buffer pool analysis
    SELECT 
        'Buffer Pool' AS ResourceType,
        COUNT(*) AS PageCount,
        COUNT(*) * 8 / 1024 AS SizeMB,
        SUM(CASE WHEN is_modified = 1 THEN 1 ELSE 0 END) AS DirtyPages,
        SUM(CASE WHEN is_modified = 0 THEN 1 ELSE 0 END) AS CleanPages
    FROM sys.dm_os_buffer_descriptors;
    
    -- Top memory consuming queries
    SELECT TOP 10
        'Memory Intensive Queries' AS ResourceType,
        query_hash,
        total_logical_reads,
        total_logical_writes,
        total_physical_reads,
        execution_count,
        total_logical_reads / execution_count AS avg_logical_reads
    FROM sys.dm_exec_query_stats
    ORDER BY total_logical_reads DESC;
    
    -- Resource recommendations
    SELECT 
        'Resource Recommendations' AS ResourceType,
        CASE 
            WHEN (SELECT (total_physical_memory_kb - available_physical_memory_kb) * 100.0 / total_physical_memory_kb FROM sys.dm_os_sys_info) > 90 
            THEN 'Consider increasing memory or optimizing queries'
            WHEN (SELECT (total_physical_memory_kb - available_physical_memory_kb) * 100.0 / total_physical_memory_kb FROM sys.dm_os_sys_info) > 80 
            THEN 'Monitor memory usage closely'
            ELSE 'Memory usage is within acceptable range'
        END AS Recommendation;
END;

-- Execute resource optimization
EXEC sp_OptimizeResources;
```

## 108. How do you implement database backup and recovery strategies in T-SQL?

### Automated Backup Strategy
```sql
-- Example 1: Automated backup strategy
CREATE PROCEDURE sp_AutomatedBackup
    @BackupType NVARCHAR(20) = 'FULL', -- 'FULL', 'DIFF', 'LOG'
    @BackupPath NVARCHAR(500) = 'C:\Backups\'
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @DatabaseName NVARCHAR(128) = DB_NAME();
    DECLARE @BackupFileName NVARCHAR(500);
    DECLARE @SQL NVARCHAR(MAX);
    DECLARE @StartTime DATETIME2 = GETDATE();
    
    -- Generate backup filename
    SET @BackupFileName = @BackupPath + @DatabaseName + '_' + @BackupType + '_' + 
                         FORMAT(GETDATE(), 'yyyyMMdd_HHmmss') + '.bak';
    
    IF @BackupType = 'FULL'
    BEGIN
        SET @SQL = 'BACKUP DATABASE [' + @DatabaseName + '] TO DISK = ''' + @BackupFileName + ''' WITH FORMAT, INIT, COMPRESSION, CHECKSUM';
    END
    ELSE IF @BackupType = 'DIFF'
    BEGIN
        SET @SQL = 'BACKUP DATABASE [' + @DatabaseName + '] TO DISK = ''' + @BackupFileName + ''' WITH DIFFERENTIAL, FORMAT, INIT, COMPRESSION, CHECKSUM';
    END
    ELSE IF @BackupType = 'LOG'
    BEGIN
        SET @SQL = 'BACKUP LOG [' + @DatabaseName + '] TO DISK = ''' + @BackupFileName + ''' WITH FORMAT, INIT, COMPRESSION, CHECKSUM';
    END;
    
    BEGIN TRY
        EXEC sp_executesql @SQL;
        
        DECLARE @EndTime DATETIME2 = GETDATE();
        SELECT 
            'Backup completed successfully' AS Status,
            @BackupFileName AS BackupFile,
            DATEDIFF(SECOND, @StartTime, @EndTime) AS DurationSeconds,
            @BackupType AS BackupType;
            
    END TRY
    BEGIN CATCH
        SELECT 
            'Backup failed: ' + ERROR_MESSAGE() AS Status,
            @BackupFileName AS BackupFile,
            @BackupType AS BackupType;
    END CATCH;
END;

-- Execute automated backup
EXEC sp_AutomatedBackup @BackupType = 'FULL', @BackupPath = 'C:\Backups\';
```

### Backup Verification
```sql
-- Example 2: Backup verification and integrity checks
CREATE PROCEDURE sp_VerifyBackups
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Check backup history
    SELECT 
        'Backup History' AS CheckType,
        database_name,
        backup_start_date,
        backup_finish_date,
        backup_size,
        backup_type,
        CASE 
            WHEN backup_type = 'D' THEN 'Full'
            WHEN backup_type = 'I' THEN 'Differential'
            WHEN backup_type = 'L' THEN 'Log'
        END AS BackupType,
        DATEDIFF(MINUTE, backup_start_date, backup_finish_date) AS DurationMinutes,
        CASE 
            WHEN has_backup_checksums = 1 THEN 'Yes'
            ELSE 'No'
        END AS HasChecksums
    FROM msdb.dbo.backupset
    WHERE database_name = DB_NAME()
    ORDER BY backup_start_date DESC;
    
    -- Check for recent backups
    DECLARE @LastFullBackup DATETIME2;
    DECLARE @LastDiffBackup DATETIME2;
    DECLARE @LastLogBackup DATETIME2;
    
    SELECT @LastFullBackup = MAX(backup_start_date)
    FROM msdb.dbo.backupset
    WHERE database_name = DB_NAME() AND backup_type = 'D';
    
    SELECT @LastDiffBackup = MAX(backup_start_date)
    FROM msdb.dbo.backupset
    WHERE database_name = DB_NAME() AND backup_type = 'I';
    
    SELECT @LastLogBackup = MAX(backup_start_date)
    FROM msdb.dbo.backupset
    WHERE database_name = DB_NAME() AND backup_type = 'L';
    
    -- Backup age analysis
    SELECT 
        'Backup Age Analysis' AS CheckType,
        'Full Backup' AS BackupType,
        @LastFullBackup AS LastBackup,
        DATEDIFF(HOUR, @LastFullBackup, GETDATE()) AS HoursAgo,
        CASE 
            WHEN DATEDIFF(HOUR, @LastFullBackup, GETDATE()) > 24 THEN 'OVERDUE'
            WHEN DATEDIFF(HOUR, @LastFullBackup, GETDATE()) > 12 THEN 'WARNING'
            ELSE 'OK'
        END AS Status
    UNION ALL
    SELECT 
        'Backup Age Analysis',
        'Differential Backup',
        @LastDiffBackup,
        DATEDIFF(HOUR, @LastDiffBackup, GETDATE()),
        CASE 
            WHEN DATEDIFF(HOUR, @LastDiffBackup, GETDATE()) > 6 THEN 'OVERDUE'
            WHEN DATEDIFF(HOUR, @LastDiffBackup, GETDATE()) > 3 THEN 'WARNING'
            ELSE 'OK'
        END
    UNION ALL
    SELECT 
        'Backup Age Analysis',
        'Log Backup',
        @LastLogBackup,
        DATEDIFF(MINUTE, @LastLogBackup, GETDATE()),
        CASE 
            WHEN DATEDIFF(MINUTE, @LastLogBackup, GETDATE()) > 60 THEN 'OVERDUE'
            WHEN DATEDIFF(MINUTE, @LastLogBackup, GETDATE()) > 30 THEN 'WARNING'
            ELSE 'OK'
        END;
END;

-- Execute backup verification
EXEC sp_VerifyBackups;
```

### Recovery Testing
```sql
-- Example 3: Recovery testing and validation
CREATE PROCEDURE sp_TestRecovery
    @BackupPath NVARCHAR(500),
    @TestDatabaseName NVARCHAR(128) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    IF @TestDatabaseName IS NULL
        SET @TestDatabaseName = 'TestRecovery_' + FORMAT(GETDATE(), 'yyyyMMdd_HHmmss');
    
    DECLARE @SQL NVARCHAR(MAX);
    DECLARE @StartTime DATETIME2 = GETDATE();
    
    BEGIN TRY
        -- Restore database for testing
        SET @SQL = 'RESTORE DATABASE [' + @TestDatabaseName + '] FROM DISK = ''' + @BackupPath + ''' WITH REPLACE, MOVE ''YourDatabase'' TO ''C:\Data\' + @TestDatabaseName + '.mdf'', MOVE ''YourDatabase_Log'' TO ''C:\Data\' + @TestDatabaseName + '_Log.ldf''';
        EXEC sp_executesql @SQL;
        
        -- Test database connectivity
        SET @SQL = 'SELECT COUNT(*) AS TableCount FROM [' + @TestDatabaseName + '].sys.tables';
        EXEC sp_executesql @SQL;
        
        -- Test data integrity
        SET @SQL = 'SELECT COUNT(*) AS RecordCount FROM [' + @TestDatabaseName + '].dbo.Orders';
        EXEC sp_executesql @SQL;
        
        -- Test stored procedures
        SET @SQL = 'SELECT COUNT(*) AS ProcedureCount FROM [' + @TestDatabaseName + '].sys.procedures';
        EXEC sp_executesql @SQL;
        
        -- Clean up test database
        SET @SQL = 'DROP DATABASE [' + @TestDatabaseName + ']';
        EXEC sp_executesql @SQL;
        
        DECLARE @EndTime DATETIME2 = GETDATE();
        SELECT 
            'Recovery test completed successfully' AS Status,
            DATEDIFF(SECOND, @StartTime, @EndTime) AS DurationSeconds,
            @TestDatabaseName AS TestDatabase;
        
    END TRY
    BEGIN CATCH
        -- Clean up on error
        IF EXISTS (SELECT 1 FROM sys.databases WHERE name = @TestDatabaseName)
        BEGIN
            SET @SQL = 'DROP DATABASE [' + @TestDatabaseName + ']';
            EXEC sp_executesql @SQL;
        END;
        
        SELECT 
            'Recovery test failed: ' + ERROR_MESSAGE() AS Status,
            @TestDatabaseName AS TestDatabase;
    END CATCH;
END;

-- Execute recovery test
EXEC sp_TestRecovery @BackupPath = 'C:\Backups\YourDatabase_Full.bak';
```

## 109. How do you implement database monitoring and alerting in T-SQL?

### Performance Monitoring
```sql
-- Example 1: Comprehensive performance monitoring
CREATE PROCEDURE sp_MonitorDatabasePerformance
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Monitor active sessions
    SELECT 
        'Active Sessions' AS Metric,
        COUNT(*) AS Value
    FROM sys.dm_exec_sessions
    WHERE is_user_process = 1
    
    UNION ALL
    
    -- Monitor blocking
    SELECT 
        'Blocking Sessions' AS Metric,
        COUNT(*) AS Value
    FROM sys.dm_exec_requests
    WHERE blocking_session_id > 0
    
    UNION ALL
    
    -- Monitor memory usage
    SELECT 
        'Memory Usage %' AS Metric,
        CAST((total_physical_memory_kb - available_physical_memory_kb) * 100.0 / total_physical_memory_kb AS DECIMAL(5,2)) AS Value
    FROM sys.dm_os_sys_info
    
    UNION ALL
    
    -- Monitor CPU usage
    SELECT 
        'CPU Usage %' AS Metric,
        CAST(cpu_percent AS DECIMAL(5,2)) AS Value
    FROM sys.dm_os_ring_buffers
    WHERE ring_buffer_type = 'RING_BUFFER_SCHEDULER_MONITOR';
END;

-- Execute monitoring
EXEC sp_MonitorDatabasePerformance;
```

**Result:**
```
Metric          | Value
----------------|-------
Active Sessions | 15
Blocking Sessions | 2
Memory Usage %  | 75.50
CPU Usage %     | 45.20
```

### Alert System
```sql
-- Example 2: Database alerting system
CREATE TABLE DatabaseAlerts (
    AlertID INT IDENTITY(1,1) PRIMARY KEY,
    AlertType NVARCHAR(50) NOT NULL,
    AlertMessage NVARCHAR(MAX) NOT NULL,
    AlertLevel NVARCHAR(20) NOT NULL,
    CreatedDate DATETIME2 DEFAULT GETDATE(),
    IsResolved BIT DEFAULT 0
);

CREATE PROCEDURE sp_CheckDatabaseAlerts
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @MemoryUsage DECIMAL(5,2);
    DECLARE @CPUUsage DECIMAL(5,2);
    DECLARE @BlockingCount INT;
    DECLARE @ActiveSessions INT;
    
    -- Get current metrics
    SELECT @MemoryUsage = (total_physical_memory_kb - available_physical_memory_kb) * 100.0 / total_physical_memory_kb
    FROM sys.dm_os_sys_info;
    
    SELECT @CPUUsage = cpu_percent
    FROM sys.dm_os_ring_buffers
    WHERE ring_buffer_type = 'RING_BUFFER_SCHEDULER_MONITOR';
    
    SELECT @BlockingCount = COUNT(*)
    FROM sys.dm_exec_requests
    WHERE blocking_session_id > 0;
    
    SELECT @ActiveSessions = COUNT(*)
    FROM sys.dm_exec_sessions
    WHERE is_user_process = 1;
    
    -- Check for alerts
    IF @MemoryUsage > 90
    BEGIN
        INSERT INTO DatabaseAlerts (AlertType, AlertMessage, AlertLevel)
        VALUES ('Memory', 'Memory usage is above 90%: ' + CAST(@MemoryUsage AS VARCHAR(10)) + '%', 'CRITICAL');
    END;
    
    IF @CPUUsage > 80
    BEGIN
        INSERT INTO DatabaseAlerts (AlertType, AlertMessage, AlertLevel)
        VALUES ('CPU', 'CPU usage is above 80%: ' + CAST(@CPUUsage AS VARCHAR(10)) + '%', 'WARNING');
    END;
    
    IF @BlockingCount > 5
    BEGIN
        INSERT INTO DatabaseAlerts (AlertType, AlertMessage, AlertLevel)
        VALUES ('Blocking', 'High number of blocking sessions: ' + CAST(@BlockingCount AS VARCHAR(10)), 'WARNING');
    END;
    
    IF @ActiveSessions > 100
    BEGIN
        INSERT INTO DatabaseAlerts (AlertType, AlertMessage, AlertLevel)
        VALUES ('Sessions', 'High number of active sessions: ' + CAST(@ActiveSessions AS VARCHAR(10)), 'INFO');
    END;
    
    -- Return current alerts
    SELECT * FROM DatabaseAlerts
    WHERE IsResolved = 0
    ORDER BY CreatedDate DESC;
END;

-- Execute alert check
EXEC sp_CheckDatabaseAlerts;
```

### Automated Monitoring
```sql
-- Example 3: Automated monitoring with thresholds
CREATE PROCEDURE sp_AutomatedMonitoring
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @Thresholds TABLE (
        MetricName NVARCHAR(50),
        WarningThreshold DECIMAL(10,2),
        CriticalThreshold DECIMAL(10,2),
        CurrentValue DECIMAL(10,2)
    );
    
    -- Define thresholds and get current values
    INSERT INTO @Thresholds (MetricName, WarningThreshold, CriticalThreshold, CurrentValue)
    VALUES 
        ('MemoryUsage', 80.0, 90.0, (SELECT (total_physical_memory_kb - available_physical_memory_kb) * 100.0 / total_physical_memory_kb FROM sys.dm_os_sys_info)),
        ('CPUUsage', 70.0, 85.0, (SELECT cpu_percent FROM sys.dm_os_ring_buffers WHERE ring_buffer_type = 'RING_BUFFER_SCHEDULER_MONITOR')),
        ('BlockingSessions', 3.0, 10.0, (SELECT COUNT(*) FROM sys.dm_exec_requests WHERE blocking_session_id > 0));
    
    -- Check thresholds and create alerts
    SELECT 
        MetricName,
        CurrentValue,
        CASE 
            WHEN CurrentValue >= CriticalThreshold THEN 'CRITICAL'
            WHEN CurrentValue >= WarningThreshold THEN 'WARNING'
            ELSE 'OK'
        END AS AlertLevel,
        CASE 
            WHEN CurrentValue >= CriticalThreshold THEN 'Critical threshold exceeded'
            WHEN CurrentValue >= WarningThreshold THEN 'Warning threshold exceeded'
            ELSE 'All metrics within normal range'
        END AS AlertMessage
    FROM @Thresholds;
END;

-- Execute automated monitoring
EXEC sp_AutomatedMonitoring;
```

## 110. How do you implement database maintenance and optimization in T-SQL?

### Automated Maintenance
```sql
-- Example 1: Automated database maintenance
CREATE PROCEDURE sp_AutomatedMaintenance
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @StartTime DATETIME2 = GETDATE();
    DECLARE @Results TABLE (
        TaskName NVARCHAR(100),
        Status NVARCHAR(20),
        Duration INT,
        Details NVARCHAR(MAX)
    );
    
    -- Task 1: Update statistics
    BEGIN TRY
        DECLARE @StatsStart DATETIME2 = GETDATE();
        EXEC sp_updatestats;
        INSERT INTO @Results VALUES ('Update Statistics', 'SUCCESS', DATEDIFF(SECOND, @StatsStart, GETDATE()), 'All statistics updated');
    END TRY
    BEGIN CATCH
        INSERT INTO @Results VALUES ('Update Statistics', 'FAILED', 0, ERROR_MESSAGE());
    END CATCH;
    
    -- Task 2: Rebuild fragmented indexes
    BEGIN TRY
        DECLARE @IndexStart DATETIME2 = GETDATE();
        DECLARE @SQL NVARCHAR(MAX);
        
        SET @SQL = 'ALTER INDEX ALL ON Orders REBUILD';
        EXEC sp_executesql @SQL;
        
        INSERT INTO @Results VALUES ('Rebuild Indexes', 'SUCCESS', DATEDIFF(SECOND, @IndexStart, GETDATE()), 'All indexes rebuilt');
    END TRY
    BEGIN CATCH
        INSERT INTO @Results VALUES ('Rebuild Indexes', 'FAILED', 0, ERROR_MESSAGE());
    END CATCH;
    
    -- Task 3: Clean up old data
    BEGIN TRY
        DECLARE @CleanupStart DATETIME2 = GETDATE();
        DECLARE @DeletedCount INT;
        
        DELETE FROM SystemLogs
        WHERE LogDate < DATEADD(MONTH, -3, GETDATE());
        
        SET @DeletedCount = @@ROWCOUNT;
        INSERT INTO @Results VALUES ('Data Cleanup', 'SUCCESS', DATEDIFF(SECOND, @CleanupStart, GETDATE()), 'Deleted ' + CAST(@DeletedCount AS VARCHAR(10)) + ' old records');
    END TRY
    BEGIN CATCH
        INSERT INTO @Results VALUES ('Data Cleanup', 'FAILED', 0, ERROR_MESSAGE());
    END CATCH;
    
    -- Return results
    SELECT 
        TaskName,
        Status,
        Duration,
        Details,
        DATEDIFF(SECOND, @StartTime, GETDATE()) AS TotalDuration
    FROM @Results;
END;

-- Execute automated maintenance
EXEC sp_AutomatedMaintenance;
```

### Index Maintenance
```sql
-- Example 2: Index maintenance and optimization
CREATE PROCEDURE sp_MaintainIndexes
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @Results TABLE (
        TableName NVARCHAR(128),
        IndexName NVARCHAR(128),
        FragmentationPercent DECIMAL(5,2),
        ActionTaken NVARCHAR(50),
        Duration INT
    );
    
    -- Analyze and maintain indexes
    DECLARE @TableName NVARCHAR(128);
    DECLARE @IndexName NVARCHAR(128);
    DECLARE @FragmentationPercent DECIMAL(5,2);
    DECLARE @Action NVARCHAR(50);
    DECLARE @SQL NVARCHAR(MAX);
    DECLARE @StartTime DATETIME2;
    
    DECLARE index_cursor CURSOR FOR
    SELECT 
        OBJECT_NAME(ips.object_id) AS TableName,
        i.name AS IndexName,
        ips.avg_fragmentation_in_percent AS FragmentationPercent
    FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'LIMITED') ips
    INNER JOIN sys.indexes i ON ips.object_id = i.object_id AND ips.index_id = i.index_id
    WHERE ips.avg_fragmentation_in_percent > 10
        AND i.name IS NOT NULL;
    
    OPEN index_cursor;
    FETCH NEXT FROM index_cursor INTO @TableName, @IndexName, @FragmentationPercent;
    
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @StartTime = GETDATE();
        
        IF @FragmentationPercent > 30
        BEGIN
            SET @Action = 'REBUILD';
            SET @SQL = 'ALTER INDEX [' + @IndexName + '] ON [' + @TableName + '] REBUILD';
        END
        ELSE IF @FragmentationPercent > 10
        BEGIN
            SET @Action = 'REORGANIZE';
            SET @SQL = 'ALTER INDEX [' + @IndexName + '] ON [' + @TableName + '] REORGANIZE';
        END;
        
        BEGIN TRY
            EXEC sp_executesql @SQL;
            INSERT INTO @Results VALUES (@TableName, @IndexName, @FragmentationPercent, @Action, DATEDIFF(SECOND, @StartTime, GETDATE()));
        END TRY
        BEGIN CATCH
            INSERT INTO @Results VALUES (@TableName, @IndexName, @FragmentationPercent, 'FAILED: ' + ERROR_MESSAGE(), 0);
        END CATCH;
        
        FETCH NEXT FROM index_cursor INTO @TableName, @IndexName, @FragmentationPercent;
    END;
    
    CLOSE index_cursor;
    DEALLOCATE index_cursor;
    
    -- Return results
    SELECT * FROM @Results ORDER BY FragmentationPercent DESC;
END;

-- Execute index maintenance
EXEC sp_MaintainIndexes;
```

### Database Optimization
```sql
-- Example 3: Database optimization and tuning
CREATE PROCEDURE sp_OptimizeDatabase
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Analyze database size and growth
    SELECT 
        'Database Size Analysis' AS AnalysisType,
        name AS DatabaseName,
        size * 8 / 1024 AS SizeMB,
        max_size * 8 / 1024 AS MaxSizeMB,
        growth AS GrowthMB,
        is_percent_growth AS IsPercentGrowth
    FROM sys.database_files
    ORDER BY size DESC;
    
    -- Analyze table sizes
    SELECT 
        'Table Size Analysis' AS AnalysisType,
        t.name AS TableName,
        p.rows AS RowCount,
        SUM(a.total_pages) * 8 / 1024 AS TotalSizeMB,
        SUM(a.used_pages) * 8 / 1024 AS UsedSizeMB,
        SUM(a.data_pages) * 8 / 1024 AS DataSizeMB
    FROM sys.tables t
    INNER JOIN sys.indexes i ON t.object_id = i.object_id
    INNER JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
    INNER JOIN sys.allocation_units a ON p.partition_id = a.container_id
    GROUP BY t.name, p.rows
    ORDER BY TotalSizeMB DESC;
    
    -- Analyze index usage
    SELECT 
        'Index Usage Analysis' AS AnalysisType,
        OBJECT_NAME(i.object_id) AS TableName,
        i.name AS IndexName,
        i.type_desc AS IndexType,
        s.user_seeks,
        s.user_scans,
        s.user_lookups,
        s.user_updates,
        s.last_user_seek,
        s.last_user_scan,
        s.last_user_lookup
    FROM sys.indexes i
    LEFT JOIN sys.dm_db_index_usage_stats s ON i.object_id = s.object_id AND i.index_id = s.index_id
    WHERE i.name IS NOT NULL
    ORDER BY s.user_seeks + s.user_scans + s.user_lookups DESC;
    
    -- Optimization recommendations
    SELECT 
        'Optimization Recommendations' AS AnalysisType,
        'Consider removing unused indexes' AS Recommendation,
        COUNT(*) AS UnusedIndexCount
    FROM sys.indexes i
    LEFT JOIN sys.dm_db_index_usage_stats s ON i.object_id = s.object_id AND i.index_id = s.index_id
    WHERE i.name IS NOT NULL
        AND (s.user_seeks + s.user_scans + s.user_lookups) = 0
        AND i.type_desc != 'HEAP'
    
    UNION ALL
    
    SELECT 
        'Optimization Recommendations',
        'Consider adding missing indexes',
        COUNT(*)
    FROM sys.dm_db_missing_index_groups mig
    INNER JOIN sys.dm_db_missing_index_group_stats migs ON migs.group_handle = mig.index_group_handle
    WHERE migs.avg_total_user_cost * (migs.avg_user_impact / 100.0) * (migs.user_seeks + migs.user_scans) > 10;
END;

-- Execute database optimization
EXEC sp_OptimizeDatabase;
```

## 111. How do you implement database scalability and high availability in T-SQL?

### High Availability Setup
```sql
-- Example 1: High availability configuration
CREATE PROCEDURE sp_ConfigureHighAvailability
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Check current availability group status
    SELECT 
        'Availability Group Status' AS ConfigurationType,
        ag.name AS AvailabilityGroupName,
        ag.primary_replica,
        ag.failover_mode_desc,
        ag.availability_mode_desc,
        ar.replica_server_name,
        ar.availability_mode_desc,
        ar.failover_mode_desc,
        ar.primary_role_allow_connections_desc
    FROM sys.availability_groups ag
    INNER JOIN sys.availability_replicas ar ON ag.group_id = ar.group_id;
    
    -- Check database replica status
    SELECT 
        'Database Replica Status' AS ConfigurationType,
        dr.database_name,
        dr.replica_id,
        dr.synchronization_state_desc,
        dr.synchronization_health_desc,
        dr.is_suspended,
        dr.suspend_reason_desc
    FROM sys.dm_hadr_database_replica_states dr
    INNER JOIN sys.availability_replicas ar ON dr.replica_id = ar.replica_id;
    
    -- Monitor replication lag
    SELECT 
        'Replication Lag' AS ConfigurationType,
        dr.database_name,
        dr.replica_id,
        dr.log_send_queue_size,
        dr.log_send_rate,
        dr.redo_queue_size,
        dr.redo_rate,
        DATEDIFF(SECOND, dr.last_commit_time, GETDATE()) AS LagSeconds
    FROM sys.dm_hadr_database_replica_states dr
    WHERE dr.is_primary_replica = 0;
END;

-- Execute high availability configuration
EXEC sp_ConfigureHighAvailability;
```

**Result:**
```
ConfigurationType      | AvailabilityGroupName | primary_replica | failover_mode_desc
-----------------------|----------------------|-----------------|-------------------
Availability Group Status | AG_Production | Server1 | AUTOMATIC
Database Replica Status | YourDatabase | 1 | SYNCHRONIZED
Replication Lag | YourDatabase | 2 | 0 | 0 | 0 | 0 | 5
```

### Load Balancing
```sql
-- Example 2: Load balancing and read-only routing
CREATE PROCEDURE sp_ConfigureLoadBalancing
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Configure read-only routing
    ALTER AVAILABILITY GROUP [AG_Production]
    MODIFY REPLICA ON 'Server1' WITH (PRIMARY_ROLE(READ_ONLY_ROUTING_LIST = ('Server2', 'Server3')));
    
    ALTER AVAILABILITY GROUP [AG_Production]
    MODIFY REPLICA ON 'Server2' WITH (PRIMARY_ROLE(READ_ONLY_ROUTING_LIST = ('Server1', 'Server3')));
    
    ALTER AVAILABILITY GROUP [AG_Production]
    MODIFY REPLICA ON 'Server3' WITH (PRIMARY_ROLE(READ_ONLY_ROUTING_LIST = ('Server1', 'Server2')));
    
    -- Configure read-only routing for secondary replicas
    ALTER AVAILABILITY GROUP [AG_Production]
    MODIFY REPLICA ON 'Server1' WITH (SECONDARY_ROLE(READ_ONLY_ROUTING_URL = 'TCP://Server1:1433'));
    
    ALTER AVAILABILITY GROUP [AG_Production]
    MODIFY REPLICA ON 'Server2' WITH (SECONDARY_ROLE(READ_ONLY_ROUTING_URL = 'TCP://Server2:1433'));
    
    ALTER AVAILABILITY GROUP [AG_Production]
    MODIFY REPLICA ON 'Server3' WITH (SECONDARY_ROLE(READ_ONLY_ROUTING_URL = 'TCP://Server3:1433'));
    
    -- Monitor load balancing
    SELECT 
        'Load Balancing Status' AS ConfigurationType,
        ar.replica_server_name,
        ar.availability_mode_desc,
        ar.failover_mode_desc,
        ar.primary_role_allow_connections_desc,
        ar.secondary_role_allow_connections_desc
    FROM sys.availability_groups ag
    INNER JOIN sys.availability_replicas ar ON ag.group_id = ar.group_id;
END;

-- Execute load balancing configuration
EXEC sp_ConfigureLoadBalancing;
```

### Scalability Monitoring
```sql
-- Example 3: Scalability monitoring and optimization
CREATE PROCEDURE sp_MonitorScalability
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Monitor connection distribution
    SELECT 
        'Connection Distribution' AS MetricType,
        @@SERVERNAME AS ServerName,
        COUNT(*) AS ActiveConnections,
        COUNT(CASE WHEN is_user_process = 1 THEN 1 END) AS UserConnections,
        COUNT(CASE WHEN is_user_process = 0 THEN 1 END) AS SystemConnections
    FROM sys.dm_exec_sessions
    WHERE status = 'running'
    
    UNION ALL
    
    -- Monitor resource usage
    SELECT 
        'Resource Usage' AS MetricType,
        @@SERVERNAME AS ServerName,
        (total_physical_memory_kb / 1024.0) AS TotalMemoryGB,
        (available_physical_memory_kb / 1024.0) AS AvailableMemoryGB,
        CAST((total_physical_memory_kb - available_physical_memory_kb) * 100.0 / total_physical_memory_kb AS DECIMAL(5,2)) AS MemoryUsagePercent
    FROM sys.dm_os_sys_info;
    
    -- Monitor query performance across replicas
    SELECT 
        'Query Performance' AS MetricType,
        @@SERVERNAME AS ServerName,
        AVG(total_elapsed_time) AS AvgElapsedTime,
        MAX(total_elapsed_time) AS MaxElapsedTime,
        COUNT(*) AS QueryCount
    FROM sys.dm_exec_query_stats
    WHERE last_execution_time >= DATEADD(HOUR, -1, GETDATE());
    
    -- Scalability recommendations
    SELECT 
        'Scalability Recommendations' AS MetricType,
        @@SERVERNAME AS ServerName,
        CASE 
            WHEN (SELECT COUNT(*) FROM sys.dm_exec_sessions WHERE is_user_process = 1) > 100 
            THEN 'Consider connection pooling or read-only replicas'
            WHEN (SELECT (total_physical_memory_kb - available_physical_memory_kb) * 100.0 / total_physical_memory_kb FROM sys.dm_os_sys_info) > 80 
            THEN 'Consider increasing memory or adding more replicas'
            ELSE 'Current configuration appears adequate'
        END AS Recommendation;
END;

-- Execute scalability monitoring
EXEC sp_MonitorScalability;
```

## 112. How do you implement database security and access control in T-SQL?

### Security Implementation
```sql
-- Example 1: Comprehensive security implementation
CREATE PROCEDURE sp_ImplementSecurity
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Create security roles
    IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = 'DataReader')
        CREATE ROLE DataReader;
    
    IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = 'DataWriter')
        CREATE ROLE DataWriter;
    
    IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = 'DataAdmin')
        CREATE ROLE DataAdmin;
    
    -- Grant permissions to roles
    GRANT SELECT ON SCHEMA::dbo TO DataReader;
    GRANT INSERT, UPDATE, DELETE ON SCHEMA::dbo TO DataWriter;
    GRANT ALL ON SCHEMA::dbo TO DataAdmin;
    
    -- Create application users
    IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = 'AppUser_ReadOnly')
        CREATE USER AppUser_ReadOnly FOR LOGIN AppUser_ReadOnly;
    
    IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = 'AppUser_ReadWrite')
        CREATE USER AppUser_ReadWrite FOR LOGIN AppUser_ReadWrite;
    
    -- Assign users to roles
    ALTER ROLE DataReader ADD MEMBER AppUser_ReadOnly;
    ALTER ROLE DataWriter ADD MEMBER AppUser_ReadWrite;
    
    -- Create row-level security function
    CREATE FUNCTION dbo.fn_SecurityPredicate(@CustomerID INT)
    RETURNS TABLE
    WITH SCHEMABINDING
    AS
    RETURN SELECT 1 AS fn_securitypredicate_result
    WHERE @CustomerID = CAST(SESSION_CONTEXT(N'CustomerID') AS INT);
    
    -- Apply security policy
    CREATE SECURITY POLICY dbo.CustomerSecurityPolicy
    ADD FILTER PREDICATE dbo.fn_SecurityPredicate(CustomerID) ON dbo.Orders;
    
    -- Return security configuration
    SELECT 
        'Security Configuration' AS ConfigurationType,
        'Roles Created' AS Item,
        COUNT(*) AS Count
    FROM sys.database_principals
    WHERE type = 'R' AND name IN ('DataReader', 'DataWriter', 'DataAdmin')
    
    UNION ALL
    
    SELECT 
        'Security Configuration',
        'Users Created',
        COUNT(*)
    FROM sys.database_principals
    WHERE type = 'S' AND name LIKE 'AppUser_%'
    
    UNION ALL
    
    SELECT 
        'Security Configuration',
        'Security Policies',
        COUNT(*)
    FROM sys.security_policies;
END;

-- Execute security implementation
EXEC sp_ImplementSecurity;
```

### Access Control
```sql
-- Example 2: Access control and permission management
CREATE PROCEDURE sp_ManageAccessControl
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Check current permissions
    SELECT 
        'Current Permissions' AS AccessType,
        p.name AS PrincipalName,
        p.type_desc AS PrincipalType,
        pr.permission_name AS Permission,
        pr.state_desc AS PermissionState,
        o.name AS ObjectName
    FROM sys.database_principals p
    LEFT JOIN sys.database_permissions pr ON p.principal_id = pr.grantee_principal_id
    LEFT JOIN sys.objects o ON pr.major_id = o.object_id
    WHERE p.name IN ('DataReader', 'DataWriter', 'DataAdmin', 'AppUser_ReadOnly', 'AppUser_ReadWrite')
    ORDER BY p.name, pr.permission_name;
    
    -- Check role memberships
    SELECT 
        'Role Memberships' AS AccessType,
        r.name AS RoleName,
        p.name AS MemberName,
        p.type_desc AS MemberType
    FROM sys.database_role_members rm
    INNER JOIN sys.database_principals r ON rm.role_principal_id = r.principal_id
    INNER JOIN sys.database_principals p ON rm.member_principal_id = p.principal_id
    WHERE r.name IN ('DataReader', 'DataWriter', 'DataAdmin')
    ORDER BY r.name, p.name;
    
    -- Check security policies
    SELECT 
        'Security Policies' AS AccessType,
        sp.name AS PolicyName,
        sp.is_enabled AS IsEnabled,
        sp.is_schema_bound AS IsSchemaBound,
        sp.is_not_for_replication AS IsNotForReplication
    FROM sys.security_policies sp
    ORDER BY sp.name;
    
    -- Access control recommendations
    SELECT 
        'Access Control Recommendations' AS AccessType,
        'Review excessive permissions' AS Recommendation,
        COUNT(*) AS ExcessivePermissionCount
    FROM sys.database_permissions
    WHERE permission_name IN ('CONTROL', 'ALTER', 'TAKE OWNERSHIP')
    
    UNION ALL
    
    SELECT 
        'Access Control Recommendations',
        'Review unused roles',
        COUNT(*)
    FROM sys.database_principals p
    LEFT JOIN sys.database_role_members rm ON p.principal_id = rm.role_principal_id
    WHERE p.type = 'R' AND rm.role_principal_id IS NULL;
END;

-- Execute access control management
EXEC sp_ManageAccessControl;
```

### Security Monitoring
```sql
-- Example 3: Security monitoring and auditing
CREATE PROCEDURE sp_MonitorSecurity
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Monitor failed login attempts
    SELECT 
        'Failed Logins' AS SecurityEvent,
        login_name,
        host_name,
        program_name,
        login_time,
        last_request_start_time
    FROM sys.dm_exec_sessions
    WHERE is_user_process = 1
        AND login_time >= DATEADD(HOUR, -24, GETDATE())
    ORDER BY login_time DESC;
    
    -- Monitor permission changes
    SELECT 
        'Permission Changes' AS SecurityEvent,
        event_time,
        server_principal_name,
        target_server_principal_name,
        permission_name,
        action_id
    FROM sys.fn_get_audit_file('C:\Audit\*.sqlaudit', DEFAULT, DEFAULT)
    WHERE action_id IN ('AUP', 'AUD', 'AUR')
        AND event_time >= DATEADD(DAY, -7, GETDATE())
    ORDER BY event_time DESC;
    
    -- Monitor data access
    SELECT 
        'Data Access' AS SecurityEvent,
        session_id,
        login_name,
        host_name,
        program_name,
        last_request_start_time
    FROM sys.dm_exec_sessions
    WHERE is_user_process = 1
        AND last_request_start_time >= DATEADD(HOUR, -1, GETDATE())
    ORDER BY last_request_start_time DESC;
    
    -- Security recommendations
    SELECT 
        'Security Recommendations' AS SecurityEvent,
        'Enable audit logging' AS Recommendation,
        CASE 
            WHEN EXISTS (SELECT 1 FROM sys.server_audits WHERE is_state_enabled = 1) 
            THEN 'COMPLIANT' 
            ELSE 'NON_COMPLIANT' 
        END AS Status
    UNION ALL
    SELECT 
        'Security Recommendations',
        'Implement row-level security',
        CASE 
            WHEN EXISTS (SELECT 1 FROM sys.security_policies) 
            THEN 'COMPLIANT' 
            ELSE 'NON_COMPLIANT' 
        END
    UNION ALL
    SELECT 
        'Security Recommendations',
        'Encrypt sensitive data',
        CASE 
            WHEN EXISTS (SELECT 1 FROM sys.symmetric_keys WHERE name = 'CustomerDataKey') 
            THEN 'COMPLIANT' 
            ELSE 'NON_COMPLIANT' 
        END;
END;

-- Execute security monitoring
EXEC sp_MonitorSecurity;
```

## 113. How do you implement database performance optimization in T-SQL?

### Performance Analysis
```sql
-- Example 1: Comprehensive performance analysis
CREATE PROCEDURE sp_AnalyzePerformance
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Analyze slow queries
    SELECT TOP 10
        'Slow Queries' AS AnalysisType,
        query_hash,
        total_elapsed_time,
        execution_count,
        total_elapsed_time / execution_count AS avg_elapsed_time,
        total_logical_reads,
        total_logical_writes,
        last_execution_time
    FROM sys.dm_exec_query_stats
    ORDER BY total_elapsed_time DESC;
    
    -- Analyze missing indexes
    SELECT 
        'Missing Indexes' AS AnalysisType,
        migs.avg_total_user_cost * (migs.avg_user_impact / 100.0) * (migs.user_seeks + migs.user_scans) AS improvement_measure,
        'CREATE INDEX IX_' + OBJECT_NAME(mid.object_id) + '_' + 
        REPLACE(REPLACE(REPLACE(ISNULL(mid.equality_columns, ''), ', ', '_'), '[', ''), ']', '') + 
        ' ON ' + mid.statement + ' (' + ISNULL(mid.equality_columns, '') + 
        CASE WHEN mid.equality_columns IS NOT NULL AND mid.inequality_columns IS NOT NULL 
             THEN ',' ELSE '' END + ISNULL(mid.inequality_columns, '') + ')' +
        CASE WHEN mid.included_columns IS NOT NULL 
             THEN ' INCLUDE (' + mid.included_columns + ')' ELSE '' END AS create_index_statement
    FROM sys.dm_db_missing_index_groups mig
    INNER JOIN sys.dm_db_missing_index_group_stats migs ON migs.group_handle = mig.index_group_handle
    INNER JOIN sys.dm_db_missing_index_details mid ON mig.index_handle = mid.index_handle
    WHERE migs.avg_total_user_cost * (migs.avg_user_impact / 100.0) * (migs.user_seeks + migs.user_scans) > 10
    ORDER BY improvement_measure DESC;
    
    -- Analyze index fragmentation
    SELECT 
        'Index Fragmentation' AS AnalysisType,
        OBJECT_NAME(ips.object_id) AS TableName,
        i.name AS IndexName,
        ips.avg_fragmentation_in_percent AS FragmentationPercent,
        ips.page_count,
        CASE 
            WHEN ips.avg_fragmentation_in_percent > 30 THEN 'REBUILD'
            WHEN ips.avg_fragmentation_in_percent > 10 THEN 'REORGANIZE'
            ELSE 'NO_ACTION'
        END AS RecommendedAction
    FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'LIMITED') ips
    INNER JOIN sys.indexes i ON ips.object_id = i.object_id AND ips.index_id = i.index_id
    WHERE ips.avg_fragmentation_in_percent > 10
        AND i.name IS NOT NULL
    ORDER BY ips.avg_fragmentation_in_percent DESC;
END;

-- Execute performance analysis
EXEC sp_AnalyzePerformance;
```

### Query Optimization
```sql
-- Example 2: Query optimization techniques
CREATE PROCEDURE sp_OptimizeQueries
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Create optimized indexes
    CREATE NONCLUSTERED INDEX IX_Orders_Customer_Date_Covering 
    ON Orders (CustomerID, OrderDate) 
    INCLUDE (TotalAmount, Status);
    
    CREATE NONCLUSTERED INDEX IX_Customers_Email 
    ON Customers (Email) 
    INCLUDE (CustomerName, Phone);
    
    -- Optimize query with proper joins
    SELECT 
        'Optimized Query' AS QueryType,
        c.CustomerName,
        c.Email,
        COUNT(o.OrderID) AS OrderCount,
        SUM(o.TotalAmount) AS TotalSpent
    FROM Customers c
    INNER JOIN Orders o ON c.CustomerID = o.CustomerID
    WHERE o.OrderDate >= DATEADD(MONTH, -6, GETDATE())
    GROUP BY c.CustomerID, c.CustomerName, c.Email
    HAVING COUNT(o.OrderID) > 5
    ORDER BY TotalSpent DESC;
    
    -- Use window functions for better performance
    SELECT 
        'Window Function Query' AS QueryType,
        CustomerID,
        OrderDate,
        TotalAmount,
        ROW_NUMBER() OVER (PARTITION BY CustomerID ORDER BY OrderDate DESC) AS RowNumber,
        SUM(TotalAmount) OVER (PARTITION BY CustomerID) AS CustomerTotal,
        AVG(TotalAmount) OVER (PARTITION BY CustomerID) AS CustomerAverage
    FROM Orders
    WHERE OrderDate >= DATEADD(MONTH, -3, GETDATE());
    
    -- Return optimization results
    SELECT 
        'Optimization Results' AS ResultType,
        'Indexes Created' AS Item,
        COUNT(*) AS Count
    FROM sys.indexes
    WHERE name LIKE 'IX_%_Covering' OR name LIKE 'IX_%_Email'
    
    UNION ALL
    
    SELECT 
        'Optimization Results',
        'Fragmented Indexes',
        COUNT(*)
    FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'LIMITED') ips
    INNER JOIN sys.indexes i ON ips.object_id = i.object_id AND ips.index_id = i.index_id
    WHERE ips.avg_fragmentation_in_percent > 10
        AND i.name IS NOT NULL;
END;

-- Execute query optimization
EXEC sp_OptimizeQueries;
```

### Performance Monitoring
```sql
-- Example 3: Performance monitoring and tuning
CREATE PROCEDURE sp_MonitorPerformance
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Monitor current performance
    SELECT 
        'Current Performance' AS MetricType,
        'Active Sessions' AS Metric,
        COUNT(*) AS Value
    FROM sys.dm_exec_sessions
    WHERE is_user_process = 1
    
    UNION ALL
    
    SELECT 
        'Current Performance',
        'Blocking Sessions',
        COUNT(*)
    FROM sys.dm_exec_requests
    WHERE blocking_session_id > 0
    
    UNION ALL
    
    SELECT 
        'Current Performance',
        'Memory Usage %',
        CAST((total_physical_memory_kb - available_physical_memory_kb) * 100.0 / total_physical_memory_kb AS DECIMAL(5,2))
    FROM sys.dm_os_sys_info;
    
    -- Monitor query performance
    SELECT 
        'Query Performance' AS MetricType,
        'Slow Queries' AS Metric,
        COUNT(*) AS Value
    FROM sys.dm_exec_query_stats
    WHERE total_elapsed_time > 1000000 -- Queries taking more than 1 second
    
    UNION ALL
    
    SELECT 
        'Query Performance',
        'CPU Intensive Queries',
        COUNT(*)
    FROM sys.dm_exec_query_stats
    WHERE total_worker_time > 1000000;
    
    -- Performance recommendations
    SELECT 
        'Performance Recommendations' AS MetricType,
        'Consider adding missing indexes' AS Recommendation,
        COUNT(*) AS MissingIndexCount
    FROM sys.dm_db_missing_index_groups mig
    INNER JOIN sys.dm_db_missing_index_group_stats migs ON migs.group_handle = mig.index_group_handle
    WHERE migs.avg_total_user_cost * (migs.avg_user_impact / 100.0) * (migs.user_seeks + migs.user_scans) > 10
    
    UNION ALL
    
    SELECT 
        'Performance Recommendations',
        'Consider rebuilding fragmented indexes',
        COUNT(*)
    FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'LIMITED') ips
    INNER JOIN sys.indexes i ON ips.object_id = i.object_id AND ips.index_id = i.index_id
    WHERE ips.avg_fragmentation_in_percent > 30
        AND i.name IS NOT NULL;
END;

-- Execute performance monitoring
EXEC sp_MonitorPerformance;
```

## 114. How do you implement database backup and recovery strategies in T-SQL?

### Backup Strategy
```sql
-- Example 1: Comprehensive backup strategy
CREATE PROCEDURE sp_ImplementBackupStrategy
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Full backup
    BACKUP DATABASE [YourDatabase] 
    TO DISK = 'C:\Backups\YourDatabase_Full.bak'
    WITH FORMAT, INIT, COMPRESSION, CHECKSUM;
    
    -- Differential backup
    BACKUP DATABASE [YourDatabase] 
    TO DISK = 'C:\Backups\YourDatabase_Diff.bak'
    WITH DIFFERENTIAL, FORMAT, INIT, COMPRESSION, CHECKSUM;
    
    -- Transaction log backup
    BACKUP LOG [YourDatabase] 
    TO DISK = 'C:\Backups\YourDatabase_Log.bak'
    WITH FORMAT, INIT, COMPRESSION, CHECKSUM;
    
    -- Verify backup integrity
    RESTORE VERIFYONLY FROM DISK = 'C:\Backups\YourDatabase_Full.bak';
    
    -- Return backup status
    SELECT 
        'Backup Status' AS StatusType,
        'Full Backup' AS BackupType,
        'Completed' AS Status,
        GETDATE() AS CompletionTime
    UNION ALL
    SELECT 
        'Backup Status',
        'Differential Backup',
        'Completed',
        GETDATE()
    UNION ALL
    SELECT 
        'Backup Status',
        'Log Backup',
        'Completed',
        GETDATE();
END;

-- Execute backup strategy
EXEC sp_ImplementBackupStrategy;
```

### Recovery Testing
```sql
-- Example 2: Recovery testing and validation
CREATE PROCEDURE sp_TestRecovery
    @BackupPath NVARCHAR(500),
    @TestDatabaseName NVARCHAR(128) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    IF @TestDatabaseName IS NULL
        SET @TestDatabaseName = 'TestRecovery_' + FORMAT(GETDATE(), 'yyyyMMdd_HHmmss');
    
    DECLARE @SQL NVARCHAR(MAX);
    DECLARE @StartTime DATETIME2 = GETDATE();
    
    BEGIN TRY
        -- Restore database for testing
        SET @SQL = 'RESTORE DATABASE [' + @TestDatabaseName + '] FROM DISK = ''' + @BackupPath + ''' WITH REPLACE, MOVE ''YourDatabase'' TO ''C:\Data\' + @TestDatabaseName + '.mdf'', MOVE ''YourDatabase_Log'' TO ''C:\Data\' + @TestDatabaseName + '_Log.ldf''';
        EXEC sp_executesql @SQL;
        
        -- Test database connectivity
        SET @SQL = 'SELECT COUNT(*) AS TableCount FROM [' + @TestDatabaseName + '].sys.tables';
        EXEC sp_executesql @SQL;
        
        -- Test data integrity
        SET @SQL = 'SELECT COUNT(*) AS RecordCount FROM [' + @TestDatabaseName + '].dbo.Orders';
        EXEC sp_executesql @SQL;
        
        -- Test stored procedures
        SET @SQL = 'SELECT COUNT(*) AS ProcedureCount FROM [' + @TestDatabaseName + '].sys.procedures';
        EXEC sp_executesql @SQL;
        
        -- Clean up test database
        SET @SQL = 'DROP DATABASE [' + @TestDatabaseName + ']';
        EXEC sp_executesql @SQL;
        
        DECLARE @EndTime DATETIME2 = GETDATE();
        SELECT 
            'Recovery test completed successfully' AS Status,
            DATEDIFF(SECOND, @StartTime, @EndTime) AS DurationSeconds,
            @TestDatabaseName AS TestDatabase;
        
    END TRY
    BEGIN CATCH
        -- Clean up on error
        IF EXISTS (SELECT 1 FROM sys.databases WHERE name = @TestDatabaseName)
        BEGIN
            SET @SQL = 'DROP DATABASE [' + @TestDatabaseName + ']';
            EXEC sp_executesql @SQL;
        END;
        
        SELECT 
            'Recovery test failed: ' + ERROR_MESSAGE() AS Status,
            @TestDatabaseName AS TestDatabase;
    END CATCH;
END;

-- Execute recovery test
EXEC sp_TestRecovery @BackupPath = 'C:\Backups\YourDatabase_Full.bak';
```

### Backup Monitoring
```sql
-- Example 3: Backup monitoring and validation
CREATE PROCEDURE sp_MonitorBackups
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Check backup history
    SELECT 
        'Backup History' AS MonitorType,
        database_name,
        backup_start_date,
        backup_finish_date,
        backup_size,
        backup_type,
        CASE 
            WHEN backup_type = 'D' THEN 'Full'
            WHEN backup_type = 'I' THEN 'Differential'
            WHEN backup_type = 'L' THEN 'Log'
        END AS BackupType,
        DATEDIFF(MINUTE, backup_start_date, backup_finish_date) AS DurationMinutes,
        CASE 
            WHEN has_backup_checksums = 1 THEN 'Yes'
            ELSE 'No'
        END AS HasChecksums
    FROM msdb.dbo.backupset
    WHERE database_name = DB_NAME()
    ORDER BY backup_start_date DESC;
    
    -- Check for recent backups
    DECLARE @LastFullBackup DATETIME2;
    DECLARE @LastDiffBackup DATETIME2;
    DECLARE @LastLogBackup DATETIME2;
    
    SELECT @LastFullBackup = MAX(backup_start_date)
    FROM msdb.dbo.backupset
    WHERE database_name = DB_NAME() AND backup_type = 'D';
    
    SELECT @LastDiffBackup = MAX(backup_start_date)
    FROM msdb.dbo.backupset
    WHERE database_name = DB_NAME() AND backup_type = 'I';
    
    SELECT @LastLogBackup = MAX(backup_start_date)
    FROM msdb.dbo.backupset
    WHERE database_name = DB_NAME() AND backup_type = 'L';
    
    -- Backup age analysis
    SELECT 
        'Backup Age Analysis' AS MonitorType,
        'Full Backup' AS BackupType,
        @LastFullBackup AS LastBackup,
        DATEDIFF(HOUR, @LastFullBackup, GETDATE()) AS HoursAgo,
        CASE 
            WHEN DATEDIFF(HOUR, @LastFullBackup, GETDATE()) > 24 THEN 'OVERDUE'
            WHEN DATEDIFF(HOUR, @LastFullBackup, GETDATE()) > 12 THEN 'WARNING'
            ELSE 'OK'
        END AS Status
    UNION ALL
    SELECT 
        'Backup Age Analysis',
        'Differential Backup',
        @LastDiffBackup,
        DATEDIFF(HOUR, @LastDiffBackup, GETDATE()),
        CASE 
            WHEN DATEDIFF(HOUR, @LastDiffBackup, GETDATE()) > 6 THEN 'OVERDUE'
            WHEN DATEDIFF(HOUR, @LastDiffBackup, GETDATE()) > 3 THEN 'WARNING'
            ELSE 'OK'
        END
    UNION ALL
    SELECT 
        'Backup Age Analysis',
        'Log Backup',
        @LastLogBackup,
        DATEDIFF(MINUTE, @LastLogBackup, GETDATE()),
        CASE 
            WHEN DATEDIFF(MINUTE, @LastLogBackup, GETDATE()) > 60 THEN 'OVERDUE'
            WHEN DATEDIFF(MINUTE, @LastLogBackup, GETDATE()) > 30 THEN 'WARNING'
            ELSE 'OK'
        END;
END;

-- Execute backup monitoring
EXEC sp_MonitorBackups;
```

## 115. How do you implement database maintenance and optimization in T-SQL?

### Maintenance Tasks
```sql
-- Example 1: Automated maintenance tasks
CREATE PROCEDURE sp_AutomatedMaintenance
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @StartTime DATETIME2 = GETDATE();
    DECLARE @Results TABLE (
        TaskName NVARCHAR(100),
        Status NVARCHAR(20),
        Duration INT,
        Details NVARCHAR(MAX)
    );
    
    -- Task 1: Update statistics
    BEGIN TRY
        DECLARE @StatsStart DATETIME2 = GETDATE();
        EXEC sp_updatestats;
        INSERT INTO @Results VALUES ('Update Statistics', 'SUCCESS', DATEDIFF(SECOND, @StatsStart, GETDATE()), 'All statistics updated');
    END TRY
    BEGIN CATCH
        INSERT INTO @Results VALUES ('Update Statistics', 'FAILED', 0, ERROR_MESSAGE());
    END CATCH;
    
    -- Task 2: Rebuild fragmented indexes
    BEGIN TRY
        DECLARE @IndexStart DATETIME2 = GETDATE();
        DECLARE @SQL NVARCHAR(MAX);
        
        SET @SQL = 'ALTER INDEX ALL ON Orders REBUILD';
        EXEC sp_executesql @SQL;
        
        INSERT INTO @Results VALUES ('Rebuild Indexes', 'SUCCESS', DATEDIFF(SECOND, @IndexStart, GETDATE()), 'All indexes rebuilt');
    END TRY
    BEGIN CATCH
        INSERT INTO @Results VALUES ('Rebuild Indexes', 'FAILED', 0, ERROR_MESSAGE());
    END CATCH;
    
    -- Task 3: Clean up old data
    BEGIN TRY
        DECLARE @CleanupStart DATETIME2 = GETDATE();
        DECLARE @DeletedCount INT;
        
        DELETE FROM SystemLogs
        WHERE LogDate < DATEADD(MONTH, -3, GETDATE());
        
        SET @DeletedCount = @@ROWCOUNT;
        INSERT INTO @Results VALUES ('Data Cleanup', 'SUCCESS', DATEDIFF(SECOND, @CleanupStart, GETDATE()), 'Deleted ' + CAST(@DeletedCount AS VARCHAR(10)) + ' old records');
    END TRY
    BEGIN CATCH
        INSERT INTO @Results VALUES ('Data Cleanup', 'FAILED', 0, ERROR_MESSAGE());
    END CATCH;
    
    -- Return results
    SELECT 
        TaskName,
        Status,
        Duration,
        Details,
        DATEDIFF(SECOND, @StartTime, GETDATE()) AS TotalDuration
    FROM @Results;
END;

-- Execute automated maintenance
EXEC sp_AutomatedMaintenance;
```

### Index Maintenance
```sql
-- Example 2: Index maintenance and optimization
CREATE PROCEDURE sp_MaintainIndexes
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @Results TABLE (
        TableName NVARCHAR(128),
        IndexName NVARCHAR(128),
        FragmentationPercent DECIMAL(5,2),
        ActionTaken NVARCHAR(50),
        Duration INT
    );
    
    -- Analyze and maintain indexes
    DECLARE @TableName NVARCHAR(128);
    DECLARE @IndexName NVARCHAR(128);
    DECLARE @FragmentationPercent DECIMAL(5,2);
    DECLARE @Action NVARCHAR(50);
    DECLARE @SQL NVARCHAR(MAX);
    DECLARE @StartTime DATETIME2;
    
    DECLARE index_cursor CURSOR FOR
    SELECT 
        OBJECT_NAME(ips.object_id) AS TableName,
        i.name AS IndexName,
        ips.avg_fragmentation_in_percent AS FragmentationPercent
    FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'LIMITED') ips
    INNER JOIN sys.indexes i ON ips.object_id = i.object_id AND ips.index_id = i.index_id
    WHERE ips.avg_fragmentation_in_percent > 10
        AND i.name IS NOT NULL;
    
    OPEN index_cursor;
    FETCH NEXT FROM index_cursor INTO @TableName, @IndexName, @FragmentationPercent;
    
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @StartTime = GETDATE();
        
        IF @FragmentationPercent > 30
        BEGIN
            SET @Action = 'REBUILD';
            SET @SQL = 'ALTER INDEX [' + @IndexName + '] ON [' + @TableName + '] REBUILD';
        END
        ELSE IF @FragmentationPercent > 10
        BEGIN
            SET @Action = 'REORGANIZE';
            SET @SQL = 'ALTER INDEX [' + @IndexName + '] ON [' + @TableName + '] REORGANIZE';
        END;
        
        BEGIN TRY
            EXEC sp_executesql @SQL;
            INSERT INTO @Results VALUES (@TableName, @IndexName, @FragmentationPercent, @Action, DATEDIFF(SECOND, @StartTime, GETDATE()));
        END TRY
        BEGIN CATCH
            INSERT INTO @Results VALUES (@TableName, @IndexName, @FragmentationPercent, 'FAILED: ' + ERROR_MESSAGE(), 0);
        END CATCH;
        
        FETCH NEXT FROM index_cursor INTO @TableName, @IndexName, @FragmentationPercent;
    END;
    
    CLOSE index_cursor;
    DEALLOCATE index_cursor;
    
    -- Return results
    SELECT * FROM @Results ORDER BY FragmentationPercent DESC;
END;

-- Execute index maintenance
EXEC sp_MaintainIndexes;
```

### Database Optimization
```sql
-- Example 3: Database optimization and tuning
CREATE PROCEDURE sp_OptimizeDatabase
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Analyze database size and growth
    SELECT 
        'Database Size Analysis' AS AnalysisType,
        name AS DatabaseName,
        size * 8 / 1024 AS SizeMB,
        max_size * 8 / 1024 AS MaxSizeMB,
        growth AS GrowthMB,
        is_percent_growth AS IsPercentGrowth
    FROM sys.database_files
    ORDER BY size DESC;
    
    -- Analyze table sizes
    SELECT 
        'Table Size Analysis' AS AnalysisType,
        t.name AS TableName,
        p.rows AS RowCount,
        SUM(a.total_pages) * 8 / 1024 AS TotalSizeMB,
        SUM(a.used_pages) * 8 / 1024 AS UsedSizeMB,
        SUM(a.data_pages) * 8 / 1024 AS DataSizeMB
    FROM sys.tables t
    INNER JOIN sys.indexes i ON t.object_id = i.object_id
    INNER JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
    INNER JOIN sys.allocation_units a ON p.partition_id = a.container_id
    GROUP BY t.name, p.rows
    ORDER BY TotalSizeMB DESC;
    
    -- Analyze index usage
    SELECT 
        'Index Usage Analysis' AS AnalysisType,
        OBJECT_NAME(i.object_id) AS TableName,
        i.name AS IndexName,
        i.type_desc AS IndexType,
        s.user_seeks,
        s.user_scans,
        s.user_lookups,
        s.user_updates,
        s.last_user_seek,
        s.last_user_scan,
        s.last_user_lookup
    FROM sys.indexes i
    LEFT JOIN sys.dm_db_index_usage_stats s ON i.object_id = s.object_id AND i.index_id = s.index_id
    WHERE i.name IS NOT NULL
    ORDER BY s.user_seeks + s.user_scans + s.user_lookups DESC;
    
    -- Optimization recommendations
    SELECT 
        'Optimization Recommendations' AS AnalysisType,
        'Consider removing unused indexes' AS Recommendation,
        COUNT(*) AS UnusedIndexCount
    FROM sys.indexes i
    LEFT JOIN sys.dm_db_index_usage_stats s ON i.object_id = s.object_id AND i.index_id = s.index_id
    WHERE i.name IS NOT NULL
        AND (s.user_seeks + s.user_scans + s.user_lookups) = 0
        AND i.type_desc != 'HEAP'
    
    UNION ALL
    
    SELECT 
        'Optimization Recommendations',
        'Consider adding missing indexes',
        COUNT(*)
    FROM sys.dm_db_missing_index_groups mig
    INNER JOIN sys.dm_db_missing_index_group_stats migs ON migs.group_handle = mig.index_group_handle
    WHERE migs.avg_total_user_cost * (migs.avg_user_impact / 100.0) * (migs.user_seeks + migs.user_scans) > 10;
END;

-- Execute database optimization
EXEC sp_OptimizeDatabase;
```



## 116. How do you implement database monitoring and alerting in T-SQL?

### Performance Monitoring
```sql
-- Example 1: Comprehensive performance monitoring
CREATE PROCEDURE sp_MonitorDatabasePerformance
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Monitor active sessions
    SELECT 
        'Active Sessions' AS Metric,
        COUNT(*) AS Value
    FROM sys.dm_exec_sessions
    WHERE is_user_process = 1
    
    UNION ALL
    
    -- Monitor blocking
    SELECT 
        'Blocking Sessions' AS Metric,
        COUNT(*) AS Value
    FROM sys.dm_exec_requests
    WHERE blocking_session_id > 0
    
    UNION ALL
    
    -- Monitor memory usage
    SELECT 
        'Memory Usage %' AS Metric,
        CAST((total_physical_memory_kb - available_physical_memory_kb) * 100.0 / total_physical_memory_kb AS DECIMAL(5,2)) AS Value
    FROM sys.dm_os_sys_info
    
    UNION ALL
    
    -- Monitor CPU usage
    SELECT 
        'CPU Usage %' AS Metric,
        CAST(cpu_percent AS DECIMAL(5,2)) AS Value
    FROM sys.dm_os_ring_buffers
    WHERE ring_buffer_type = 'RING_BUFFER_SCHEDULER_MONITOR';
END;

-- Execute monitoring
EXEC sp_MonitorDatabasePerformance;
```

**Result:**
```
Metric          | Value
----------------|-------
Active Sessions | 15
Blocking Sessions | 2
Memory Usage %  | 75.50
CPU Usage %     | 45.20
```

### Alert System
```sql
-- Example 2: Database alerting system
CREATE TABLE DatabaseAlerts (
    AlertID INT IDENTITY(1,1) PRIMARY KEY,
    AlertType NVARCHAR(50) NOT NULL,
    AlertMessage NVARCHAR(MAX) NOT NULL,
    AlertLevel NVARCHAR(20) NOT NULL,
    CreatedDate DATETIME2 DEFAULT GETDATE(),
    IsResolved BIT DEFAULT 0
);

CREATE PROCEDURE sp_CheckDatabaseAlerts
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @MemoryUsage DECIMAL(5,2);
    DECLARE @CPUUsage DECIMAL(5,2);
    DECLARE @BlockingCount INT;
    DECLARE @ActiveSessions INT;
    
    -- Get current metrics
    SELECT @MemoryUsage = (total_physical_memory_kb - available_physical_memory_kb) * 100.0 / total_physical_memory_kb
    FROM sys.dm_os_sys_info;
    
    SELECT @CPUUsage = cpu_percent
    FROM sys.dm_os_ring_buffers
    WHERE ring_buffer_type = 'RING_BUFFER_SCHEDULER_MONITOR';
    
    SELECT @BlockingCount = COUNT(*)
    FROM sys.dm_exec_requests
    WHERE blocking_session_id > 0;
    
    SELECT @ActiveSessions = COUNT(*)
    FROM sys.dm_exec_sessions
    WHERE is_user_process = 1;
    
    -- Check for alerts
    IF @MemoryUsage > 90
    BEGIN
        INSERT INTO DatabaseAlerts (AlertType, AlertMessage, AlertLevel)
        VALUES ('Memory', 'Memory usage is above 90%: ' + CAST(@MemoryUsage AS VARCHAR(10)) + '%', 'CRITICAL');
    END;
    
    IF @CPUUsage > 80
    BEGIN
        INSERT INTO DatabaseAlerts (AlertType, AlertMessage, AlertLevel)
        VALUES ('CPU', 'CPU usage is above 80%: ' + CAST(@CPUUsage AS VARCHAR(10)) + '%', 'WARNING');
    END;
    
    IF @BlockingCount > 5
    BEGIN
        INSERT INTO DatabaseAlerts (AlertType, AlertMessage, AlertLevel)
        VALUES ('Blocking', 'High number of blocking sessions: ' + CAST(@BlockingCount AS VARCHAR(10)), 'WARNING');
    END;
    
    IF @ActiveSessions > 100
    BEGIN
        INSERT INTO DatabaseAlerts (AlertType, AlertMessage, AlertLevel)
        VALUES ('Sessions', 'High number of active sessions: ' + CAST(@ActiveSessions AS VARCHAR(10)), 'INFO');
    END;
    
    -- Return current alerts
    SELECT * FROM DatabaseAlerts
    WHERE IsResolved = 0
    ORDER BY CreatedDate DESC;
END;

-- Execute alert check
EXEC sp_CheckDatabaseAlerts;
```

### Automated Monitoring
```sql
-- Example 3: Automated monitoring with thresholds
CREATE PROCEDURE sp_AutomatedMonitoring
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @Thresholds TABLE (
        MetricName NVARCHAR(50),
        WarningThreshold DECIMAL(10,2),
        CriticalThreshold DECIMAL(10,2),
        CurrentValue DECIMAL(10,2)
    );
    
    -- Define thresholds and get current values
    INSERT INTO @Thresholds (MetricName, WarningThreshold, CriticalThreshold, CurrentValue)
    VALUES 
        ('MemoryUsage', 80.0, 90.0, (SELECT (total_physical_memory_kb - available_physical_memory_kb) * 100.0 / total_physical_memory_kb FROM sys.dm_os_sys_info)),
        ('CPUUsage', 70.0, 85.0, (SELECT cpu_percent FROM sys.dm_os_ring_buffers WHERE ring_buffer_type = 'RING_BUFFER_SCHEDULER_MONITOR')),
        ('BlockingSessions', 3.0, 10.0, (SELECT COUNT(*) FROM sys.dm_exec_requests WHERE blocking_session_id > 0));
    
    -- Check thresholds and create alerts
    SELECT 
        MetricName,
        CurrentValue,
        CASE 
            WHEN CurrentValue >= CriticalThreshold THEN 'CRITICAL'
            WHEN CurrentValue >= WarningThreshold THEN 'WARNING'
            ELSE 'OK'
        END AS AlertLevel,
        CASE 
            WHEN CurrentValue >= CriticalThreshold THEN 'Critical threshold exceeded'
            WHEN CurrentValue >= WarningThreshold THEN 'Warning threshold exceeded'
            ELSE 'All metrics within normal range'
        END AS AlertMessage
    FROM @Thresholds;
END;

-- Execute automated monitoring
EXEC sp_AutomatedMonitoring;
```

## 117. How do you implement database security and compliance in T-SQL?

### Security Audit Implementation
```sql
-- Example 1: Comprehensive security audit
CREATE PROCEDURE sp_SecurityAudit
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Check user permissions
    SELECT 
        'User Permissions' AS AuditCategory,
        p.name AS PrincipalName,
        p.type_desc AS PrincipalType,
        r.name AS RoleName,
        r.type_desc AS RoleType
    FROM sys.database_principals p
    LEFT JOIN sys.database_role_members rm ON p.principal_id = rm.member_principal_id
    LEFT JOIN sys.database_principals r ON rm.role_principal_id = r.principal_id
    WHERE p.type IN ('S', 'U', 'G')
    
    UNION ALL
    
    -- Check object permissions
    SELECT 
        'Object Permissions' AS AuditCategory,
        p.name AS PrincipalName,
        o.name AS ObjectName,
        pr.permission_name AS Permission,
        pr.state_desc AS PermissionState
    FROM sys.database_principals p
    CROSS JOIN sys.objects o
    INNER JOIN sys.database_permissions pr ON p.principal_id = pr.grantee_principal_id
    WHERE o.type IN ('U', 'V', 'P', 'FN', 'IF', 'TF');
    
    -- Check for excessive permissions
    SELECT 
        'Excessive Permissions' AS AlertType,
        COUNT(*) AS Count
    FROM sys.database_permissions
    WHERE permission_name IN ('CONTROL', 'ALTER', 'TAKE OWNERSHIP')
    
    UNION ALL
    
    SELECT 
        'Public Role Members' AS AlertType,
        COUNT(*) AS Count
    FROM sys.database_role_members rm
    INNER JOIN sys.database_principals r ON rm.role_principal_id = r.principal_id
    WHERE r.name = 'public';
END;

-- Execute security audit
EXEC sp_SecurityAudit;
```

**Result:**
```
AuditCategory      | PrincipalName | PrincipalType | RoleName | RoleType
-------------------|---------------|---------------|----------|----------
User Permissions   | dbo           | SQL_USER      | db_owner | DATABASE_ROLE
User Permissions   | guest         | SQL_USER      | NULL     | NULL
Object Permissions | dbo           | Orders        | SELECT   | GRANT
```

### Data Encryption Implementation
```sql
-- Example 2: Data encryption and decryption
-- Create encryption key
CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'YourStrongPassword123!';

-- Create certificate
CREATE CERTIFICATE CustomerDataCert
WITH SUBJECT = 'Customer Data Encryption Certificate';

-- Create symmetric key
CREATE SYMMETRIC KEY CustomerDataKey
WITH ALGORITHM = AES_256
ENCRYPTION BY CERTIFICATE CustomerDataCert;

-- Create table with encrypted columns
CREATE TABLE EncryptedCustomers (
    CustomerID INT IDENTITY(1,1) PRIMARY KEY,
    CustomerName NVARCHAR(100),
    EncryptedSSN VARBINARY(MAX),
    EncryptedEmail VARBINARY(MAX),
    CreatedDate DATETIME2 DEFAULT GETDATE()
);

-- Insert encrypted data
OPEN SYMMETRIC KEY CustomerDataKey DECRYPTION BY CERTIFICATE CustomerDataCert;

INSERT INTO EncryptedCustomers (CustomerName, EncryptedSSN, EncryptedEmail)
VALUES 
    ('John Smith', 
     EncryptByKey(Key_GUID('CustomerDataKey'), '123-45-6789'),
     EncryptByKey(Key_GUID('CustomerDataKey'), 'john@example.com')),
    ('Jane Doe', 
     EncryptByKey(Key_GUID('CustomerDataKey'), '987-65-4321'),
     EncryptByKey(Key_GUID('CustomerDataKey'), 'jane@example.com'));

CLOSE SYMMETRIC KEY CustomerDataKey;

-- Query decrypted data
OPEN SYMMETRIC KEY CustomerDataKey DECRYPTION BY CERTIFICATE CustomerDataCert;

SELECT 
    CustomerID,
    CustomerName,
    CONVERT(NVARCHAR(20), DecryptByKey(EncryptedSSN)) AS DecryptedSSN,
    CONVERT(NVARCHAR(100), DecryptByKey(EncryptedEmail)) AS DecryptedEmail
FROM EncryptedCustomers;

CLOSE SYMMETRIC KEY CustomerDataKey;
```

**Result:**
```
CustomerID | CustomerName | DecryptedSSN | DecryptedEmail
-----------|--------------|--------------|----------------
1          | John Smith   | 123-45-6789  | john@example.com
2          | Jane Doe     | 987-65-4321  | jane@example.com
```

### Compliance Reporting
```sql
-- Example 3: Compliance reporting and monitoring
CREATE PROCEDURE sp_ComplianceReport
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Data access audit
    SELECT 
        'Data Access Audit' AS ReportSection,
        session_id,
        login_name,
        host_name,
        program_name,
        login_time,
        last_request_start_time
    FROM sys.dm_exec_sessions
    WHERE is_user_process = 1
        AND login_time >= DATEADD(DAY, -7, GETDATE())
    ORDER BY login_time DESC;
    
    -- Permission changes audit
    SELECT 
        'Permission Changes' AS ReportSection,
        event_time,
        server_principal_name,
        target_server_principal_name,
        permission_name,
        action_id
    FROM sys.fn_get_audit_file('C:\Audit\*.sqlaudit', DEFAULT, DEFAULT)
    WHERE action_id IN ('AUP', 'AUD', 'AUR')
        AND event_time >= DATEADD(DAY, -7, GETDATE())
    ORDER BY event_time DESC;
    
    -- Data modification audit
    SELECT 
        'Data Modifications' AS ReportSection,
        COUNT(*) AS ModificationCount,
        MAX(event_time) AS LastModification
    FROM sys.fn_get_audit_file('C:\Audit\*.sqlaudit', DEFAULT, DEFAULT)
    WHERE action_id IN ('IN', 'UP', 'DL')
        AND event_time >= DATEADD(DAY, -7, GETDATE());
    
    -- Compliance status
    SELECT 
        'Compliance Status' AS ReportSection,
        'Data Encryption' AS ComplianceItem,
        CASE 
            WHEN EXISTS (SELECT 1 FROM sys.symmetric_keys WHERE name = 'CustomerDataKey') 
            THEN 'COMPLIANT' 
            ELSE 'NON_COMPLIANT' 
        END AS Status
    UNION ALL
    SELECT 
        'Compliance Status',
        'Audit Logging',
        CASE 
            WHEN EXISTS (SELECT 1 FROM sys.server_audits WHERE is_state_enabled = 1) 
            THEN 'COMPLIANT' 
            ELSE 'NON_COMPLIANT' 
        END
    UNION ALL
    SELECT 
        'Compliance Status',
        'Row Level Security',
        CASE 
            WHEN EXISTS (SELECT 1 FROM sys.security_policies) 
            THEN 'COMPLIANT' 
            ELSE 'NON_COMPLIANT' 
        END;
END;

-- Execute compliance report
EXEC sp_ComplianceReport;
```

## 118. How do you implement database performance tuning and optimization in T-SQL?

### Query Performance Analysis
```sql
-- Example 1: Query performance analysis
CREATE PROCEDURE sp_AnalyzeQueryPerformance
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Top 10 slowest queries
    SELECT TOP 10
        'Slowest Queries' AS AnalysisType,
        query_hash,
        query_plan_hash,
        total_elapsed_time,
        execution_count,
        total_elapsed_time / execution_count AS avg_elapsed_time,
        total_logical_reads,
        total_logical_writes,
        last_execution_time
    FROM sys.dm_exec_query_stats
    ORDER BY total_elapsed_time DESC;
    
    -- Most expensive queries by CPU
    SELECT TOP 10
        'CPU Intensive Queries' AS AnalysisType,
        query_hash,
        total_worker_time,
        execution_count,
        total_worker_time / execution_count AS avg_worker_time,
        total_logical_reads,
        last_execution_time
    FROM sys.dm_exec_query_stats
    ORDER BY total_worker_time DESC;
    
    -- Missing index recommendations
    SELECT 
        'Missing Indexes' AS AnalysisType,
        migs.avg_total_user_cost * (migs.avg_user_impact / 100.0) * (migs.user_seeks + migs.user_scans) AS improvement_measure,
        'CREATE INDEX IX_' + OBJECT_NAME(mid.object_id) + '_' + 
        REPLACE(REPLACE(REPLACE(ISNULL(mid.equality_columns, ''), ', ', '_'), '[', ''), ']', '') + 
        ' ON ' + mid.statement + ' (' + ISNULL(mid.equality_columns, '') + 
        CASE WHEN mid.equality_columns IS NOT NULL AND mid.inequality_columns IS NOT NULL 
             THEN ',' ELSE '' END + ISNULL(mid.inequality_columns, '') + ')' +
        CASE WHEN mid.included_columns IS NOT NULL 
             THEN ' INCLUDE (' + mid.included_columns + ')' ELSE '' END AS create_index_statement
    FROM sys.dm_db_missing_index_groups mig
    INNER JOIN sys.dm_db_missing_index_group_stats migs ON migs.group_handle = mig.index_group_handle
    INNER JOIN sys.dm_db_missing_index_details mid ON mig.index_handle = mid.index_handle
    WHERE migs.avg_total_user_cost * (migs.avg_user_impact / 100.0) * (migs.user_seeks + migs.user_scans) > 10
    ORDER BY improvement_measure DESC;
END;

-- Execute performance analysis
EXEC sp_AnalyzeQueryPerformance;
```

### Index Optimization
```sql
-- Example 2: Index optimization and maintenance
CREATE PROCEDURE sp_OptimizeIndexes
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @Results TABLE (
        TableName NVARCHAR(128),
        IndexName NVARCHAR(128),
        FragmentationPercent DECIMAL(5,2),
        RecommendedAction NVARCHAR(20),
        EstimatedTime INT
    );
    
    -- Analyze index fragmentation
    INSERT INTO @Results (TableName, IndexName, FragmentationPercent, RecommendedAction, EstimatedTime)
    SELECT 
        OBJECT_NAME(ips.object_id) AS TableName,
        i.name AS IndexName,
        ips.avg_fragmentation_in_percent AS FragmentationPercent,
        CASE 
            WHEN ips.avg_fragmentation_in_percent > 30 THEN 'REBUILD'
            WHEN ips.avg_fragmentation_in_percent > 10 THEN 'REORGANIZE'
            ELSE 'NO_ACTION'
        END AS RecommendedAction,
        CASE 
            WHEN ips.avg_fragmentation_in_percent > 30 THEN ips.page_count / 1000
            WHEN ips.avg_fragmentation_in_percent > 10 THEN ips.page_count / 2000
            ELSE 0
        END AS EstimatedTime
    FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'LIMITED') ips
    INNER JOIN sys.indexes i ON ips.object_id = i.object_id AND ips.index_id = i.index_id
    WHERE ips.avg_fragmentation_in_percent > 10
        AND i.name IS NOT NULL;
    
    -- Execute recommended actions
    DECLARE @TableName NVARCHAR(128);
    DECLARE @IndexName NVARCHAR(128);
    DECLARE @Action NVARCHAR(20);
    DECLARE @SQL NVARCHAR(MAX);
    
    DECLARE action_cursor CURSOR FOR
    SELECT TableName, IndexName, RecommendedAction
    FROM @Results
    WHERE RecommendedAction != 'NO_ACTION';
    
    OPEN action_cursor;
    FETCH NEXT FROM action_cursor INTO @TableName, @IndexName, @Action;
    
    WHILE @@FETCH_STATUS = 0
    BEGIN
        IF @Action = 'REBUILD'
        BEGIN
            SET @SQL = 'ALTER INDEX [' + @IndexName + '] ON [' + @TableName + '] REBUILD';
        END
        ELSE IF @Action = 'REORGANIZE'
        BEGIN
            SET @SQL = 'ALTER INDEX [' + @IndexName + '] ON [' + @TableName + '] REORGANIZE';
        END;
        
        BEGIN TRY
            EXEC sp_executesql @SQL;
            PRINT 'Executed: ' + @SQL;
        END TRY
        BEGIN CATCH
            PRINT 'Failed: ' + @SQL + ' - ' + ERROR_MESSAGE();
        END CATCH;
        
        FETCH NEXT FROM action_cursor INTO @TableName, @IndexName, @Action;
    END;
    
    CLOSE action_cursor;
    DEALLOCATE action_cursor;
    
    -- Return results
    SELECT * FROM @Results ORDER BY FragmentationPercent DESC;
END;

-- Execute index optimization
EXEC sp_OptimizeIndexes;
```

### Memory and Resource Optimization
```sql
-- Example 3: Memory and resource optimization
CREATE PROCEDURE sp_OptimizeResources
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Memory usage analysis
    SELECT 
        'Memory Usage' AS ResourceType,
        (total_physical_memory_kb / 1024.0) AS TotalMemoryGB,
        (available_physical_memory_kb / 1024.0) AS AvailableMemoryGB,
        ((total_physical_memory_kb - available_physical_memory_kb) / 1024.0) AS UsedMemoryGB,
        CAST((total_physical_memory_kb - available_physical_memory_kb) * 100.0 / total_physical_memory_kb AS DECIMAL(5,2)) AS MemoryUsagePercent
    FROM sys.dm_os_sys_info;
    
    -- Buffer pool analysis
    SELECT 
        'Buffer Pool' AS ResourceType,
        COUNT(*) AS PageCount,
        COUNT(*) * 8 / 1024 AS SizeMB,
        SUM(CASE WHEN is_modified = 1 THEN 1 ELSE 0 END) AS DirtyPages,
        SUM(CASE WHEN is_modified = 0 THEN 1 ELSE 0 END) AS CleanPages
    FROM sys.dm_os_buffer_descriptors;
    
    -- Top memory consuming queries
    SELECT TOP 10
        'Memory Intensive Queries' AS ResourceType,
        query_hash,
        total_logical_reads,
        total_logical_writes,
        total_physical_reads,
        execution_count,
        total_logical_reads / execution_count AS avg_logical_reads
    FROM sys.dm_exec_query_stats
    ORDER BY total_logical_reads DESC;
    
    -- Resource recommendations
    SELECT 
        'Resource Recommendations' AS ResourceType,
        CASE 
            WHEN (SELECT (total_physical_memory_kb - available_physical_memory_kb) * 100.0 / total_physical_memory_kb FROM sys.dm_os_sys_info) > 90 
            THEN 'Consider increasing memory or optimizing queries'
            WHEN (SELECT (total_physical_memory_kb - available_physical_memory_kb) * 100.0 / total_physical_memory_kb FROM sys.dm_os_sys_info) > 80 
            THEN 'Monitor memory usage closely'
            ELSE 'Memory usage is within acceptable range'
        END AS Recommendation;
END;

-- Execute resource optimization
EXEC sp_OptimizeResources;
```

## 119. How do you implement database backup and recovery strategies in T-SQL?

### Automated Backup Strategy
```sql
-- Example 1: Automated backup strategy
CREATE PROCEDURE sp_AutomatedBackup
    @BackupType NVARCHAR(20) = 'FULL', -- 'FULL', 'DIFF', 'LOG'
    @BackupPath NVARCHAR(500) = 'C:\Backups\'
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @DatabaseName NVARCHAR(128) = DB_NAME();
    DECLARE @BackupFileName NVARCHAR(500);
    DECLARE @SQL NVARCHAR(MAX);
    DECLARE @StartTime DATETIME2 = GETDATE();
    
    -- Generate backup filename
    SET @BackupFileName = @BackupPath + @DatabaseName + '_' + @BackupType + '_' + 
                         FORMAT(GETDATE(), 'yyyyMMdd_HHmmss') + '.bak';
    
    IF @BackupType = 'FULL'
    BEGIN
        SET @SQL = 'BACKUP DATABASE [' + @DatabaseName + '] TO DISK = ''' + @BackupFileName + ''' WITH FORMAT, INIT, COMPRESSION, CHECKSUM';
    END
    ELSE IF @BackupType = 'DIFF'
    BEGIN
        SET @SQL = 'BACKUP DATABASE [' + @DatabaseName + '] TO DISK = ''' + @BackupFileName + ''' WITH DIFFERENTIAL, FORMAT, INIT, COMPRESSION, CHECKSUM';
    END
    ELSE IF @BackupType = 'LOG'
    BEGIN
        SET @SQL = 'BACKUP LOG [' + @DatabaseName + '] TO DISK = ''' + @BackupFileName + ''' WITH FORMAT, INIT, COMPRESSION, CHECKSUM';
    END;
    
    BEGIN TRY
        EXEC sp_executesql @SQL;
        
        DECLARE @EndTime DATETIME2 = GETDATE();
        SELECT 
            'Backup completed successfully' AS Status,
            @BackupFileName AS BackupFile,
            DATEDIFF(SECOND, @StartTime, @EndTime) AS DurationSeconds,
            @BackupType AS BackupType;
            
    END TRY
    BEGIN CATCH
        SELECT 
            'Backup failed: ' + ERROR_MESSAGE() AS Status,
            @BackupFileName AS BackupFile,
            @BackupType AS BackupType;
    END CATCH;
END;

-- Execute automated backup
EXEC sp_AutomatedBackup @BackupType = 'FULL', @BackupPath = 'C:\Backups\';
```

### Backup Verification
```sql
-- Example 2: Backup verification and integrity checks
CREATE PROCEDURE sp_VerifyBackups
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Check backup history
    SELECT 
        'Backup History' AS CheckType,
        database_name,
        backup_start_date,
        backup_finish_date,
        backup_size,
        backup_type,
        CASE 
            WHEN backup_type = 'D' THEN 'Full'
            WHEN backup_type = 'I' THEN 'Differential'
            WHEN backup_type = 'L' THEN 'Log'
        END AS BackupType,
        DATEDIFF(MINUTE, backup_start_date, backup_finish_date) AS DurationMinutes,
        CASE 
            WHEN has_backup_checksums = 1 THEN 'Yes'
            ELSE 'No'
        END AS HasChecksums
    FROM msdb.dbo.backupset
    WHERE database_name = DB_NAME()
    ORDER BY backup_start_date DESC;
    
    -- Check for recent backups
    DECLARE @LastFullBackup DATETIME2;
    DECLARE @LastDiffBackup DATETIME2;
    DECLARE @LastLogBackup DATETIME2;
    
    SELECT @LastFullBackup = MAX(backup_start_date)
    FROM msdb.dbo.backupset
    WHERE database_name = DB_NAME() AND backup_type = 'D';
    
    SELECT @LastDiffBackup = MAX(backup_start_date)
    FROM msdb.dbo.backupset
    WHERE database_name = DB_NAME() AND backup_type = 'I';
    
    SELECT @LastLogBackup = MAX(backup_start_date)
    FROM msdb.dbo.backupset
    WHERE database_name = DB_NAME() AND backup_type = 'L';
    
    -- Backup age analysis
    SELECT 
        'Backup Age Analysis' AS CheckType,
        'Full Backup' AS BackupType,
        @LastFullBackup AS LastBackup,
        DATEDIFF(HOUR, @LastFullBackup, GETDATE()) AS HoursAgo,
        CASE 
            WHEN DATEDIFF(HOUR, @LastFullBackup, GETDATE()) > 24 THEN 'OVERDUE'
            WHEN DATEDIFF(HOUR, @LastFullBackup, GETDATE()) > 12 THEN 'WARNING'
            ELSE 'OK'
        END AS Status
    UNION ALL
    SELECT 
        'Backup Age Analysis',
        'Differential Backup',
        @LastDiffBackup,
        DATEDIFF(HOUR, @LastDiffBackup, GETDATE()),
        CASE 
            WHEN DATEDIFF(HOUR, @LastDiffBackup, GETDATE()) > 6 THEN 'OVERDUE'
            WHEN DATEDIFF(HOUR, @LastDiffBackup, GETDATE()) > 3 THEN 'WARNING'
            ELSE 'OK'
        END
    UNION ALL
    SELECT 
        'Backup Age Analysis',
        'Log Backup',
        @LastLogBackup,
        DATEDIFF(MINUTE, @LastLogBackup, GETDATE()),
        CASE 
            WHEN DATEDIFF(MINUTE, @LastLogBackup, GETDATE()) > 60 THEN 'OVERDUE'
            WHEN DATEDIFF(MINUTE, @LastLogBackup, GETDATE()) > 30 THEN 'WARNING'
            ELSE 'OK'
        END;
END;

-- Execute backup verification
EXEC sp_VerifyBackups;
```

### Recovery Testing
```sql
-- Example 3: Recovery testing and validation
CREATE PROCEDURE sp_TestRecovery
    @BackupPath NVARCHAR(500),
    @TestDatabaseName NVARCHAR(128) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    
    IF @TestDatabaseName IS NULL
        SET @TestDatabaseName = 'TestRecovery_' + FORMAT(GETDATE(), 'yyyyMMdd_HHmmss');
    
    DECLARE @SQL NVARCHAR(MAX);
    DECLARE @StartTime DATETIME2 = GETDATE();
    
    BEGIN TRY
        -- Restore database for testing
        SET @SQL = 'RESTORE DATABASE [' + @TestDatabaseName + '] FROM DISK = ''' + @BackupPath + ''' WITH REPLACE, MOVE ''YourDatabase'' TO ''C:\Data\' + @TestDatabaseName + '.mdf'', MOVE ''YourDatabase_Log'' TO ''C:\Data\' + @TestDatabaseName + '_Log.ldf''';
        EXEC sp_executesql @SQL;
        
        -- Test database connectivity
        SET @SQL = 'SELECT COUNT(*) AS TableCount FROM [' + @TestDatabaseName + '].sys.tables';
        EXEC sp_executesql @SQL;
        
        -- Test data integrity
        SET @SQL = 'SELECT COUNT(*) AS RecordCount FROM [' + @TestDatabaseName + '].dbo.Orders';
        EXEC sp_executesql @SQL;
        
        -- Test stored procedures
        SET @SQL = 'SELECT COUNT(*) AS ProcedureCount FROM [' + @TestDatabaseName + '].sys.procedures';
        EXEC sp_executesql @SQL;
        
        -- Clean up test database
        SET @SQL = 'DROP DATABASE [' + @TestDatabaseName + ']';
        EXEC sp_executesql @SQL;
        
        DECLARE @EndTime DATETIME2 = GETDATE();
        SELECT 
            'Recovery test completed successfully' AS Status,
            DATEDIFF(SECOND, @StartTime, @EndTime) AS DurationSeconds,
            @TestDatabaseName AS TestDatabase;
        
    END TRY
    BEGIN CATCH
        -- Clean up on error
        IF EXISTS (SELECT 1 FROM sys.databases WHERE name = @TestDatabaseName)
        BEGIN
            SET @SQL = 'DROP DATABASE [' + @TestDatabaseName + ']';
            EXEC sp_executesql @SQL;
        END;
        
        SELECT 
            'Recovery test failed: ' + ERROR_MESSAGE() AS Status,
            @TestDatabaseName AS TestDatabase;
    END CATCH;
END;

-- Execute recovery test
EXEC sp_TestRecovery @BackupPath = 'C:\Backups\YourDatabase_Full.bak';
```

## 120. How do you implement database maintenance and optimization in T-SQL?

### Automated Maintenance
```sql
-- Example 1: Automated database maintenance
CREATE PROCEDURE sp_AutomatedMaintenance
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @StartTime DATETIME2 = GETDATE();
    DECLARE @Results TABLE (
        TaskName NVARCHAR(100),
        Status NVARCHAR(20),
        Duration INT,
        Details NVARCHAR(MAX)
    );
    
    -- Task 1: Update statistics
    BEGIN TRY
        DECLARE @StatsStart DATETIME2 = GETDATE();
        EXEC sp_updatestats;
        INSERT INTO @Results VALUES ('Update Statistics', 'SUCCESS', DATEDIFF(SECOND, @StatsStart, GETDATE()), 'All statistics updated');
    END TRY
    BEGIN CATCH
        INSERT INTO @Results VALUES ('Update Statistics', 'FAILED', 0, ERROR_MESSAGE());
    END CATCH;
    
    -- Task 2: Rebuild fragmented indexes
    BEGIN TRY
        DECLARE @IndexStart DATETIME2 = GETDATE();
        DECLARE @SQL NVARCHAR(MAX);
        
        SET @SQL = 'ALTER INDEX ALL ON Orders REBUILD';
        EXEC sp_executesql @SQL;
        
        INSERT INTO @Results VALUES ('Rebuild Indexes', 'SUCCESS', DATEDIFF(SECOND, @IndexStart, GETDATE()), 'All indexes rebuilt');
    END TRY
    BEGIN CATCH
        INSERT INTO @Results VALUES ('Rebuild Indexes', 'FAILED', 0, ERROR_MESSAGE());
    END CATCH;
    
    -- Task 3: Clean up old data
    BEGIN TRY
        DECLARE @CleanupStart DATETIME2 = GETDATE();
        DECLARE @DeletedCount INT;
        
        DELETE FROM SystemLogs
        WHERE LogDate < DATEADD(MONTH, -3, GETDATE());
        
        SET @DeletedCount = @@ROWCOUNT;
        INSERT INTO @Results VALUES ('Data Cleanup', 'SUCCESS', DATEDIFF(SECOND, @CleanupStart, GETDATE()), 'Deleted ' + CAST(@DeletedCount AS VARCHAR(10)) + ' old records');
    END TRY
    BEGIN CATCH
        INSERT INTO @Results VALUES ('Data Cleanup', 'FAILED', 0, ERROR_MESSAGE());
    END CATCH;
    
    -- Return results
    SELECT 
        TaskName,
        Status,
        Duration,
        Details,
        DATEDIFF(SECOND, @StartTime, GETDATE()) AS TotalDuration
    FROM @Results;
END;

-- Execute automated maintenance
EXEC sp_AutomatedMaintenance;
```

### Index Maintenance
```sql
-- Example 2: Index maintenance and optimization
CREATE PROCEDURE sp_MaintainIndexes
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @Results TABLE (
        TableName NVARCHAR(128),
        IndexName NVARCHAR(128),
        FragmentationPercent DECIMAL(5,2),
        ActionTaken NVARCHAR(50),
        Duration INT
    );
    
    -- Analyze and maintain indexes
    DECLARE @TableName NVARCHAR(128);
    DECLARE @IndexName NVARCHAR(128);
    DECLARE @FragmentationPercent DECIMAL(5,2);
    DECLARE @Action NVARCHAR(50);
    DECLARE @SQL NVARCHAR(MAX);
    DECLARE @StartTime DATETIME2;
    
    DECLARE index_cursor CURSOR FOR
    SELECT 
        OBJECT_NAME(ips.object_id) AS TableName,
        i.name AS IndexName,
        ips.avg_fragmentation_in_percent AS FragmentationPercent
    FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'LIMITED') ips
    INNER JOIN sys.indexes i ON ips.object_id = i.object_id AND ips.index_id = i.index_id
    WHERE ips.avg_fragmentation_in_percent > 10
        AND i.name IS NOT NULL;
    
    OPEN index_cursor;
    FETCH NEXT FROM index_cursor INTO @TableName, @IndexName, @FragmentationPercent;
    
    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @StartTime = GETDATE();
        
        IF @FragmentationPercent > 30
        BEGIN
            SET @Action = 'REBUILD';
            SET @SQL = 'ALTER INDEX [' + @IndexName + '] ON [' + @TableName + '] REBUILD';
        END
        ELSE IF @FragmentationPercent > 10
        BEGIN
            SET @Action = 'REORGANIZE';
            SET @SQL = 'ALTER INDEX [' + @IndexName + '] ON [' + @TableName + '] REORGANIZE';
        END;
        
        BEGIN TRY
            EXEC sp_executesql @SQL;
            INSERT INTO @Results VALUES (@TableName, @IndexName, @FragmentationPercent, @Action, DATEDIFF(SECOND, @StartTime, GETDATE()));
        END TRY
        BEGIN CATCH
            INSERT INTO @Results VALUES (@TableName, @IndexName, @FragmentationPercent, 'FAILED: ' + ERROR_MESSAGE(), 0);
        END CATCH;
        
        FETCH NEXT FROM index_cursor INTO @TableName, @IndexName, @FragmentationPercent;
    END;
    
    CLOSE index_cursor;
    DEALLOCATE index_cursor;
    
    -- Return results
    SELECT * FROM @Results ORDER BY FragmentationPercent DESC;
END;

-- Execute index maintenance
EXEC sp_MaintainIndexes;
```

### Database Optimization
```sql
-- Example 3: Database optimization and tuning
CREATE PROCEDURE sp_OptimizeDatabase
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Analyze database size and growth
    SELECT 
        'Database Size Analysis' AS AnalysisType,
        name AS DatabaseName,
        size * 8 / 1024 AS SizeMB,
        max_size * 8 / 1024 AS MaxSizeMB,
        growth AS GrowthMB,
        is_percent_growth AS IsPercentGrowth
    FROM sys.database_files
    ORDER BY size DESC;
    
    -- Analyze table sizes
    SELECT 
        'Table Size Analysis' AS AnalysisType,
        t.name AS TableName,
        p.rows AS RowCount,
        SUM(a.total_pages) * 8 / 1024 AS TotalSizeMB,
        SUM(a.used_pages) * 8 / 1024 AS UsedSizeMB,
        SUM(a.data_pages) * 8 / 1024 AS DataSizeMB
    FROM sys.tables t
    INNER JOIN sys.indexes i ON t.object_id = i.object_id
    INNER JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
    INNER JOIN sys.allocation_units a ON p.partition_id = a.container_id
    GROUP BY t.name, p.rows
    ORDER BY TotalSizeMB DESC;
    
    -- Analyze index usage
    SELECT 
        'Index Usage Analysis' AS AnalysisType,
        OBJECT_NAME(i.object_id) AS TableName,
        i.name AS IndexName,
        i.type_desc AS IndexType,
        s.user_seeks,
        s.user_scans,
        s.user_lookups,
        s.user_updates,
        s.last_user_seek,
        s.last_user_scan,
        s.last_user_lookup
    FROM sys.indexes i
    LEFT JOIN sys.dm_db_index_usage_stats s ON i.object_id = s.object_id AND i.index_id = s.index_id
    WHERE i.name IS NOT NULL
    ORDER BY s.user_seeks + s.user_scans + s.user_lookups DESC;
    
    -- Optimization recommendations
    SELECT 
        'Optimization Recommendations' AS AnalysisType,
        'Consider removing unused indexes' AS Recommendation,
        COUNT(*) AS UnusedIndexCount
    FROM sys.indexes i
    LEFT JOIN sys.dm_db_index_usage_stats s ON i.object_id = s.object_id AND i.index_id = s.index_id
    WHERE i.name IS NOT NULL
        AND (s.user_seeks + s.user_scans + s.user_lookups) = 0
        AND i.type_desc != 'HEAP'
    
    UNION ALL
    
    SELECT 
        'Optimization Recommendations',
        'Consider adding missing indexes',
        COUNT(*)
    FROM sys.dm_db_missing_index_groups mig
    INNER JOIN sys.dm_db_missing_index_group_stats migs ON migs.group_handle = mig.index_group_handle
    WHERE migs.avg_total_user_cost * (migs.avg_user_impact / 100.0) * (migs.user_seeks + migs.user_scans) > 10;
END;

-- Execute database optimization
EXEC sp_OptimizeDatabase;
```

## 121. How do you implement real-time data processing in T-SQL?

### Change Data Capture
```sql
-- Example 1: Change Data Capture implementation
-- Enable CDC on database
EXEC sys.sp_cdc_enable_db;

-- Enable CDC on table
EXEC sys.sp_cdc_enable_table
    @source_schema = N'dbo',
    @source_name = N'Orders',
    @role_name = NULL;

-- Query CDC changes
SELECT 
    __$start_lsn,
    __$operation,
    __$update_mask,
    OrderID,
    CustomerID,
    TotalAmount,
    Status
FROM cdc.dbo_Orders_CT
WHERE __$start_lsn >= sys.fn_cdc_get_min_lsn('dbo_Orders')
ORDER BY __$start_lsn;
```

**Result:**
```
__$start_lsn | __$operation | OrderID | CustomerID | TotalAmount | Status
-------------|-------------|---------|------------|-------------|----------
0x00000001   | 2           | 101     | 1          | 150.00      | Completed
0x00000002   | 4           | 102     | 2          | 200.00      | Pending
```

### Temporal Tables
```sql
-- Example 2: Temporal tables for historical tracking
CREATE TABLE Products (
    ProductID INT IDENTITY(1,1) PRIMARY KEY,
    ProductName NVARCHAR(100) NOT NULL,
    UnitPrice DECIMAL(10,2) NOT NULL,
    StockQuantity INT NOT NULL,
    ValidFrom DATETIME2 GENERATED ALWAYS AS ROW START,
    ValidTo DATETIME2 GENERATED ALWAYS AS ROW END,
    PERIOD FOR SYSTEM_TIME (ValidFrom, ValidTo)
)
WITH (SYSTEM_VERSIONING = ON (HISTORY_TABLE = dbo.ProductsHistory));

-- Insert data
INSERT INTO Products (ProductName, UnitPrice, StockQuantity)
VALUES ('Laptop', 999.99, 50);

-- Update data
UPDATE Products 
SET UnitPrice = 899.99 
WHERE ProductID = 1;

-- Query historical data
SELECT 
    ProductID,
    ProductName,
    UnitPrice,
    ValidFrom,
    ValidTo
FROM Products 
FOR SYSTEM_TIME AS OF '2024-01-15 10:00:00';
```

### Real-time Monitoring
```sql
-- Example 3: Real-time data monitoring
CREATE PROCEDURE sp_RealTimeMonitoring
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Monitor real-time changes
    SELECT 
        'Real-time Changes' AS MonitorType,
        __$operation AS OperationType,
        COUNT(*) AS ChangeCount,
        MAX(__$start_lsn) AS LastLSN
    FROM cdc.dbo_Orders_CT
    WHERE __$start_lsn >= sys.fn_cdc_get_min_lsn('dbo_Orders')
    GROUP BY __$operation;
    
    -- Check data freshness
    SELECT 
        'Data Freshness' AS MonitorType,
        DATEDIFF(SECOND, MAX(CreatedDate), GETDATE()) AS SecondsSinceLastUpdate
    FROM Orders;
END;

-- Execute real-time monitoring
EXEC sp_RealTimeMonitoring;
```

## 122. How do you implement advanced data analysis techniques in T-SQL?

### Time Series Analysis
```sql
-- Example 1: Time series analysis
CREATE PROCEDURE sp_TimeSeriesAnalysis
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Calculate moving averages
    SELECT 
        OrderDate,
        TotalAmount,
        AVG(TotalAmount) OVER (
            ORDER BY OrderDate 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS MovingAvg7Days,
        AVG(TotalAmount) OVER (
            ORDER BY OrderDate 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS MovingAvg30Days,
        LAG(TotalAmount, 1) OVER (ORDER BY OrderDate) AS PreviousDayAmount,
        TotalAmount - LAG(TotalAmount, 1) OVER (ORDER BY OrderDate) AS DayOverDayChange
    FROM (
        SELECT 
            CAST(OrderDate AS DATE) AS OrderDate,
            SUM(TotalAmount) AS TotalAmount
        FROM Orders
        GROUP BY CAST(OrderDate AS DATE)
    ) daily_sales
    ORDER BY OrderDate;
END;

-- Execute time series analysis
EXEC sp_TimeSeriesAnalysis;
```

### Customer Segmentation
```sql
-- Example 2: Advanced customer segmentation
WITH CustomerMetrics AS (
    SELECT 
        c.CustomerID,
        c.CustomerName,
        COUNT(o.OrderID) AS TotalOrders,
        SUM(o.TotalAmount) AS TotalSpent,
        AVG(o.TotalAmount) AS AvgOrderValue,
        DATEDIFF(DAY, MAX(o.OrderDate), GETDATE()) AS DaysSinceLastOrder,
        DATEDIFF(DAY, MIN(o.OrderDate), MAX(o.OrderDate)) AS CustomerLifetimeDays
    FROM Customers c
    LEFT JOIN Orders o ON c.CustomerID = o.CustomerID
    GROUP BY c.CustomerID, c.CustomerName
),
SegmentedCustomers AS (
    SELECT 
        *,
        CASE 
            WHEN TotalSpent > 1000 AND DaysSinceLastOrder <= 30 THEN 'High Value Active'
            WHEN TotalSpent > 1000 AND DaysSinceLastOrder > 30 THEN 'High Value Inactive'
            WHEN TotalSpent BETWEEN 500 AND 1000 AND DaysSinceLastOrder <= 60 THEN 'Medium Value Active'
            WHEN TotalSpent BETWEEN 500 AND 1000 AND DaysSinceLastOrder > 60 THEN 'Medium Value Inactive'
            WHEN TotalSpent < 500 AND DaysSinceLastOrder <= 90 THEN 'Low Value Active'
            ELSE 'Low Value Inactive'
        END AS CustomerSegment
    FROM CustomerMetrics
)
SELECT 
    CustomerSegment,
    COUNT(*) AS CustomerCount,
    AVG(TotalSpent) AS AvgTotalSpent,
    AVG(AvgOrderValue) AS AvgOrderValue,
    AVG(DaysSinceLastOrder) AS AvgDaysSinceLastOrder
FROM SegmentedCustomers
GROUP BY CustomerSegment
ORDER BY AvgTotalSpent DESC;
```

### Predictive Analytics
```sql
-- Example 3: Predictive analytics implementation
CREATE PROCEDURE sp_PredictiveAnalytics
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Predict customer lifetime value
    WITH CustomerPrediction AS (
        SELECT 
            CustomerID,
            CustomerName,
            TotalOrders,
            TotalSpent,
            AvgOrderValue,
            CustomerLifetimeDays,
            -- Simple linear prediction based on historical data
            CASE 
                WHEN CustomerLifetimeDays > 0 
                THEN (TotalSpent / CustomerLifetimeDays) * 365 * 2 -- Project 2 years
                ELSE TotalSpent 
            END AS PredictedLifetimeValue,
            -- Churn risk calculation
            CASE 
                WHEN DaysSinceLastOrder > 180 THEN 'High Risk'
                WHEN DaysSinceLastOrder > 90 THEN 'Medium Risk'
                ELSE 'Low Risk'
            END AS ChurnRisk
        FROM (
            SELECT 
                c.CustomerID,
                c.CustomerName,
                COUNT(o.OrderID) AS TotalOrders,
                SUM(o.TotalAmount) AS TotalSpent,
                AVG(o.TotalAmount) AS AvgOrderValue,
                DATEDIFF(DAY, MAX(o.OrderDate), GETDATE()) AS DaysSinceLastOrder,
                DATEDIFF(DAY, MIN(o.OrderDate), MAX(o.OrderDate)) AS CustomerLifetimeDays
            FROM Customers c
            LEFT JOIN Orders o ON c.CustomerID = o.CustomerID
            GROUP BY c.CustomerID, c.CustomerName
        ) base
    )
    SELECT 
        CustomerID,
        CustomerName,
        TotalSpent,
        PredictedLifetimeValue,
        ChurnRisk,
        PredictedLifetimeValue - TotalSpent AS PotentialValue
    FROM CustomerPrediction
    WHERE PredictedLifetimeValue > TotalSpent
    ORDER BY PotentialValue DESC;
END;

-- Execute predictive analytics
EXEC sp_PredictiveAnalytics;
```

## 123. How do you implement data compression and storage optimization in T-SQL?

### Data Compression Implementation
```sql
-- Example 1: Data compression implementation
-- Create table with row compression
CREATE TABLE CompressedOrders (
    OrderID INT IDENTITY(1,1) PRIMARY KEY,
    CustomerID INT NOT NULL,
    OrderDate DATETIME2 NOT NULL,
    TotalAmount DECIMAL(10,2) NOT NULL,
    Status NVARCHAR(20) NOT NULL
) WITH (DATA_COMPRESSION = ROW);

-- Create table with page compression
CREATE TABLE PageCompressedProducts (
    ProductID INT IDENTITY(1,1) PRIMARY KEY,
    ProductName NVARCHAR(100) NOT NULL,
    Description NVARCHAR(MAX),
    UnitPrice DECIMAL(10,2) NOT NULL,
    CategoryID INT NOT NULL
) WITH (DATA_COMPRESSION = PAGE);

-- Check compression effectiveness
SELECT 
    OBJECT_NAME(object_id) AS TableName,
    data_compression_desc AS CompressionType,
    page_count,
    compressed_page_count,
    CASE 
        WHEN page_count > 0 
        THEN CAST((page_count - compressed_page_count) * 100.0 / page_count AS DECIMAL(5,2))
        ELSE 0 
    END AS CompressionRatio
FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'DETAILED')
WHERE data_compression_desc != 'NONE';
```

**Result:**
```
TableName | CompressionType | page_count | compressed_page_count | CompressionRatio
----------|-----------------|------------|----------------------|-----------------
CompressedOrders | ROW | 1 | 1 | 0.00
PageCompressedProducts | PAGE | 1 | 1 | 0.00
```

### Columnstore Compression
```sql
-- Example 2: Columnstore compression
-- Create table with clustered columnstore index
CREATE TABLE FactSales (
    SaleID BIGINT IDENTITY(1,1),
    DateKey INT NOT NULL,
    ProductKey INT NOT NULL,
    CustomerKey INT NOT NULL,
    SalesAmount DECIMAL(10,2) NOT NULL,
    Quantity INT NOT NULL,
    INDEX CCI_FactSales CLUSTERED COLUMNSTORE
);

-- Create nonclustered columnstore index
CREATE NONCLUSTERED COLUMNSTORE INDEX NCCI_Orders_Analytics
ON Orders (CustomerID, OrderDate, TotalAmount, Status);

-- Analyze columnstore compression
SELECT 
    OBJECT_NAME(p.object_id) AS TableName,
    i.name AS IndexName,
    i.type_desc AS IndexType,
    p.partition_number,
    p.rows AS RowCount,
    p.data_compression_desc AS CompressionType,
    p.reserved_page_count * 8 / 1024 AS ReservedSpaceMB
FROM sys.partitions p
INNER JOIN sys.indexes i ON p.object_id = i.object_id AND p.index_id = i.index_id
WHERE i.type IN (5, 6) -- Columnstore indexes
ORDER BY ReservedSpaceMB DESC;
```

### Storage Optimization
```sql
-- Example 3: Storage optimization strategies
CREATE PROCEDURE sp_OptimizeStorage
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Analyze table sizes and compression opportunities
    SELECT 
        t.name AS TableName,
        p.rows AS RowCount,
        SUM(a.total_pages) * 8 / 1024 AS TotalSizeMB,
        SUM(a.used_pages) * 8 / 1024 AS UsedSizeMB,
        SUM(a.data_pages) * 8 / 1024 AS DataSizeMB,
        CASE 
            WHEN SUM(a.total_pages) > 1000 AND ps.data_compression_desc = 'NONE'
            THEN 'Consider compression'
            WHEN SUM(a.total_pages) > 10000 
            THEN 'Consider columnstore'
            ELSE 'Optimal'
        END AS Recommendation
    FROM sys.tables t
    INNER JOIN sys.indexes i ON t.object_id = i.object_id
    INNER JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
    INNER JOIN sys.allocation_units a ON p.partition_id = a.container_id
    LEFT JOIN sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'LIMITED') ps 
        ON i.object_id = ps.object_id AND i.index_id = ps.index_id
    WHERE i.index_id = 1 -- Clustered index or heap
    GROUP BY t.name, p.rows, ps.data_compression_desc
    ORDER BY TotalSizeMB DESC;
    
    -- Identify unused space
    SELECT 
        'Storage Analysis' AS AnalysisType,
        SUM(total_pages) * 8 / 1024 AS TotalAllocatedMB,
        SUM(used_pages) * 8 / 1024 AS TotalUsedMB,
        (SUM(total_pages) - SUM(used_pages)) * 8 / 1024 AS UnusedSpaceMB,
        CAST((SUM(total_pages) - SUM(used_pages)) * 100.0 / SUM(total_pages) AS DECIMAL(5,2)) AS UnusedSpacePercent
    FROM sys.allocation_units;
END;

-- Execute storage optimization analysis
EXEC sp_OptimizeStorage;
```

## 124. How do you implement high-availability and disaster recovery in T-SQL?

### Always On Availability Groups
```sql
-- Example 1: Always On Availability Groups monitoring
CREATE PROCEDURE sp_MonitorAvailabilityGroups
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Check availability group status
    SELECT 
        ag.name AS AvailabilityGroupName,
        ag.primary_replica,
        ag.failover_mode_desc,
        ag.availability_mode_desc,
        ar.replica_server_name,
        ar.availability_mode_desc AS ReplicaMode,
        ar.failover_mode_desc AS ReplicaFailoverMode,
        ar.primary_role_allow_connections_desc,
        ar.secondary_role_allow_connections_desc
    FROM sys.availability_groups ag
    INNER JOIN sys.availability_replicas ar ON ag.group_id = ar.group_id
    ORDER BY ag.name, ar.replica_server_name;
    
    -- Check database replica states
    SELECT 
        dr.database_name,
        dr.replica_id,
        ar.replica_server_name,
        dr.synchronization_state_desc,
        dr.synchronization_health_desc,
        dr.is_suspended,
        dr.suspend_reason_desc,
        dr.log_send_queue_size,
        dr.log_send_rate,
        dr.redo_queue_size,
        dr.redo_rate
    FROM sys.dm_hadr_database_replica_states dr
    INNER JOIN sys.availability_replicas ar ON dr.replica_id = ar.replica_id
    ORDER BY dr.database_name, ar.replica_server_name;
END;

-- Execute availability group monitoring
EXEC sp_MonitorAvailabilityGroups;
```

### Automated Failover Testing
```sql
-- Example 2: Automated failover testing
CREATE PROCEDURE sp_TestFailover
    @AvailabilityGroupName NVARCHAR(128),
    @TargetReplica NVARCHAR(128)
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @SQL NVARCHAR(MAX);
    DECLARE @StartTime DATETIME2 = GETDATE();
    
    BEGIN TRY
        -- Check current primary replica
        SELECT 
            'Pre-Failover Status' AS TestPhase,
            ag.name AS AvailabilityGroupName,
            ag.primary_replica AS CurrentPrimary,
            @TargetReplica AS TargetPrimary
        FROM sys.availability_groups ag
        WHERE ag.name = @AvailabilityGroupName;
        
        -- Perform failover (commented for safety)
        -- SET @SQL = 'ALTER AVAILABILITY GROUP [' + @AvailabilityGroupName + '] FAILOVER';
        -- EXEC sp_executesql @SQL;
        
        -- Simulate failover validation
        WAITFOR DELAY '00:00:05'; -- Simulate failover time
        
        -- Check post-failover status
        SELECT 
            'Post-Failover Status' AS TestPhase,
            ag.name AS AvailabilityGroupName,
            ag.primary_replica AS NewPrimary,
            DATEDIFF(SECOND, @StartTime, GETDATE()) AS FailoverTimeSeconds,
            CASE 
                WHEN ag.primary_replica = @TargetReplica THEN 'SUCCESS'
                ELSE 'FAILED'
            END AS FailoverResult
        FROM sys.availability_groups ag
        WHERE ag.name = @AvailabilityGroupName;
        
    END TRY
    BEGIN CATCH
        SELECT 
            'Failover Error' AS TestPhase,
            ERROR_MESSAGE() AS ErrorMessage,
            ERROR_NUMBER() AS ErrorNumber;
    END CATCH;
END;

-- Execute failover test (commented for safety)
-- EXEC sp_TestFailover @AvailabilityGroupName = 'AG_Production', @TargetReplica = 'Server2';
```

### Disaster Recovery Planning
```sql
-- Example 3: Comprehensive disaster recovery planning
CREATE PROCEDURE sp_DisasterRecoveryPlan
AS
BEGIN
    SET NOCOUNT ON;
    
    -- RTO/RPO Analysis
    SELECT 
        'Recovery Objectives' AS PlanSection,
        'RTO (Recovery Time Objective)' AS Metric,
        '4 hours' AS Target,
        'Maximum acceptable downtime' AS Description
    UNION ALL
    SELECT 
        'Recovery Objectives',
        'RPO (Recovery Point Objective)',
        '15 minutes',
        'Maximum acceptable data loss'
    UNION ALL
    SELECT 
        'Recovery Objectives',
        'MTTR (Mean Time To Recovery)',
        '2 hours',
        'Average time to restore service';
    
    -- Backup verification for DR
    SELECT 
        'Backup Readiness' AS PlanSection,
        bs.database_name,
        bs.backup_type,
        bs.backup_start_date,
        bs.backup_finish_date,
        CASE 
            WHEN bs.backup_type = 'D' AND DATEDIFF(HOUR, bs.backup_start_date, GETDATE()) <= 24 THEN 'READY'
            WHEN bs.backup_type = 'L' AND DATEDIFF(MINUTE, bs.backup_start_date, GETDATE()) <= 30 THEN 'READY'
            ELSE 'OUTDATED'
        END AS DRReadiness
    FROM msdb.dbo.backupset bs
    WHERE bs.backup_start_date >= DATEADD(DAY, -1, GETDATE())
    ORDER BY bs.database_name, bs.backup_start_date DESC;
    
    -- Recovery procedures checklist
    SELECT 
        'Recovery Procedures' AS PlanSection,
        'Step 1: Assess damage' AS RecoveryStep,
        'Determine scope of failure' AS Description,
        '15 minutes' AS EstimatedTime
    UNION ALL
    SELECT 
        'Recovery Procedures',
        'Step 2: Activate DR site',
        'Switch to disaster recovery location',
        '30 minutes'
    UNION ALL
    SELECT 
        'Recovery Procedures',
        'Step 3: Restore databases',
        'Restore from latest backups',
        '2 hours'
    UNION ALL
    SELECT 
        'Recovery Procedures',
        'Step 4: Validate systems',
        'Test all critical functions',
        '1 hour'
    UNION ALL
    SELECT 
        'Recovery Procedures',
        'Step 5: Resume operations',
        'Redirect users to restored systems',
        '30 minutes';
END;

-- Execute disaster recovery plan
EXEC sp_DisasterRecoveryPlan;
```

## 125. How do you implement data partitioning strategies in T-SQL?

### Range Partitioning
```sql
-- Example 1: Range partitioning implementation
-- Create partition function
CREATE PARTITION FUNCTION pf_OrderDate (DATETIME2)
AS RANGE RIGHT FOR VALUES (
    '2023-01-01', 
    '2024-01-01', 
    '2025-01-01'
);

-- Create partition scheme
CREATE PARTITION SCHEME ps_OrderDate
AS PARTITION pf_OrderDate 
TO ([PRIMARY], [PRIMARY], [PRIMARY], [PRIMARY]);

-- Create partitioned table
CREATE TABLE PartitionedOrders (
    OrderID INT IDENTITY(1,1),
    CustomerID INT NOT NULL,
    OrderDate DATETIME2 NOT NULL,
    TotalAmount DECIMAL(10,2) NOT NULL,
    Status NVARCHAR(20) NOT NULL,
    CONSTRAINT PK_PartitionedOrders PRIMARY KEY (OrderID, OrderDate)
) ON ps_OrderDate(OrderDate);

-- Check partition distribution
SELECT 
    p.partition_number,
    p.rows AS RowCount,
    rv.value AS PartitionBoundary,
    fg.name AS FileGroupName
FROM sys.partitions p
INNER JOIN sys.partition_schemes ps ON p.partition_id = ps.data_space_id
INNER JOIN sys.partition_range_values rv ON ps.function_id = rv.function_id 
    AND p.partition_number = rv.boundary_id + 1
INNER JOIN sys.destination_data_spaces dds ON ps.data_space_id = dds.partition_scheme_id 
    AND p.partition_number = dds.destination_id
INNER JOIN sys.filegroups fg ON dds.data_space_id = fg.data_space_id
WHERE p.object_id = OBJECT_ID('PartitionedOrders')
ORDER BY p.partition_number;
```

### Partition Maintenance
```sql
-- Example 2: Partition maintenance operations
CREATE PROCEDURE sp_MaintainPartitions
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Add new partition for next year
    DECLARE @NextYear DATETIME2 = DATEADD(YEAR, 1, GETDATE());
    DECLARE @SQL NVARCHAR(MAX);
    
    -- Split partition to add new range
    SET @SQL = 'ALTER PARTITION FUNCTION pf_OrderDate() SPLIT RANGE (''' + 
               CAST(@NextYear AS NVARCHAR(20)) + ''')';
    
    BEGIN TRY
        EXEC sp_executesql @SQL;
        PRINT 'New partition added for year: ' + CAST(YEAR(@NextYear) AS VARCHAR(4));
    END TRY
    BEGIN CATCH
        PRINT 'Failed to add partition: ' + ERROR_MESSAGE();
    END CATCH;
    
    -- Archive old partitions
    DECLARE @ArchiveDate DATETIME2 = DATEADD(YEAR, -3, GETDATE());
    
    -- Switch out old partition to staging table
    CREATE TABLE OrdersArchiveStaging (
        OrderID INT,
        CustomerID INT NOT NULL,
        OrderDate DATETIME2 NOT NULL,
        TotalAmount DECIMAL(10,2) NOT NULL,
        Status NVARCHAR(20) NOT NULL,
        CONSTRAINT PK_OrdersArchiveStaging PRIMARY KEY (OrderID, OrderDate)
    ) ON [PRIMARY];
    
    -- Switch partition (example for partition 1)
    -- ALTER TABLE PartitionedOrders SWITCH PARTITION 1 TO OrdersArchiveStaging;
    
    -- Merge empty partition
    -- ALTER PARTITION FUNCTION pf_OrderDate() MERGE RANGE ('2023-01-01');
    
    -- Report partition status
    SELECT 
        'Partition Status' AS ReportType,
        OBJECT_NAME(p.object_id) AS TableName,
        p.partition_number,
        p.rows AS RowCount,
        CAST(rv.value AS DATETIME2) AS BoundaryValue,
        p.data_compression_desc AS CompressionType
    FROM sys.partitions p
    LEFT JOIN sys.partition_range_values rv ON p.partition_number = rv.boundary_id + 1
    WHERE p.object_id = OBJECT_ID('PartitionedOrders')
        AND p.index_id = 1
    ORDER BY p.partition_number;
END;

-- Execute partition maintenance
EXEC sp_MaintainPartitions;
```

### Partition Elimination Testing
```sql
-- Example 3: Partition elimination and performance testing
CREATE PROCEDURE sp_TestPartitionElimination
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Test query with partition elimination
    SELECT 
        'Partition Elimination Test' AS TestType,
        COUNT(*) AS RowCount,
        MIN(OrderDate) AS MinDate,
        MAX(OrderDate) AS MaxDate
    FROM PartitionedOrders
    WHERE OrderDate >= '2024-01-01' 
        AND OrderDate < '2025-01-01';
    
    -- Query execution plan analysis
    SET STATISTICS IO ON;
    SET STATISTICS TIME ON;
    
    -- Query that should use partition elimination
    SELECT 
        CustomerID,
        COUNT(*) AS OrderCount,
        SUM(TotalAmount) AS TotalSpent
    FROM PartitionedOrders
    WHERE OrderDate >= '2024-06-01' 
        AND OrderDate <= '2024-12-31'
    GROUP BY CustomerID
    HAVING COUNT(*) > 5
    ORDER BY TotalSpent DESC;
    
    SET STATISTICS IO OFF;
    SET STATISTICS TIME OFF;
    
    -- Compare with non-partitioned query performance
    SELECT 
        'Performance Comparison' AS TestType,
        'Partitioned table allows for partition elimination' AS Benefit,
        'Only relevant partitions are scanned' AS Description
    UNION ALL
    SELECT 
        'Performance Comparison',
        'Parallel processing per partition',
        'Each partition can be processed in parallel'
    UNION ALL
    SELECT 
        'Performance Comparison',
        'Maintenance operations per partition',
        'Index rebuild/reorganize can target specific partitions';
END;

-- Execute partition elimination test
EXEC sp_TestPartitionElimination;
```

## 126. How do you implement performance tuning best practices in T-SQL?

### Query Optimization Best Practices
```sql
-- Example 1: Query optimization best practices
CREATE PROCEDURE sp_QueryOptimizationBestPractices
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Best Practice 1: Use appropriate indexes
    CREATE NONCLUSTERED INDEX IX_Orders_Customer_Date_Covering 
    ON Orders (CustomerID, OrderDate) 
    INCLUDE (TotalAmount, Status);
    
    -- Best Practice 2: Use EXISTS instead of IN for subqueries
    SELECT 
        CustomerID,
        CustomerName,
        Email
    FROM Customers c
    WHERE EXISTS (
        SELECT 1 
        FROM Orders o 
        WHERE o.CustomerID = c.CustomerID 
            AND o.OrderDate >= '2024-01-01'
    );
    
    -- Best Practice 3: Use parameterized queries
    DECLARE @StartDate DATETIME2 = '2024-01-01';
    DECLARE @EndDate DATETIME2 = '2024-12-31';
    
    SELECT 
        CustomerID,
        COUNT(*) AS OrderCount,
        SUM(TotalAmount) AS TotalSpent
    FROM Orders
    WHERE OrderDate >= @StartDate 
        AND OrderDate <= @EndDate
    GROUP BY CustomerID
    HAVING COUNT(*) > 10
    ORDER BY TotalSpent DESC;
    
    -- Best Practice 4: Use window functions efficiently
    SELECT 
        CustomerID,
        OrderDate,
        TotalAmount,
        ROW_NUMBER() OVER (PARTITION BY CustomerID ORDER BY OrderDate DESC) AS RowNum,
        SUM(TotalAmount) OVER (PARTITION BY CustomerID) AS CustomerTotal,
        AVG(TotalAmount) OVER (PARTITION BY CustomerID) AS CustomerAverage
    FROM Orders
    WHERE OrderDate >= @StartDate;
END;

-- Execute query optimization examples
EXEC sp_QueryOptimizationBestPractices;
```

### Index Optimization Best Practices
```sql
-- Example 2: Index optimization best practices
CREATE PROCEDURE sp_IndexOptimizationBestPractices
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Best Practice 1: Monitor index usage
    SELECT 
        OBJECT_NAME(i.object_id) AS TableName,
        i.name AS IndexName,
        i.type_desc AS IndexType,
        s.user_seeks,
        s.user_scans,
        s.user_lookups,
        s.user_updates,
        s.last_user_seek,
        s.last_user_scan,
        s.last_user_lookup,
        CASE 
            WHEN s.user_seeks + s.user_scans + s.user_lookups = 0 THEN 'UNUSED'
            WHEN s.user_updates > (s.user_seeks + s.user_scans + s.user_lookups) * 2 THEN 'OVER_MAINTAINED'
            ELSE 'WELL_USED'
        END AS IndexUsageStatus
    FROM sys.indexes i
    LEFT JOIN sys.dm_db_index_usage_stats s ON i.object_id = s.object_id 
        AND i.index_id = s.index_id
    WHERE i.name IS NOT NULL
        AND i.object_id = OBJECT_ID('Orders')
    ORDER BY s.user_seeks + s.user_scans + s.user_lookups DESC;
    
    -- Best Practice 2: Identify missing indexes
    SELECT 
        migs.avg_total_user_cost * (migs.avg_user_impact / 100.0) * (migs.user_seeks + migs.user_scans) AS improvement_measure,
        'CREATE INDEX IX_' + OBJECT_NAME(mid.object_id) + '_' + 
        REPLACE(REPLACE(REPLACE(ISNULL(mid.equality_columns, ''), ', ', '_'), '[', ''), ']', '') + 
        CASE WHEN mid.inequality_columns IS NOT NULL THEN '_' + 
             REPLACE(REPLACE(REPLACE(mid.inequality_columns, ', ', '_'), '[', ''), ']', '') 
             ELSE '' END +
        ' ON ' + mid.statement + ' (' + ISNULL(mid.equality_columns, '') + 
        CASE WHEN mid.equality_columns IS NOT NULL AND mid.inequality_columns IS NOT NULL 
             THEN ',' ELSE '' END + ISNULL(mid.inequality_columns, '') + ')' +
        CASE WHEN mid.included_columns IS NOT NULL 
             THEN ' INCLUDE (' + mid.included_columns + ')' ELSE '' END AS create_index_statement,
        migs.user_seeks,
        migs.user_scans,
        migs.avg_user_impact
    FROM sys.dm_db_missing_index_groups mig
    INNER JOIN sys.dm_db_missing_index_group_stats migs ON migs.group_handle = mig.index_group_handle
    INNER JOIN sys.dm_db_missing_index_details mid ON mig.index_handle = mid.index_handle
    WHERE migs.avg_total_user_cost * (migs.avg_user_impact / 100.0) * (migs.user_seeks + migs.user_scans) > 10
    ORDER BY improvement_measure DESC;
    
    -- Best Practice 3: Analyze index fragmentation
    SELECT 
        OBJECT_NAME(ips.object_id) AS TableName,
        i.name AS IndexName,
        ips.avg_fragmentation_in_percent,
        ips.page_count,
        CASE 
            WHEN ips.avg_fragmentation_in_percent > 30 THEN 'REBUILD'
            WHEN ips.avg_fragmentation_in_percent > 10 THEN 'REORGANIZE'
            ELSE 'NO_ACTION'
        END AS RecommendedAction
    FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'LIMITED') ips
    INNER JOIN sys.indexes i ON ips.object_id = i.object_id AND ips.index_id = i.index_id
    WHERE ips.avg_fragmentation_in_percent > 5
        AND i.name IS NOT NULL
    ORDER BY ips.avg_fragmentation_in_percent DESC;
END;

-- Execute index optimization analysis
EXEC sp_IndexOptimizationBestPractices;
```

### Performance Monitoring Best Practices
```sql
-- Example 3: Performance monitoring best practices
CREATE PROCEDURE sp_PerformanceMonitoringBestPractices
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Best Practice 1: Monitor wait statistics
    SELECT TOP 10
        wait_type,
        waiting_tasks_count,
        wait_time_ms,
        max_wait_time_ms,
        signal_wait_time_ms,
        wait_time_ms - signal_wait_time_ms AS resource_wait_time_ms,
        CAST(100.0 * wait_time_ms / SUM(wait_time_ms) OVER() AS DECIMAL(5,2)) AS percentage
    FROM sys.dm_os_wait_stats
    WHERE wait_type NOT IN (
        'CLR_SEMAPHORE', 'LAZYWRITER_SLEEP', 'RESOURCE_QUEUE', 'SLEEP_TASK',
        'SLEEP_SYSTEMTASK', 'SQLTRACE_BUFFER_FLUSH', 'WAITFOR', 'LOGMGR_QUEUE',
        'CHECKPOINT_QUEUE', 'REQUEST_FOR_DEADLOCK_SEARCH', 'XE_TIMER_EVENT',
        'BROKER_TO_FLUSH', 'BROKER_TASK_STOP', 'CLR_MANUAL_EVENT'
    )
    ORDER BY wait_time_ms DESC;
    
    -- Best Practice 2: Monitor resource consumption
    SELECT 
        'Resource Consumption' AS MonitorType,
        'Memory Usage' AS Resource,
        CAST((total_physical_memory_kb - available_physical_memory_kb) * 100.0 / total_physical_memory_kb AS DECIMAL(5,2)) AS UsagePercent,
        'Keep below 80%' AS Recommendation
    FROM sys.dm_os_sys_info
    
    UNION ALL
    
    SELECT 
        'Resource Consumption',
        'CPU Usage',
        CAST(AVG(cpu_percent) AS DECIMAL(5,2)),
        'Monitor for sustained high CPU'
    FROM sys.dm_os_ring_buffers
    WHERE ring_buffer_type = 'RING_BUFFER_SCHEDULER_MONITOR'
    
    UNION ALL
    
    SELECT 
        'Resource Consumption',
        'Active Sessions',
        CAST(COUNT(*) AS DECIMAL(5,2)),
        'Monitor for unusual spikes'
    FROM sys.dm_exec_sessions
    WHERE is_user_process = 1;
    
    -- Best Practice 3: Query performance baseline
    SELECT 
        'Performance Baseline' AS MonitorType,
        'Avg Query Duration' AS Metric,
        CAST(AVG(total_elapsed_time / execution_count) AS DECIMAL(15,2)) AS AverageValue,
        'Microseconds' AS Unit,
        'Establish baseline for comparison' AS Note
    FROM sys.dm_exec_query_stats
    WHERE execution_count > 10
    
    UNION ALL
    
    SELECT 
        'Performance Baseline',
        'Avg Logical Reads',
        CAST(AVG(total_logical_reads / execution_count) AS DECIMAL(15,2)),
        'Pages',
        'Monitor for query plan changes'
    FROM sys.dm_exec_query_stats
    WHERE execution_count > 10;
END;

-- Execute performance monitoring
EXEC sp_PerformanceMonitoringBestPractices;
```
