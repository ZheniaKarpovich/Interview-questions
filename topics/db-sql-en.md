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
  v_total NUMBER;
  BEGIN
    SELECT COUNT(*) INTO v_total FROM employees;
    DBMS_OUTPUT.PUT_LINE('Total employees: ' || v_total);
  EXCEPTION
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

## 31. Что такое взаимная блокировка (deadlock)?

Deadlock возникает, когда два процесса ожидают друг друга, удерживая при этом блокировки, необходимые для продолжения работы.  
Например:
- Процесс 1 заблокировал строку A и ждёт строку B.
- Процесс 2 заблокировал строку B и ждёт строку A → Оба процесса не могут продолжить выполнение.

В БД это часто происходит из-за неправильного порядка захвата блокировок или долгих транзакций.  
Блокировки снимаются автоматически при выполнении `COMMIT`/`ROLLBACK` или если один из процессов завершён принудительно.

### Deadlock в PostgreSQL
- PostgreSQL **сам обнаруживает** взаимные блокировки.
- Чтобы предотвратить зависание:
  1. Одна из транзакций **прерывается**.
  2. Другая продолжает выполнение.
  3. Сообщение *`deadlock detected`* появляется, если PostgreSQL находит циклическую зависимость.

### Основные причины deadlock:
1. Разный порядок доступа к ресурсам в транзакциях.
2. Долгие транзакции, удерживающие блокировки слишком долго.
3. Массовые обновления/удаления в нескольких таблицах без согласованного порядка.

### Итог:
Deadlock — это ошибка проектирования или управления транзакциями, и его нужно предотвращать с помощью:
- согласованного порядка блокировок;
- минимизации времени удержания блокировок;
- разбиения долгих транзакций на более короткие.  

## 32. Что такое роли (roles)?

Роли — это именованные группы привилегий, которые могут назначаться пользователям или другим ролям.

### Основные возможности ролей:
1. Управление доступом к объектам базы данных (таблицам, схемам, функциям).
2. Централизация прав доступа: вместо назначения прав каждому пользователю отдельно, можно объединять их в роли.
3. Возможность создания иерархий: роль может содержать другие роли.

### Типичные роли в PostgreSQL:
- **LOGIN ROLE (пользователь)** — роль, которой можно войти в систему.
- **GROUP ROLE (группа)** — роль, которая объединяет права нескольких пользователей.

### Примеры:
```sql
    --Создание роли:
    CREATE ROLE analyst;
    --Назначение роли пользователю: 
    GRANT analyst TO user1; 
    --Создание роли с правами входа:
    CREATE ROLE app_user LOGIN PASSWORD ‘secret’;
```

## 33. Что такое оконные функции в SQL и зачем они нужны?

**Оконные функции (window functions)** — это специальные функции SQL, которые выполняют вычисления **над набором строк (окном)**, сохраняя при этом детализацию каждой строки.

Отличие от агрегатных функций:
- агрегаты (`SUM`, `AVG` и др.) **сворачивают строки** в один результат,
- оконные функции **сохраняют каждую строку** и добавляют дополнительное вычисленное значение.

### Зачем нужны оконные функции?
Они позволяют решать аналитические задачи, которые сложно выразить с помощью обычных агрегатов или подзапросов:
- сравнение текущей строки с другими (предыдущая/следующая запись);
- нумерация строк и ранжирование;
- кумулятивные суммы и скользящие средние;
- вычисление процентов и долей;
- упрощение построения отчётов (например, «топ N по каждой группе»).

### Синтаксис
```sql
  <function>() OVER (
      PARTITION BY <разделение>
      ORDER BY <сортировка>
      ROWS|RANGE <окно>
  )
```
- PARTITION BY — делит строки на группы (как GROUP BY, но без агрегации).
- ORDER BY — порядок строк в окне.
- ROWS / RANGE — рамки окна (текущая, предыдущие, все до текущей и т.п.).

### Примеры
1. Нумерация строк
```sql
  SELECT
    employee_id,
    department,
    ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) AS rank_in_dept
  FROM employees;
```

2. Кумулятивная сумма
```sql
  SELECT
    order_id,
    order_date,
    SUM(amount) OVER (ORDER BY order_date) AS running_total
  FROM orders;
```

3. Сравнение с предыдущим значением
```sql
  SELECT
    order_id,
    amount,
    LAG(amount, 1) OVER (ORDER BY order_date) AS prev_amount
  FROM orders;
```

4. Доля в группе
```sql
  SELECT
   department,
   salary,
   salary * 100.0 / SUM(salary) OVER (PARTITION BY department) AS pct_in_dept
  FROM employees;
```

### Основные типы оконных функций

1. **Агрегатные функции как оконные**
  - `SUM`
  - `AVG`
  - `COUNT`
  - `MAX`
  - `MIN`

2. **Ранжирование**
  - `ROW_NUMBER()` — уникальный номер строки
  - `RANK()` / `DENSE_RANK()` — ранжирование с пропусками или без
  - `NTILE(N)` — деление на группы

3. **Функции смещения**
  - `LAG()` / `LEAD()` — доступ к предыдущей или следующей строке
  - `FIRST_VALUE()` / `LAST_VALUE()` — первое/последнее значение в окне  

### Преимущества

- Читаемость SQL-запросов (без сложных подзапросов).
- Более высокая производительность, чем вложенные запросы и джойны.
- Поддержка аналитических задач (BI, OLAP).
- Универсальность (PostgreSQL, Oracle, SQL Server, MySQL ≥ 8). 

## 34. Чем отличаются агрегатные функции от оконных?

### Агрегатные функции
Агрегатные функции (`SUM`, `AVG`, `COUNT`, `MAX`, `MIN` и др.)
- Применяются к **множеству строк** и возвращают **одно значение** для всей группы.
- Используются вместе с `GROUP BY`.

**Пример:**
```sql
  SELECT department, AVG(salary) AS avg_salary
  FROM employees
  GROUP BY department;
```
→ Для каждого департамента возвращается **одна строка** с усреднённой зарплатой.

### Оконные функции
Оконные функции используют те же агрегаты, но с добавлением `OVER()`.
- Сохраняют **все строки** в результате.
- Добавляют новое вычисляемое значение для каждой строки на основе "окна".

**Пример:**
```sql
  SELECT
    employee_id,
    department,
    salary,
    AVG(salary) OVER (PARTITION BY department) AS avg_salary_in_dept
  FROM employees;
```
→ Каждому сотруднику добавляется колонка со средней зарплатой по его департаменту, но строки **не сворачиваются**.

### Итог
- **Агрегатные функции** отвечают: *«Каков общий результат для группы?»*
- **Оконные функции** отвечают: *«Каков результат для этой строки в контексте её группы?»*  

## 35. Что такое PARTITION BY и ORDER BY в оконных функциях?

#### PARTITION BY
- Разбивает набор строк на **группы (partitions)**, внутри которых применяется оконная функция.
- Похоже на `GROUP BY`, но главное отличие — строки не сворачиваются, а остаются в результате.

**Пример:**
```sql
  SELECT
    employee_id,
    department,
    salary,
    AVG(salary) OVER (PARTITION BY department) AS avg_salary_in_dept
  FROM employees;
```
→ Средняя зарплата считается **отдельно по каждому департаменту**, но все строки сотрудников сохраняются.

### ORDER BY
- Определяет **порядок строк внутри каждой группы (partition)**.
- Влияет на то, как функция считает значения (особенно для `ROW_NUMBER`, `RANK`, `LAG`, `LEAD`, кумулятивных сумм).

**Пример:**
```sql
  SELECT
    employee_id,
    department,
    salary,
    ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) AS rank_in_dept
  FROM employees;
```
→ В каждом департаменте сотрудники будут пронумерованы по зарплате от самой большой к самой маленькой.

### Использование вместе
Обычно `PARTITION BY` и `ORDER BY` работают **в паре**:
- `PARTITION BY` отвечает за разделение на группы.
- `ORDER BY` определяет порядок внутри группы.

**Пример:**
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
→ Для каждого клиента (`customer_id`) вычисляется **накопительная сумма заказов** по дате.

### Итог
- **PARTITION BY** — задаёт «окно» для функции, определяя группы.
- **ORDER BY** — определяет последовательность строк внутри каждой группы.  
  Вместе они позволяют создавать мощные аналитические вычисления прямо в SQL.  

## 36. Чем отличаются ROW_NUMBER(), RANK() и DENSE_RANK()?

#### ROW_NUMBER()
- Присваивает уникальный порядковый номер строке в рамках окна.
- **Номера всегда идут подряд** (1, 2, 3 …), даже если значения одинаковые.

**Пример:**
```sql
  SELECT
    employee_id,
    department,
    salary,
    ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) AS row_num
  FROM employees;
```
→ В каждом департаменте сотрудникам присвоятся уникальные номера по зарплате. Даже при одинаковых зарплатах номера будут разные.

### RANK()
- Присваивает ранг строкам в зависимости от значения.
- При одинаковых значениях назначается **одинаковый ранг**, но дальше будут «пропуски».

**Пример:**
```sql
  SELECT
    employee_id,
    department,
    salary,
    RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS rank_in_dept
  FROM employees;
```
→ Если два сотрудника делят 1-е место, оба получат `1`, а следующий — `3` (будет пропуск).

### DENSE_RANK()
- Работает как `RANK()`, но **без пропусков**.
- При одинаковых значениях строки получают одинаковый ранг, но следующий идёт сразу по порядку.

**Пример:**
```sql
  SELECT
    employee_id,
    department,
    salary,
    DENSE_RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS dense_rank_in_dept
  FROM employees;
```
→ Если два сотрудника делят 1-е место, оба получат `1`, а следующий — `2` (без пропуска).

### Итог
- **ROW_NUMBER()** — уникальная нумерация строк.
- **RANK()** — учитывает равенство значений, но оставляет «дыры» в нумерации.
- **DENSE_RANK()** — как `RANK()`, но без пропусков.  

## 37. Для чего нужны LAG() и LEAD()?

- **LAG()** и **LEAD()** — оконные функции смещения, которые позволяют получить значение **предыдущей** или **следующей** строки без использования подзапросов или JOIN.
- Используются в аналитике для сравнения текущей строки с соседними.

### LAG()
- Возвращает значение из **предыдущей строки** в рамках окна.
- Если предыдущей строки нет, можно указать значение по умолчанию.

**Пример:**
```sql
  SELECT
    order_id,
    order_date,
    amount,
    LAG(amount, 1, 0) OVER (ORDER BY order_date) AS prev_amount
  FROM orders;
```
→ Для каждого заказа добавляется сумма предыдущего заказа (`0`, если предыдущего нет).

### LEAD()
- Возвращает значение из **следующей строки** в рамках окна.
- Тоже можно указать значение по умолчанию.

**Пример:**
```sql
  SELECT
    order_id,
    order_date,
    amount,
    LEAD(amount, 1, 0) OVER (ORDER BY order_date) AS next_amount
  FROM orders;
```
→ Для каждого заказа добавляется сумма следующего заказа (`0`, если следующего нет).

### Практическое применение
1. Анализ динамики (например, разница между текущим и предыдущим значением).  
```sql
  SELECT
    order_date,
    amount,
    amount - LAG(amount) OVER (ORDER BY order_date) AS diff_from_prev
  FROM orders;
```
2. Нахождение «провалов» или «скачков» в данных.
3. Сравнение текущего и следующего события (например, определить время до следующего заказа клиента).
4. В финансовых и BI-отчётах — подсчёт изменения показателей по периодам.

---

### Итог
- **LAG()** — достаёт данные из прошлой строки.
- **LEAD()** — достаёт данные из следующей строки.  
  Обе функции упрощают аналитику, заменяя сложные подзапросы и делая SQL более читаемым. 

### 38. Что такое ROWS BETWEEN и как это работает?

`ROWS BETWEEN` — это часть конструкции `OVER()` в оконных функциях, которая задаёт **рамки (frame)** окна относительно текущей строки.  
Она определяет, какие строки попадут в вычисление для каждой строки результата.

#### Синтаксис
```sql
  () OVER (
    PARTITION BY …
    ORDER BY …
    ROWS BETWEEN <frame_start> AND <frame_end>
  )
```

### Варианты рамок окна
- `UNBOUNDED PRECEDING` — от первой строки до текущей.
- `n PRECEDING` — n строк до текущей.
- `CURRENT ROW` — только текущая строка.
- `n FOLLOWING` — n строк после текущей.
- `UNBOUNDED FOLLOWING` — до последней строки.

### Примеры

**a) Кумулятивная сумма (от начала до текущей строки):**
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
→ Показывает накопительный итог для каждой строки.

**b) Скользящее среднее (текущая строка + 2 предыдущие):**
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
→ Для каждой строки берутся её значение и два предыдущих, затем считается среднее.

**c) Сравнение с соседями (предыдущая, текущая, следующая):**
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
→ Для каждой строки сумма считается из трёх значений: предыдущая + текущая + следующая.

### Итог
- `ROWS BETWEEN` управляет тем, какие строки попадают в расчёт оконной функции.
- Позволяет делать **кумулятивные итоги**, **скользящие средние**, **локальные агрегаты**.
- Отличается от простого `PARTITION BY` + `ORDER BY` тем, что даёт тонкий контроль над рамками окна. 

## 39. Можно ли использовать оконные функции в WHERE?

Нет, **оконные функции нельзя использовать напрямую в `WHERE`**.  
Причина:
- `WHERE` выполняется **до** вычисления оконных функций.
- Оконные функции считаются уже на этапе формирования результирующего набора (после `FROM` → `WHERE` → `GROUP BY` → `HAVING` → `WINDOW/SELECT` → `ORDER BY`).

### Что будет, если попробовать?
```sql
  SELECT
    employee_id,
    department,
    salary,
    ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) AS row_num
  FROM employees
  WHERE row_num = 1;  – Ошибка!
```
→ Ошибка: колонка `row_num` не существует на момент выполнения `WHERE`.

### Как правильно использовать?
Есть три основных подхода:

**a) Подзапрос (derived table / CTE)**
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
→ Получим по одному сотруднику с максимальной зарплатой в каждом департаменте.

**b) Фильтрация через `QUALIFY` (если поддерживается)**
Некоторые СУБД (Snowflake, BigQuery, Oracle 23c, DuckDB) поддерживают `QUALIFY`:  
```sql
  SELECT
    employee_id,
    department,
    salary,
    ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) AS row_num
  FROM employees
  QUALIFY row_num = 1;
```
→ Упрощённый синтаксис, который фильтрует по оконным функциям.

**c) Использование оконной функции в `HAVING` (редко)**
Обычно не применяется напрямую, так как `HAVING` идёт после агрегатов, но теоретически можно обернуть в подзапрос.

### Итог
- **Нельзя** использовать оконные функции в `WHERE` напрямую.
- **Можно** фильтровать по их результату через **CTE**, **подзапрос** или **QUALIFY** (если поддерживается).  

## 40. Что такое QUALIFY?

### Определение
`QUALIFY` — это оператор SQL, который позволяет фильтровать строки **по результатам оконных функций**, подобно тому как:
- `WHERE` фильтрует по обычным колонкам,
- `HAVING` фильтрует по агрегатам.

Иными словами:  
`QUALIFY` = "WHERE для оконных функций".

### Зачем нужен?
Оконные функции считаются **после** выполнения `WHERE` и `GROUP BY`.  
Обычно, чтобы отфильтровать по оконной функции (`ROW_NUMBER`, `RANK`, `LAG` и др.), приходится использовать CTE или подзапрос.

`QUALIFY` упрощает это: позволяет писать фильтрацию по оконным функциям прямо в основном запросе.

### Пример без QUALIFY (через CTE)
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

### Тот же запрос с QUALIFY
```sql
  SELECT
    employee_id,
    department,
    salary,
    ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) AS row_num
  FROM employees
  QUALIFY row_num = 1;
```
→ Получим сотрудников с **максимальной зарплатой** в каждом департаменте, но запись выглядит намного короче.

### Где поддерживается QUALIFY
- **BigQuery** (Google)
- **Snowflake**
- **Teradata**
- **DuckDB**
- **Oracle 23c** (новые версии)

⚠️ В PostgreSQL, MySQL, MS SQL Server **оператора QUALIFY нет**, там используют CTE или подзапрос.

## 41. Оптимизация SQL-запросов: EXPLAIN и EXPLAIN ANALYZE

### Оптимизация SQL-запросов
СУБД (PostgreSQL, MySQL, Oracle, SQL Server и др.) имеют **оптимизатор запросов**, который:
- выбирает лучший план выполнения (`execution plan`);
- решает, какие индексы использовать;
- определяет порядок соединений (`join order`);
- выбирает алгоритм соединений (Nested Loop, Merge Join, Hash Join);
- оценивает стоимость (`cost`) каждой операции.

Чтобы понять, **как именно будет выполнен запрос**, используют команду `EXPLAIN`.

### Что такое EXPLAIN?
`EXPLAIN <query>` показывает **план выполнения** запроса:
- какие шаги СУБД предпримет для получения результата;
- будут ли использоваться индексы или полный скан таблицы (Seq Scan);
- порядок соединений таблиц;
- оценочную стоимость выполнения (`cost`).

**Пример:**
```sql
  EXPLAIN SELECT * FROM orders WHERE customer_id = 123;
    --Вывод (PostgreSQL):
    -- Seq Scan on orders  (cost=0.00..431.00 rows=10 width=64)
    -- Filter: (customer_id = 123)
```
→ Запрос будет делать последовательное сканирование всей таблицы (`Seq Scan`).

### Что такое EXPLAIN ANALYZE?
- `EXPLAIN ANALYZE <query>` выполняет запрос реально и показывает:
  - фактическое время выполнения каждого шага;
  - количество реально прочитанных строк;
  - сколько строк вернуло каждое действие;
  - разницу между **оценками оптимизатора** и **реальностью**.

**Пример:**
```sql
    EXPLAIN ANALYZE
    SELECT * FROM orders WHERE customer_id = 123;
    Вывод:
    Seq Scan on orders  (cost=0.00..431.00 rows=10 width=64) (actual time=0.012..15.234 rows=8 loops=1)
    Filter: (customer_id = 123)
    Rows Removed by Filter: 5000
    Execution Time: 15.300 ms
```

### Как анализировать результат
1. **Тип сканирования**
  - `Seq Scan` — полный проход таблицы (медленно для больших данных).
  - `Index Scan` / `Index Only Scan` — использование индекса (быстрее).
  - `Bitmap Heap Scan` — комбинированное чтение через индекс.

2. **Join методы**
  - `Nested Loop` — хорошо для маленьких выборок.
  - `Merge Join` — хорошо для отсортированных данных.
  - `Hash Join` — хорошо для больших неотсортированных таблиц.

3. **Cost**
  - Записывается как `cost=начало..конец`.
  - Это «оценка стоимости» (чем меньше, тем лучше).
  - Используется оптимизатором для выбора плана.
  - - Из чего складывается cost
      Оптимизатор учитывает:
    - **Сканирование строк**
      - последовательно (`Seq Scan`) дороже для больших таблиц;
      - через индекс (`Index Scan`) дешевле, если выборка небольшая.
    - **Селективность** условия (сколько строк попадёт в результат).
    - **Стоимость чтения с диска vs из памяти** (в PostgreSQL настраивается параметрами `seq_page_cost`, `random_page_cost`, `cpu_tuple_cost`).
    - **Сортировка** и **соединения** таблиц.

4. **Rows**
  - `rows=10` — прогноз оптимизатора.
  - `actual rows=...` — реальные данные при `EXPLAIN ANALYZE`.
  - Большая разница → оптимизатор неправильно оценил селективность условия.

5. **Time**
  - `actual time=0.012..15.234` — реальное время выполнения шага.
  - `Execution Time` — общее время выполнения запроса.

### Как использовать для оптимизации
- Если видишь `Seq Scan` на большой таблице → нужен **индекс**.
- Если слишком много строк «Rows Removed by Filter» → возможно, стоит **переписать условие**.
- Если `Nested Loop` на больших таблицах → стоит подумать о **Hash Join** или **Merge Join**.
- Если `actual rows` сильно отличается от `rows` → стоит обновить статистику (`ANALYZE`) или переписать запрос.

### Итог
- `EXPLAIN` — показывает план выполнения (оценки оптимизатора).
- `EXPLAIN ANALYZE` — реально выполняет запрос и показывает **фактические цифры**.
- Оптимизация сводится к:
  - использованию индексов,
  - выбору правильного типа соединений,
  - минимизации лишнего чтения строк,
  - обновлению статистики таблиц.  

## 42. Что такое BASE-подход в базах данных?

**BASE** — это акроним, противопоставляемый классической модели **ACID**.  
Используется в основном в **NoSQL** и распределённых базах данных, где важна масштабируемость и доступность.

BASE расшифровывается как:
- **Basically Available** — «в основном доступная» (система всегда отвечает, пусть даже не всегда с актуальными данными).
- **Soft state** — «мягкое состояние» (состояние системы может со временем меняться, даже без новых операций, например, при репликации).
- **Eventually consistent** — «согласованность в конечном итоге» (данные в разных узлах со временем синхронизируются, но не мгновенно).

### Отличие от ACID
- **ACID**: строгое соблюдение атомарности, согласованности, изоляции и долговечности транзакций.
- **BASE**: жертвует строгой согласованностью ради **доступности и масштабируемости**.

### Где применяется
- Распределённые системы (Cassandra, DynamoDB, CouchDB, Riak).
- Высоконагруженные веб-приложения, где важнее, чтобы система **не падала** и отвечала быстро.
- Когда допустима некоторая задержка синхронизации данных.

### Пример
Система бронирования авиабилетов в стиле BASE:
- Несколько пользователей могут одновременно видеть один и тот же «последний билет».
- Возможно, что система продаст билет двум клиентам.
- Но через короткое время данные синхронизируются, и реальная доступность исправляется.

### Плюсы
- Высокая доступность даже при сбоях.
- Отличная масштабируемость в распределённых системах.
- Более высокая производительность.

### Минусы
- Нет строгой согласованности (можно временно читать «устаревшие» данные).
- Сложнее разрабатывать приложения, где требуется жёсткая транзакционная логика.
- Согласованность достигается не мгновенно, а «со временем».

### Итог
**BASE** — это подход, противопоставляемый ACID, ориентированный на **масштабируемость и доступность** ценой строгой согласованности.  
Он идеально подходит для NoSQL и распределённых систем, где важно «лучше ответить неточно, чем не ответить совсем».  

## 43. В чем отличие BASE и ACID подходов?

### ACID (традиционный подход для реляционных БД)
- **Atomicity (Атомарность)** — транзакция выполняется полностью или не выполняется вовсе.
- **Consistency (Согласованность)** — данные всегда остаются в согласованном состоянии.
- **Isolation (Изолированность)** — параллельные транзакции не мешают друг другу.
- **Durability (Надёжность)** — данные сохраняются даже при сбое.

➡ Подходит для банковских, финансовых систем, где важна **строгая корректность**.

### BASE (подход для распределённых NoSQL систем)
- **Basically Available** — система всегда отвечает, даже если не все данные согласованы.
- **Soft State** — состояние данных может меняться со временем (например, при репликации).
- **Eventually Consistent** — в конечном итоге данные синхронизируются, но не обязательно мгновенно.

➡ Подходит для масштабируемых и высоконагруженных приложений (соцсети, онлайн-магазины), где важна **доступность** и **быстродействие**.

### Сравнение ACID vs BASE

| Критерий           | ACID                                   | BASE                                      |
|--------------------|----------------------------------------|-------------------------------------------|
| **Цель**           | Корректность и надёжность              | Масштабируемость и доступность            |
| **Согласованность** | Сразу и всегда                         | В конечном итоге (eventual consistency)   |
| **Характер данных**| Строгое состояние                      | Мягкое состояние (soft state)             |
| **Транзакции**     | Строгие, атомарные                     | Ослабленные, распределённые               |
| **Применение**     | Банки, финансы, ERP-системы            | Big Data, соцсети, e-commerce, IoT        |
| **Плюсы**          | Высокая корректность                   | Высокая доступность и масштабируемость    |
| **Минусы**         | Ограниченная масштабируемость          | Возможна временная несогласованность      |

### Итог
- **ACID** = строгая согласованность и надёжность, но меньше гибкости при масштабировании.
- **BASE** = высокая доступность и масштабируемость, но согласованность достигается не сразу.  

## 44. У вас есть таблица логов с миллиардами строк. Как вы будете ускорять поиск по дате и `user_id`?

Оптимизация должна строиться по нескольким направлениям:

### 1. Партиционирование
- Разбить таблицу по дате (`RANGE PARTITION BY ts`), чтобы запросы сразу отбрасывали ненужные партиции (*partition pruning*).
- Для больших партиций можно добавить субпартиционирование по хэшу `user_id`.  
  → Это позволит читать только релевантные куски данных.

### 2. Индексы
- Создать составной индекс `(user_id, ts)` или `(ts, user_id)` — порядок выбирается по реальным паттернам запросов.
- Для широких диапазонов дат использовать **BRIN-индекс** по `ts`, он очень лёгкий и хорошо работает на больших append-only логах.
- Для часто используемых «горячих» данных сделать **частичный индекс**, например, только за последние 30 дней.
- Если запросы возвращают дополнительные поля — использовать **covering-индекс (INCLUDE)**.

### 3. Переписывание запросов
- Не использовать функции над колонкой даты (`date(ts)`) — это ломает использование индекса.
- Вместо этого задавать диапазон:
```sql
  ts >= ‘2025-08-24 00:00:00’ AND ts < ‘2025-08-25 00:00:00’
```
### 4. Кластеризация и обслуживание
- Использовать `CLUSTER` или `pg_repack` для поддержания локальности данных.
- Следить за статистикой (`ANALYZE`) и параметрами `autovacuum`.

### 5. Дополнительно
- Для сложных аналитических сценариев использовать **TimescaleDB (hypertables)** или материализованные представления.
- Проверять реальный план выполнения через:
```sql
  EXPLAIN (ANALYZE, BUFFERS)
    SELECT …
```

### Итог
- **Партиционирование по дате** + **индексы по `user_id` и `ts`** — основное решение.
- **BRIN и частичные индексы** дают выигрыш для огромных объёмов.
- Ключевой принцип — «чтобы СУБД читала как можно меньше строк и страниц».  