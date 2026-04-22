# Order Discount Rule Engine

A functional rule engine built with **Scala 3** that processes retail order transactions, evaluates discount eligibility based on configurable business rules, calculates final prices, and persists results to an Oracle database — all while adhering strictly to functional programming principles.

---

## Project Purpose

This engine reads raw order transactions from a CSV file, applies a set of qualifying and calculation rules to determine applicable discounts, computes final prices, and writes the enriched records to a database table. All engine events are logged to a file for traceability.

---

## Prerequisites

Make sure you have the following installed:

| Tool | Version |
|---|---|
| JDK | 11 or higher |
| sbt | 1.x |
| Oracle Database XE | Running locally on port `1521` |

---

## Setup

### 1. Clone the repository

```bash
git clone https://github.com/SaraTamer/order-discount-system.git
cd order-discount-system
```

### 2. Configure the database connection

Edit `src/main/resources/db.conf` with your Oracle credentials:

```hocon
oracle.url      = "jdbc:oracle:thin:@localhost:1521:XE"
oracle.user     = "your_username"
oracle.password = "your_password"
oracle.table    = "orders_with_discounts"
```

### 3. Create the target database table

Run the following DDL in your Oracle instance before executing the engine:

```sql
CREATE TABLE orders_with_discounts (
  timestamp      VARCHAR2(50),
  product_name   VARCHAR2(100),
  expiry_date    VARCHAR2(50),
  quantity       NUMBER,
  unit_price     NUMBER(10,2),
  channel        VARCHAR2(50),
  payment_method VARCHAR2(50),
  discount       NUMBER(5,2),
  final_price    NUMBER(10,2)
);
```

### 4. Add your input CSV

Place your transactions file at:

```
src/main/resources/TRX10M.csv
```

The file must have the following header:

```
timestamp,product_name,expiry_date,quantity,unit_price,channel,payment_method
```

> **Note:** The full dataset is not committed to this repository due to its size. A sample file (`TRX1000.csv`) is included for quick testing — rename or replace it with your full dataset as needed.

---

## Running the Engine

```bash
sbt run
```

The engine will:
1. Read and parse transactions from the CSV file
2. Evaluate each transaction against all discount rules
3. Calculate the final price after discount
4. Write results to the `orders_with_discounts` Oracle table
5. Log all engine events to `logs` directory

---

## Logs

All engine events are written to this directory:

```
logs
```

Log format:

```
TIMESTAMP LOGLEVEL MESSAGE
```

---

## Tech Stack

- **Scala 3.3.7**
- **sbt** — build tool
- **Oracle XE** — output database (via `ojdbc8` + `ucp` connection pool)
- **scala-parallel-collections** — parallel data processing
- **Typesafe Config** — configuration management

---

## Functional Programming Constraints

This project was built under strict functional programming rules:

- `val`s only — no `var`s
- No mutable data structures
- No loops — recursion and higher-order functions only
- All functions are pure (no side effects, output depends solely on input)
- Functional error handling for all I/O operations
