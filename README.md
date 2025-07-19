# CSV to SQL Convertor

A Python tool to convert/export banking transaction data from CSV files into an SQLite database. Easily migrate structured data for analysis, reporting, or integration with other tools.
---

## Features

- Import transaction data from any CSV file
- Validates and cleans data before insertion
- Automatically creates SQLite database with appropriate schema
- Simple logging to monitor migration steps
- Comes with sample data for quick testing
- Includes script to view or verify the resulting database

---
## Requirements


- Python 3.x
- [pandas](https://pandas.pydata.org/)

  ---

## Installation

1. **Clone this repository:**

    ```
    git clone https://github.com/subhratagarwal/csv-to-sql-convertor.git
    cd csv-to-sql-convertor
    ```

2. **Install dependencies:**

    ```
    pip install pandas
    ```

## Usage

### Run migration

Convert a CSV file to an SQLite database:

