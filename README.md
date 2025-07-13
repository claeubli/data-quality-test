# Data quality test
This repository contains generic data quality tests written in bigquery sql. Below is a break down of what kind of tests you can find here.

## Duplication test
This test will use your meta data of your tables in specified datasets. It will use the primary key specified for each table within the dataset
and check if it contains duplicates. If duplicates were identified it will write a new record to the table.

## Domain test
The domain test ensures that a field only containse the expected values. We distinguish between the following types: 
- Categorical: Ensuring that field only contains a finite list of acceptable values.
- Numeric: Ensuring that the numerical value lays between a lower and an upper bound.

In order to run the domain tests we need to specify the table path, affected field name and domain test type in a seperate table.
Once this is specified we can run the domain test. If there is violoation of the constraint a new record will be created in the table.
