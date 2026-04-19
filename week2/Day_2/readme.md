

This notebook is created to practice PySpark using real-like data such as customers, policies, claims, and agents.

First, multiple datasets are created and connected using joins to understand how real-world data is linked. Customers are connected with their policies, claims, and agents.

---------------

## Operations Performed
* Calculating total premium and total claim for each customer
* Handling missing values using fillna
* Renaming columns for better understanding
* Identifying and marking invalid data like negative premium or claim amounts
* Cleaning the data by replacing invalid values with proper values

Some validation checks are also done to find unmatched records using joins.

Then, aggregation is performed to understand:

* Total premium and claim per customer
* City-wise analysis of claims and premium
* Overall risk score based on claim and premium

SQL queries are also used to practice advanced concepts like:

* Calculating risk score for each customer
* Finding top risky customers in each city using ranking
* Comparing monthly claims using lag function
* Ranking agents based on performance

# final Outcome

This is mainly a practice notebook to improve understanding of PySpark joins, data cleaning, aggregations, window functions, and SQL queries in a simple and practical way.

