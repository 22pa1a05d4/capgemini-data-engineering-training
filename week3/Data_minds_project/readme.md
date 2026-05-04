# Healthcare Data Engineering Pipeline using Medallion Architecture

This project implements an end-to-end data pipeline using PySpark.By following the Medallion Architecture pattern. The goal is to transform raw healthcare data into a clean, structured, and analytics-ready format.

The pipeline is divided into three layers: Bronze, Silver, and Gold. Each layer is responsible for gradually improving data quality and usability.



## Project Overview

The dataset contains 4 healthcare-related tables such as patients, encounters, payers, and procedures. These tables are interconnected and represent real-world hospital operations like patient visits, treatments, and billing.

The pipeline processes this data step by step:

* Raw ingestion in Bronze
* Cleaning and standardization in Silver
* Business-level transformation in Gold

---

## Bronze Layer

The Bronze layer is the raw data layer. At this stage, data is ingested exactly as it is from the source without applying any transformations.

All CSV files were loaded into Delta format and stored as individual tables. No cleaning, filtering here.

The purpose of this layer is to:

* Preserve the original data for traceability
* Enable reprocessing if needed

---

## Silver Layer

The Silver layer focuses on cleaning and standardizing the data. This is where raw data becomes usable.

The following transformations were performed:

* Removed duplicate records based on primary keys such as encounter_id and patient_id
* Renamed columns to follow consistent naming conventions (lowercase and meaningful names)
* Converted data types such as timestamps and numeric fields
* Handled missing values by either dropping invalid records or filling them with default values
* Standardized categorical fields like marital status
* Extracted structured fields such as splitting birthplace into city, state, and country
* Joined all tables into single fact table

Each table was cleaned independently, resulting in high-quality structured datasets for patients, encounters, payers, and procedures.


---

## Gold Layer

The Gold layer is designed for analytics and reporting. This is where cleaned data is transformed into a business-ready model.

A fact table was created by joining:

* Encounters (core event data)
* Patients (who the patient is)
* Payers (who paid for the encounter)
* Aggregated procedures (what was done during the encounter)

The joins were performed using inner joins to ensure only valid and complete records were retained.

The resulting dataset represents one row per encounter and includes all relevant metrics needed for analysis.

To make the dataset suitable for analytics, the following improvements were made:

* Removed unnecessary descriptive columns such as names and addresses from the main fact table
* Retained only key identifiers and measurable attributes
* Created derived columns such as:
  * Patient age at the time of encounter
  * Out-of-pocket cost calculated from claim cost and payer coverage
  *full name by concating first name and last name along with prefix and suffix

The final output is a clean fact table that can be used directly for dashboards, reporting, and advanced analytics.

---


## Key Learnings

This project gives knowledge on several important data engineering concepts:

* Understanding and maintaining data grain (one row per encounter)
* Handling one-to-many relationships using aggregation
* Avoiding duplication during joins
* Building structured pipelines using layered architecture

---
## challenges Faced
* finding raw data with null values and duplicates took lot of time
* while joining the tables.The important step in the Silver layer was handling the procedures table. Since one encounter can have multiple procedures, the data    was aggregated at the encounter level to calculate total procedure cost and procedure count to ensure consistency when joining with the encounters table later.

## Conclusion

This pipeline transforms raw healthcare data into a structured and analytics-ready dataset using a systematic approach. The Medallion Architecture ensures that data quality improves at each stage while maintaining flexibility.

The final Gold layer provides a reliable foundation for business intelligence tools and data-driven decision making.
