## Delta Table
Storage format built on top of data lake,Supports update, delete, merge and maintains history of data

----------------
 
## Creating Delta Table
Write DataFrame in delta format
Use mode("overwrite") for fresh table

-------------------------

## DataFrame vs DeltaTable
DataFrame → view / transform data
DeltaTable → update / delete / merge data
DataFrame has no history
DeltaTable tracks versions

------------------------

## MERGE Operation


Used for update + insert together
If match → update
If not match → insert
Always use .execute()

--------------------------

## DELETE Operation
Remove records using condition
Works only on DeltaTable

----------------------------

### Schema Evolution
Adding new column to existing table
Done using:
withColumn() to add column
overwriteSchema = true while writing
Old rows → null values for new column

-------------------------------

## Viewing Data
Use DataFrame to display data
Convert DeltaTable using .toDF()
Always reload data after changes


--------------------------

## History and time travel
Shows all operations on table
Includes version, operation, timestamp
Read previous versions of data
Useful for debugging and tracking

---------------------

### Restore
Revert table to previous version
Applied on Delta table (not DataFrame)
