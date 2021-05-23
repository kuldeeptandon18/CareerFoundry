**Solution will work on both cluster and local.
In Spark submit: master argument will be local for localMode and yarn for hadoop cluster mode**

# Database Used: Hive

## Task1: Main Code Directory: CareerFoundry/src
## Task2: File- Task2-Architect_Diagram_Implementation_Plan.docx

## Test Output: CareerFoundry/Output

# **`Task1: `**

### **Approach:**
1. Calculating, Std_dev and Moving average
Calculating the moving average, std_dev on time_period 5, can be changed by setting value to time_period variable.
Performing a rolling window operations to calculate these averages as time proceed.

2. Calculating Day Data records (date, min_price, high_price, open_price, close_price). This will be useful in other calculations to predict future movement in the price.

#### Saving Data in Hive Tables: 
    1. bitcoin_volatility_table: Table with STDDEV and moving average columns
    2. bitcoin_day_data_table: Calculated Day wise Data from given Data


**`Data quality checks:`**

1. Creating Target Table if not already exists
2. Using unit tests to validate used method functionality.

## **`Diagnose and Tune the application in case of performance problems:`**

 **There any many steps we can use, sharing some basic step:** 

 ### `1. Diagnose: `
 
 1. We can check logs to detect that on what step job failed.
 2. we can use Spark UI to see if we have any sku data or partitions not distributed in even manner on nodes.
 
### ` 2. Spark Tunning: `

 1. Check data size we going to process.
 2. Repartitioning data according to cluster or available resources.
 3. Using proper join approach, for instance broadcast small data frame, as if we don't use broadcast, all huge dataframe data will collect on single machine where small dataframe partition is present and overalll performance will recuced to very high level.
 4. Pass proper spark submit properties according to data size and operation, e.g memoryOverhead, executor timeout etc.
 5. Persisting Dataframe to appropriate level if data frame will be reused.
 6. Filtering data before joining if applicable, to avoid unnecessary data shuffling and memory issues. 
 
### ` schedule this pipeline to run periodically:`

`Solutions: `

1. We can create pipeline using Apache Airflow, that is open source and can handle may pipeline. we can use timing schedules or file sensors to trigger the workflow
2. We can create our own pipeline in combination with shell and python script(We can implement our own custom requirements)

 
### `  CI/CD implementation for this application:`

1. We can use open source software Jenkins to automate the CI/CD process
2. We can use GitLab CI/CD (we can use git and CI/CD in combination)

 
#Run Instructions

### Use spark submit(source hive table need to create, bitcoin_data_table) 
### or run unit test(used given json file for testing, crating temp table in unit test)
 
### ` arguments to pass: `

 `for BitCoinDataTransformation.py:` 
 
 --master local --appName ApplicationName --inputLocation InputFilesPath --outputLocation OutPutFilePath 
 
` Note: `

 1. master argument will be local for local mode and yarn for hadoop cluster mode
 2. appName is optional
 
 
### ` Files:`

 **Task1 :** CareerFoundry/src/main/BitCoinDataTransformation.py
 
 **Unit Tests:** CareerFoundry/src/tests/UnitTests.py
 
 **Egg File:** CareerFoundry/dist/pyspark_pytest-1.0-py3.7.egg
 
 ### Task 2 Document: Task2-Architect_Diagram_Implementation_Plan.docx
 
 ### Considerations and Suggestions:
 
  1. included some dataframe show conditions for illustration purpose, need to comment those, if running in production,as those will cause delay.
  2. Not performed any repartition in code, as had no idea about data size and cluster size we going to run.
  3. We are using hive tables and save data in external tables, as that will be helpful in data exploration layer and data validation.
  
 ## Run Process:
 
 1. `Create egg file:` `python setup.py bdist_egg`
 2. cd dist
 3. use created egg file with following spark-submit commands 
 
`Task1:`
 
 spark-submit --py-files=pyspark_pytest-1.0-py3.7.egg ../src/main/BitCoinDataTransformation.py --master local --sourceTable SourceTable
 
  Note: 
  
  1. egg file name can be different according to python version
  2. pass appropriate spark-submit config in spark submit according to size and type of data.
        1. for e.g:  spark-submit --conf SparkConfig1 --conf SparkConfig2 --py-files=pyspark_pytest-1.0-py3.7.egg ../src/main/BitCoinDataTransformation.py --master local --sourceTable SourceTable
        
 ## Table Schema: bitcoin_volatility_table 
     |-- time_period_start: timestamp (nullable = true)
     |-- time_period_end: timestamp (nullable = true)
     |-- price_open: double (nullable = true)
     |-- price_high: double (nullable = true)
     |-- price_low: double (nullable = true)
     |-- price_close: double (nullable = true)
     |-- volume_traded: double (nullable = true)
     |-- trades_count: integer (nullable = true)
     |-- simple_moving_avg_5_period: double (nullable = true)
     |-- stddev_5_period: double (nullable = true)
     |-- date: date (nullable = true)
 
 ## Table Schema: bitcoin_day_data_table 
     |-- date: date (nullable = true)
     |-- min_price: double (nullable = true)
     |-- high_price: double (nullable = true)
     |-- open_price: double (nullable = true)
     |-- close_price: double (nullable = true)

  
  
  
 