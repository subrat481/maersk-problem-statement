# Databricks notebook source
# MAGIC %md <h4>Solution-1: Reading Employee data in employeeDF and sampleDF dataframe from here: https://www.briandunning.com/sample-data/us-500.zip - Done</h4>

# COMMAND ----------

import pyspark.sql.functions as f
from pyspark.sql.window import Window

# Laoding sample data
empDF = spark.read.csv("dbfs:/FileStore/tables/maersk/data/us_500.csv", header=True, inferSchema=True)
sampleDF = spark.read.csv("dbfs:/FileStore/tables/maersk/data/us_500.csv", header=True, inferSchema=True)

empDF.show()

empDF.printSchema()

# COMMAND ----------

# MAGIC %md <h4>Solution-2: Using this sampleDF dataframe create 100X more data points and store it into employeeDF ( this will have 50,000 rows ) - Done</h4>

# COMMAND ----------

# Re-sampling the data
for i in range(99):
  empDF = empDF.union(sampleDF)

print(empDF.count())


# COMMAND ----------

# MAGIC %md <h4>Solution-3: The company has to plan a vaccination drive and the priority will be based on employee population in a particular city - Done <br/> Solution-4: Create a dataframe of CityEmployeeDensity, the 1st city will be the one with maximum number of employees - Done</h4>

# COMMAND ----------

from pyspark.sql.types import IntegerType, DateType

cityEmployeesDF = empDF.select('city').groupBy('city').agg(f.count('city').alias('total_employees')).orderBy(f.desc('total_employees'))

cityEmployeesSpec = Window.orderBy(f.desc('total_employees'))

cityEmployeeDensityDF = cityEmployeesDF.withColumn('vaccine_city_order', f.row_number().over(cityEmployeesSpec)).withColumnRenamed('city', 'employee_city')

cityEmployeeDensityDF.show()

# COMMAND ----------

# MAGIC %md <h4>Solution-5: Create new dataframe by name "VaccinationDrivePlan" with all columns from employeeDF and additional column "Sequence" - Done <br/> Solution-6: In Sequence Column populate the value from the CityEmployeeDensity Dataframe. Print this dataframe - Done</h4>

# COMMAND ----------

vaccinationDrivePlanDF = empDF.join(f.broadcast(cityEmployeeDensityDF), empDF.city == cityEmployeeDensityDF.employee_city, "inner").drop('total_employees').withColumnRenamed('vaccine_city_order', 'sequence').drop('employee_city').orderBy('sequence')

# print(vaccinationDrivePlanDF.count())
vaccinationDrivePlanDF.show()

# COMMAND ----------

# MAGIC %md <h4>Solution-7: Add date column, only 100 vaccinations are happening a day - Done <br/> Solution-8: Create a Final Vaccination Schedule with a plan to vaccinate 100 employees in a day - Done </h4>

# COMMAND ----------

import sys

city_spec = Window.orderBy(f.desc('days_taken'))

vaccinationScheduledDF = cityEmployeeDensityDF.withColumn('start_date', f.current_date()).withColumn('days_taken', (f.floor(f.col('total_employees') / 100))).withColumn('cumsum', f.sum(f.col('days_taken')).over(Window.partitionBy().orderBy().rowsBetween(-sys.maxsize, 0))).withColumn('cum_date', f.expr("date_add(start_date, cumsum)")).withColumn('start_date2', f.lag('cum_date', 1).over(city_spec)).withColumn('schedule_date', f.when(f.col('start_date2').isNull(), f.current_date()).otherwise(f.col('start_date2'))).drop('start_date', 'start_date2').select('employee_city', 'total_employees', 'vaccine_city_order', 'schedule_date', 'days_taken').withColumnRenamed('vaccine_city_order', 'sequence')

vaccinationScheduledDF.show()
# vaccinationScheduleDF.count()


# COMMAND ----------

# MAGIC %md <h4>Solution-9: Print the final report showing in how many day the vaccination drive is completed per city - Done <br/></h4>
# MAGIC <h4> Here I have appended all 3 columns </h4>

# COMMAND ----------

# Final Vaccination Plan
scheduleDF = vaccinationScheduleDF.select('employee_city', 'schedule_date')

finalVaccinationScheduledDF = vaccinationDrivePlanDF.join(f.broadcast(scheduleDF), vaccinationDrivePlanDF.city == scheduleDF.employee_city, "inner").drop('employee_city')

finalVaccinationScheduledDF.show()
print(finalVaccinationScheduledDF.count())

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


