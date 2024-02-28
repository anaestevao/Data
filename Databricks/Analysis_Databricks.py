# Databricks notebook source
spark

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# COMMAND ----------

# Read the CSV file into a DataFrame
df = spark.read.csv('/FileStore/tables/loan_data.csv', header = True)

# COMMAND ----------

# Define schema and cast columns to desired data types
df = df.withColumn("Loan_ID", col("Loan_ID").cast("string")) \
       .withColumn("Gender", col("Gender").cast("string")) \
       .withColumn("Married", col("Married").cast("string")) \
       .withColumn("Dependents", col("Dependents").cast("int")) \
       .withColumn("Education", col("Education").cast("string")) \
       .withColumn("Self_Employed", col("Self_Employed").cast("string")) \
       .withColumn("ApplicantIncome", col("ApplicantIncome").cast("int")) \
       .withColumn("CoapplicantIncome", col("CoapplicantIncome").cast("double")) \
       .withColumn("LoanAmount", col("LoanAmount").cast("double")) \
       .withColumn("Loan_Amount_Term", col("Loan_Amount_Term").cast("int")) \
       .withColumn("Credit_History", col("Credit_History").cast("double")) \
       .withColumn("Property_Area", col("Property_Area").cast("string")) \
       .withColumn("Loan_Status", col("Loan_Status").cast("string"))

# COMMAND ----------

display(df)

# COMMAND ----------

df.count()

# COMMAND ----------

display(df.filter(df.Loan_Status == 'Y'))

# COMMAND ----------

#saving to Use in SQL
df.registerTempTable('table')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from table where Loan_Status = 'Y'

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from table

# COMMAND ----------

display(df.where(df.LoanAmount > df.ApplicantIncome).orderBy('LoanAmount', ascending =False))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from table where LoanAmount > ApplicantIncome order by LoanAmount desc

# COMMAND ----------

display(df.where((col("Credit_History") == 1) | (col("Self_Employed") == "No")))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM table
# MAGIC WHERE Credit_History = 1 OR Self_Employed = 'No';

# COMMAND ----------

display(df.where((col("Credit_History") == 1) & (col("Self_Employed") == "No")))

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM table
# MAGIC WHERE Credit_History = 1 AND Self_Employed = 'No';

# COMMAND ----------

df_filtered = df.where((col("Credit_History") == 1) & (col("Self_Employed") == "No"))

# COMMAND ----------

display(df_filtered)

# COMMAND ----------

df_filtered.write.json('/resultados/')

# COMMAND ----------

df_filtered.write.csv('/csv/', header=True, sep=',')
