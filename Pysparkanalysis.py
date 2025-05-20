#openpyxl and setuptools package installation required to resolve some dependency
import os
import glob
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum, countDistinct, max, when, round
from pyspark.sql.types import IntegerType, DoubleType
import pandas as pd


#Set environment paths for Hadoop & Java
#https://github.com/cdarlint/winutils,
#download extract zip file and utilize hadoop-3.3.6 folder
os.environ["HADOOP_HOME"] = "D:/Python-Projects/DMartanalysis/hadoop-3.3.6"
os.environ["HADOOP_BIN"] = "D:/Python-Projects/DMartanalysis/hadoop-3.3.6/bin"
os.environ["JAVA_HOME"] = "C:/Program Files/Java/jdk1.8.0_321"
os.environ["PATH"] += os.pathsep + os.environ["HADOOP_BIN"]

output_path = "./query_result/combined_summary.xlsx"

# Initialize Spark Session
spark = SparkSession.builder.appName("DMartanalysis").config("spark.driver.memory", "2g").getOrCreate()

#File schema check
local_file_path_Sales = "./input_file/Sales.csv"
local_file_path_Product = "./input_file/Product.csv"
local_file_path_Customer = "./input_file/Customer.csv"
df = spark.read.csv(local_file_path_Sales, header=True, inferSchema=True)
print("\n Sales.csv File Schema:")
df.printSchema()
df = spark.read.csv(local_file_path_Product, header=True, inferSchema=True)
print("\n Product.csv File Schema:")
df.printSchema()
df = spark.read.csv(local_file_path_Customer, header=True, inferSchema=True)
print("\n Customer.csv File Schema:")
df.printSchema()

# ------------------------------------------
# Task 2: Load Data into PySpark DataFrames
# ------------------------------------------

df_customer = spark.read.csv("./input_file/Customer.csv", header=True, inferSchema=True)
df_product = spark.read.csv("./input_file/Product.csv", header=True, inferSchema=True)
df_sales = spark.read.csv("./input_file/Sales.csv", header=True, inferSchema=True)

# ------------------------------------------
# Task 3: Data Transformation and Cleaning
# ------------------------------------------

# Example column renaming (if needed)
df_customer = df_customer.withColumnRenamed("Customer ID", "Customer_ID")
df_product = df_product.withColumnRenamed("Product ID", "Product_ID")
df_sales = df_sales.withColumnRenamed("Product ID", "Product_ID") \
                   .withColumnRenamed("Customer ID", "Customer_ID")

# Handle missing values
df_customer = df_customer.dropna()
df_product = df_product.dropna()
df_sales = df_sales.dropna()

# Ensure correct data types
df_sales = df_sales.withColumn("Sales", col("Sales").cast(DoubleType())) \
                   .withColumn("Profit", col("Profit").cast(DoubleType())) \
                   .withColumn("Discount", col("Discount").cast(DoubleType())) \
                   .withColumn("Quantity", col("Quantity").cast(IntegerType()))
df_customer = df_customer.withColumn("Age", col("Age").cast(IntegerType()))

# Join DataFrames
df_joined = df_sales.join(df_product, on="Product_ID", how="inner") \
                    .join(df_customer, on="Customer_ID", how="inner")

df_joined.cache()  # Optional: Speed up repeated queries

# ------------------------------------------
# Task 4 & 5: Data Analysis and Queries
# ------------------------------------------

# 1. Total sales for each product category
q1 = df_joined.groupBy("Category").agg(sum("Sales").alias("Total_Sales")).orderBy("Total_Sales", ascending=False)


# 2. Customer with highest number of purchases
q2 = df_joined.groupBy("Customer_ID", "Customer Name").count().orderBy("count", ascending=False).limit(1)

# 3. Average discount across all products
q3 = df_joined.agg(avg("Discount").alias("Average_Discount"))

# 4. Unique products sold in each region
q4 = df_joined.groupBy("Region").agg(countDistinct("Product_ID").alias("Unique_Products_Sold")).orderBy("Unique_Products_Sold", ascending=False)


# 5. Total profit generated in each state
q5 = df_joined.groupBy("State").agg(sum("Profit").alias("Total_Profit")).orderBy("Total_Profit", ascending=False)

# 6. Product sub-category with highest sales
q6 = df_joined.groupBy("Sub-Category").agg(sum("Sales").alias("Total_Sales")).orderBy("Total_Sales", ascending=False).limit(1)

# 7. Average age of customers in each segment
q7 = df_joined.groupBy("Segment").agg(avg("Age").alias("Average_Age"))

# 8. Orders shipped in each shipping mode
q8 = df_joined.groupBy("Ship Mode").count().alias("Total_Orders")

# 9. Total quantity sold in each city
q9 = df_joined.groupBy("City").agg(sum("Quantity").alias("Total_Quantity")).orderBy("Total_Quantity", ascending=False)

# 10. Customer segment with highest profit margin
df_margin = df_joined.withColumn("Profit_Margin", when(col("Sales") > 0, col("Profit") / col("Sales")).otherwise(0))
q10 = df_margin.groupBy("Segment").agg(avg("Profit_Margin").alias("Avg_Profit_Margin")).orderBy("Avg_Profit_Margin", ascending=False).limit(1)


# Get all results and summary sheet in an excel file
queries = [q1, q2, q3, q4, q5, q6, q7, q8, q9, q10]
# Descriptions of each query
query_descriptions = [
                        "1. Total sales for each product category",
                        "2. Customer with the highest number of purchases",
                        "3. Average discount given on sales across all products",
                        "4. Unique products sold in each region",
                        "5. Total profit generated in each state",
                        "6. Product sub-category with the highest sales",
                        "7. Average age of customers in each segment",
                        "8. Orders shipped in each shipping mode",
                        "9. Total quantity of products sold in each city",
                        "10. Customer segment with the highest profit margin"
                    ]

# Create a summary DataFrame
summary_df = pd.DataFrame({
    "Sheet": [f"Query_{i + 1}" for i in range(len(query_descriptions))],
    "Description": query_descriptions
})

# Write all sheets
with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
    # Write summary first (can also be last if preferred)
    summary_df.to_excel(writer, sheet_name="Summary", index=False)
    # Write each query to its own sheet
    for i, q in enumerate(queries, start=1):
        print(f"\nQuery {i} Result:")
        q.show(truncate=False)
        pdf = q.toPandas()
        pdf.to_excel(writer, sheet_name=f"Query_{i}", index=False)

spark.stop()
