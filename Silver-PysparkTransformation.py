# Databricks notebook source
spark

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.bucketadventure.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.bucketadventure.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.bucketadventure.dfs.core.windows.net", "")
spark.conf.set("fs.azure.account.oauth2.client.secret.bucketadventure.dfs.core.windows.net", '')
spark.conf.set("fs.azure.account.oauth2.client.endpoint.bucketadventure.dfs.core.windows.net", "https://login.microsoftonline.com//oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC #Data Loading
# MAGIC

# COMMAND ----------

df_cal=spark.read.format("csv").option('header','true').option('inferschema','true')\
    .load("abfss://bronze@bucketadventure.dfs.core.windows.net/AdventureWorks_Calendar")
df_cal.display()    

# COMMAND ----------

df_cust=spark.read.format("csv").option('header','true').option('inferschema','true')\
    .load("abfss://bronze@bucketadventure.dfs.core.windows.net/AdventureWorks_Customers")
df_cust.display() 

# COMMAND ----------

df_prod_cat=spark.read.format("csv").option('header','true').option('inferschema','true')\
    .load("abfss://bronze@bucketadventure.dfs.core.windows.net/AdventureWorks_Product_Categories")
df_prod_cat.display() 

# COMMAND ----------

df_prod=spark.read.format("csv").option('header','true').option('inferschema','true')\
    .load("abfss://bronze@bucketadventure.dfs.core.windows.net/AdventureWorks_Products")
df_prod.display()

# COMMAND ----------

df_ret=spark.read.format("csv").option('header','true')\
                 .option('inferschema','true')\
                 .load('abfss://bronze@bucketadventure.dfs.core.windows.net/AdventureWorks_Returns')
df_ret.display()                 

# COMMAND ----------

df_sales_2015=spark.read.format("csv").option('header','true')\
                 .option('inferschema','true')\
                 .load('abfss://bronze@bucketadventure.dfs.core.windows.net/AdventureWorks_Sales_2015')
df_sales_2015.display()

# COMMAND ----------

df_sales_2016=spark.read.format("csv").option('header','true')\
                 .option('inferschema','true')\
                 .load('abfss://bronze@bucketadventure.dfs.core.windows.net/AdventureWorks_Sales_2016')
df_sales_2016.display()

# COMMAND ----------

df_sales_2017=spark.read.format("csv").option('header','true')\
                 .option('inferschema','true')\
                 .load('abfss://bronze@bucketadventure.dfs.core.windows.net/AdventureWorks_Sales_2017')
df_sales_2017.display()

# COMMAND ----------

df_territory=spark.read.format("csv").option('header','true')\
                 .option('inferschema','true')\
                 .load('abfss://bronze@bucketadventure.dfs.core.windows.net/AdventureWorks_Territories')
df_territory.display()


# COMMAND ----------


df_prod_sub=spark.read.format("csv").option('header','true')\
                 .option('inferschema','true')\
                 .load('abfss://bronze@bucketadventure.dfs.core.windows.net/Product_Subcategories')
df_prod_sub.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Data Transformation

# COMMAND ----------

# MAGIC %md
# MAGIC ## Calender

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df_cal=df_cal.withColumn('Month',month(col('Date')))\
      .withColumn('Year',year(col('Date')))

# COMMAND ----------

df_cal.display()

# COMMAND ----------

df_cal.write.format('parquet').mode('overwrite')\
            .option('path','abfss://silver@bucketadventure.dfs.core.windows.net/AdventureWorks_Calendar')\
            .save()

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Customers

# COMMAND ----------

df_cust=df_cust.withColumn('Full_Name',concat_ws(' ',col('Prefix'),col('FirstName'),col('LastName')))
df_cust.display()

# COMMAND ----------

df_cust.write.format('parquet')\
             .mode('overwrite')\
             .option('path','abfss://silver@bucketadventure.dfs.core.windows.net/AdventureWorks_Customers')\
             .save()

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Product Category

# COMMAND ----------

df_prod_cat.display()

# COMMAND ----------

df_prod_cat.write.format('parquet')\
           .mode('overwrite')\
           .option('path','abfss://silver@bucketadventure.dfs.core.windows.net/AdventureWorks_Product_Categories')\
           .save()

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Products

# COMMAND ----------

df_prod.display()

# COMMAND ----------

df_prod=df_prod.withColumn('ProductSKU',split(col('ProductSKU'),'-')[0])\
       .withColumn('ProductName',split(col('ProductName'),' ')[0])
df_prod.display()       

# COMMAND ----------

df_prod.write.format('parquet')\
       .mode('overwrite')\
       .option('path','abfss://silver@bucketadventure.dfs.core.windows.net/AdventureWorks_Products')\
       .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Return

# COMMAND ----------

df_ret.display()

# COMMAND ----------

df_ret.write.format('parquet')\
       .mode('overwrite')\
       .option('path','abfss://silver@bucketadventure.dfs.core.windows.net/AdventureWorks_Returns')\
       .save()

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Territory

# COMMAND ----------

df_territory.display()

# COMMAND ----------

df_territory.write.format('parquet')\
            .mode('overwrite')\
            .option('path','abfss://silver@bucketadventure.dfs.core.windows.net/AdventureWorks_Territories')\
            .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Product SubCategory

# COMMAND ----------

df_prod_sub.display()

# COMMAND ----------

df_prod_sub.write.format('parquet')\
            .mode('overwrite')\
            .option('path','abfss://silver@bucketadventure.dfs.core.windows.net/AdventureWorks_Product_Subcategories')\
            .save()

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Sales

# COMMAND ----------

df_sales_2016.display()

# COMMAND ----------

df_sales_2015.count()


# COMMAND ----------

df_sales_2016.count()


# COMMAND ----------

df_sales_2017.count()

# COMMAND ----------

df_sales_2017.display()

# COMMAND ----------

df_Sales_combined = df_sales_2015.union(df_sales_2016).union(df_sales_2017)

# COMMAND ----------

df_Sales_combined.count()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

display(df_Sales_combined)

# COMMAND ----------

from pyspark.sql.functions import to_timestamp, col

df_Sales_combined=df_Sales_combined.withColumn('StockDate', to_timestamp(col('StockDate')))

df_Sales_combined.display()

# COMMAND ----------

from pyspark.sql.functions import regexp_replace

# COMMAND ----------

df_Sales_combined=df_Sales_combined.withColumn('OrderNumber',regexp_replace(col('OrderNumber'),'S','T'))
df_Sales_combined.display()

# COMMAND ----------

df_Sales_combined.groupBy('OrderDate').agg(count('OrderNumber').alias('Total_Orders')).display()

# COMMAND ----------

df_prod_cat.display()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df_Sales_combined.write.format('parquet')\
    .mode('overwrite')\
    .option('path','abfss://silver@bucketadventure.dfs.core.windows.net/AdventureWorks_Sales')\
    .save()