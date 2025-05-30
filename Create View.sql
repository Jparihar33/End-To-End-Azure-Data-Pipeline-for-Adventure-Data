create schema gold;

--Create View Calendar
CREATE VIEW gold.calendar
AS
SELECT * FROM 
OPENROWSET 
(
    BULK 'https://bucketadventure.dfs.core.windows.net/silver/AdventureWorks_Calendar/',
    FORMAT='PARQUET'
)AS QUER1


-----------------------------------
--CREATE VIEW Customer
CREATE VIEW gold.Customer
AS
SELECT * from 
OPENROWSET
(
    BULK 'https://bucketadventure.dfs.core.windows.net/silver/AdventureWorks_Customers/',
    FORMAT='Parquet'
)as quer1

------------------------------------------------------------------------------------------------------
--Create view Products_gold
CREATE VIEW gold.product_categories
AS
SELECT* FROM 
OPENROWSET
(
    BULK 'https://bucketadventure.dfs.core.windows.net/silver/AdventureWorks_Product_Categories/',
    FORMAT='Parquet'
)as quer1


----------------------------------------------------------------
--Create view Product_Subcategories
CREATE VIEW gold.product_subcategories AS
SELECT * FROM 
OPENROWSET
(
    BULK 'https://bucketadventure.dfs.core.windows.net/silver/AdventureWorks_Product_Subcategories',
    FORMAT='PARQUET'
)as quer1

---------------------------------------------------------------------------------------------------------
--create view Products
CREATE VIEW gold.products as 
select * from OPENROWSET
(
    BULK 'https://bucketadventure.dfs.core.windows.net/silver/AdventureWorks_Products',
    FORMAT='PARQUET'
)as quer1
--------------------------------------------------------------------------------------

--Create view Returns
CREATE VIEW gold.returns as 
select * from OPENROWSET
(
    BULK 'https://bucketadventure.dfs.core.windows.net/silver/AdventureWorks_Returns',
    FORMAT='PARQUET'
)as quer1
--------------------------------------------------------------------------------------
CREATE VIEW gold.sales as 
select * from OPENROWSET
(
    BULK 'https://bucketadventure.dfs.core.windows.net/silver/AdventureWorks_Sales',
    FORMAT='PARQUET'
)as quer1
--------------------------------------------------------------------------------------

CREATE VIEW gold.territories as 
select * from OPENROWSET
(
    BULK 'https://bucketadventure.dfs.core.windows.net/silver/AdventureWorks_Territories',
    FORMAT='PARQUET'
)as quer1
--------------------------------------------------------------------------------------




CREATE DATABASE SCOPED CREDENTIAL cred_Jitendra
WITH IDENTITY = 'Managed identity'


CREATE EXTERNAL DATA SOURCE silver_source
WITH
  (
    LOCATION = 'https://bucketadventure.dfs.core.windows.net/silver' ,
    CREDENTIAL = cred_Jitendra
)


CREATE EXTERNAL DATA SOURCE gold_source
WITH
(
    LOCATION ='https://bucketadventure.dfs.core.windows.net/gold',
    CREDENTIAL=cred_Jitendra

) 


CREATE EXTERNAL FILE FORMAT format_parquet
WITH
(
    FORMAT_TYPE=PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'

)


CREATE EXTERNAL TABLE GOLD.ext_sales
 WITH (
        LOCATION = 'ext_sales',
        DATA_SOURCE = gold_source,
        FILE_FORMAT=format_parquet 
)AS
select * from gold.sales


select * from gold.ext_sales
