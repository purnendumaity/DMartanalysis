=============This repository has below folder structure===========
After downloading given file we have saved in subfolder /input_file
Pyspark has hadoop dependency so that is inside subfolder hadoop-3.3.6
Also Pyspark has JVM dependency which we have indicated in the program
by attaching that path to environment variable
Input files has been manually pre-downloaded and saved inside subfolder input_files
Final output file gets saved inside subfolder query_result
DMartanalysis
--subdirectory--/input_file
--subdirectory--/hadoop-3.3.6
--subdirectory--/query_result
Pysparkanalysis.py
Readme.txt

=========Additional Notes related to Hadoop Dependency=============
Pyspark needs Hadoop but it works easily in Unix but not in Windows machine.
1st we have to download the Hadoop windows utility from below url
https://github.com/cdarlint/winutils,
Then we extract zip file and utilize hadoop-3.3.6 folder
After that to give requisite permission to Hadoop winutils we have to use below command
in Command prompt through admin user.
My project path is:D:\Python-Projects\DMartanalysis
D:\Python-Projects\DMartanalysis\hadoop-3.3.6\bin>winutils.exe chmod 777 D:/Python-Projects/DMartanalysis


=========This repository has only 1 Python file Pysparkanalysis.py =============
It utilizes Pyspark package to do all cleaning and transformation and data query steps.
I have also utilized pandas, openpyxl package so that each query result pandas dataframe we can dump 
in excel file in respective sheet.

=========== Pysparkanalysis.py run console prints =====================
D:\Python-Projects\DMartanalysis\.venv\Scripts\python.exe D:\Python-Projects\DMartanalysis\Pysparkanalysis.py 
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).

 Sales.csv File Schema:
root
 |-- Order Line: integer (nullable = true)
 |-- Order ID: string (nullable = true)
 |-- Order Date: date (nullable = true)
 |-- Ship Date: date (nullable = true)
 |-- Ship Mode: string (nullable = true)
 |-- Customer ID: string (nullable = true)
 |-- Product ID: string (nullable = true)
 |-- Sales: double (nullable = true)
 |-- Quantity: integer (nullable = true)
 |-- Discount: double (nullable = true)
 |-- Profit: double (nullable = true)


 Product.csv File Schema:
root
 |-- Product ID: string (nullable = true)
 |-- Category: string (nullable = true)
 |-- Sub-Category: string (nullable = true)
 |-- Product Name: string (nullable = true)


 Customer.csv File Schema:
root
 |-- Customer ID: string (nullable = true)
 |-- Customer Name: string (nullable = true)
 |-- Segment: string (nullable = true)
 |-- Age: integer (nullable = true)
 |-- Country: string (nullable = true)
 |-- City: string (nullable = true)
 |-- State: string (nullable = true)
 |-- Postal Code: integer (nullable = true)
 |-- Region: string (nullable = true)


Query 1 Result:
+---------------+-----------------+
|Category       |Total_Sales      |
+---------------+-----------------+
|Technology     |836154.0329999966|
|Furniture      |741999.7952999998|
|Office Supplies|719047.0320000029|
+---------------+-----------------+


Query 2 Result:
+-----------+-------------+-----+
|Customer_ID|Customer Name|count|
+-----------+-------------+-----+
|WB-21850   |William Brown|37   |
+-----------+-------------+-----+


Query 3 Result:
+-------------------+
|Average_Discount   |
+-------------------+
|0.15620272163298934|
+-------------------+


Query 4 Result:
+-------+--------------------+
|Region |Unique_Products_Sold|
+-------+--------------------+
|West   |1536                |
|East   |1408                |
|Central|1316                |
|South  |1059                |
+-------+--------------------+


Query 5 Result:
+-------------+------------------+
|State        |Total_Profit      |
+-------------+------------------+
|California   |59398.31250000002 |
|New York     |58177.834100000066|
|Washington   |24405.796599999983|
|Texas        |20528.91100000002 |
|Pennsylvania |13604.935000000007|
|Georgia      |12781.342599999998|
|Arizona      |9563.200100000004 |
|Illinois     |9560.145599999993 |
|Wisconsin    |8569.869700000003 |
|Michigan     |7752.2969000000085|
|Minnesota    |7202.522500000001 |
|Virginia     |6940.111200000005 |
|Ohio         |5985.887000000001 |
|Massachusetts|5905.5446         |
|Kentucky     |4513.313999999998 |
|Tennessee    |3434.276499999999 |
|Delaware     |3336.382700000002 |
|Alabama      |2845.0624         |
|Indiana      |2707.349500000002 |
|Louisiana    |2659.2401         |
+-------------+------------------+
only showing top 20 rows


Query 6 Result:
+------------+-----------------+
|Sub-Category|Total_Sales      |
+------------+-----------------+
|Phones      |330007.0540000001|
+------------+-----------------+


Query 7 Result:
+-----------+------------------+
|Segment    |Average_Age       |
+-----------+------------------+
|Consumer   |44.60585628973223 |
|Home Office|43.28210880538418 |
|Corporate  |44.816556291390725|
+-----------+------------------+


Query 8 Result:
+--------------+-----+
|Ship Mode     |count|
+--------------+-----+
|First Class   |1538 |
|Same Day      |543  |
|Second Class  |1945 |
|Standard Class|5968 |
+--------------+-----+


Query 9 Result:
+-------------+--------------+
|City         |Total_Quantity|
+-------------+--------------+
|New York City|3217          |
|Los Angeles  |2756          |
|Philadelphia |2299          |
|San Francisco|1773          |
|Houston      |1425          |
|Seattle      |1371          |
|Chicago      |1153          |
|Columbus     |854           |
|Aurora       |611           |
|San Diego    |609           |
|Dallas       |602           |
|Jacksonville |362           |
|Detroit      |332           |
|Springfield  |282           |
|Rochester    |279           |
|Charlotte    |275           |
|Wilmington   |271           |
|Tucson       |257           |
|Phoenix      |256           |
|Dover        |256           |
+-------------+--------------+
only showing top 20 rows


Query 10 Result:
+-----------+-------------------+
|Segment    |Avg_Profit_Margin  |
+-----------+-------------------+
|Home Office|0.14286958506103364|
+-----------+-------------------+


Process finished with exit code 0

 
