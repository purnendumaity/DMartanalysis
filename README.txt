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
D:\Python-Projects\DatapipelineAmazon\.venv\Scripts\python.exe D:\Python-Projects\DatapipelineAmazon\Pysparkcsvprocessing.py 
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).

 CSV File Schema:
root
 |-- review_date: date (nullable = true)
 |-- marketplace: string (nullable = true)
 |-- customer_id: integer (nullable = true)
 |-- review_id: string (nullable = true)
 |-- product_id: string (nullable = true)
 |-- product_parent: double (nullable = true)
 |-- product_title: string (nullable = true)
 |-- product_category: string (nullable = true)
 |-- star_rating: integer (nullable = true)
 |-- helpful_votes: integer (nullable = true)
 |-- total_votes: integer (nullable = true)
 |-- vine: boolean (nullable = true)
 |-- verified_purchase: boolean (nullable = true)
 |-- review_headline: string (nullable = true)
 |-- review_body: string (nullable = true)


 Total Rows in File: 151

 Now Showing First 5 Rows:
+-----------+-----------+-----------+--------------+----------+--------------+------------------------------------------------------------------------------+----------------+-----------+-------------+-----------+-----+-----------------+------------------------------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|review_date|marketplace|customer_id|review_id     |product_id|product_parent|product_title                                                                 |product_category|star_rating|helpful_votes|total_votes|vine |verified_purchase|review_headline                                       |review_body                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
+-----------+-----------+-----------+--------------+----------+--------------+------------------------------------------------------------------------------+----------------+-----------+-------------+-----------+-----+-----------------+------------------------------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
|1995-06-24 |US         |53096571   |RHL4UW17ZK72A |521314925 |9.81E8        |Invention and Evolution:Design in Nature and Engineering                      |Books           |5          |9            |9          |false|false            |BUY THIS BOOK!                                        |This is a beautiful book. French talks about energy, form, mechanism, and economy in natural and man-made things. He compares birds to planes in terms of fuel-capacity, energy conversion efficiency, drag, etc. He compares suspension bridges and dinosaurs. He provides examples of neat inventions and the thought that has gone into them (every- thing from steam-catapults to toy cars to grommets). This is &quot;How Things Work&quot; for the non-moron crowd.                                                                                                                                                                                                                |
|1995-06-24 |US         |53096571   |R34N4QWDXX58WB|870210092 |4.43E8        |Arming and Fitting of English Ships of War, 1600-1815                         |Books           |4          |12           |13         |false|false            |good enough to understand all of Pat O'brien          |Nice diags, lucid explanations of rigging, guns, hull, etc. A lot of the pics also appear in &quot;Nelson's Navy&quot;, so if you have that, don't bother.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
|1995-07-07 |US         |53096573   |RPLV77JZXG575 |047194128X|3.77E8        |Object-Oriented Type Systems                                                  |Books           |4          |4            |4          |false|false            |Good techniques, well written.                        |The best (and possibly only) book I've seen on the topic. I very much liked their approach of starting with a simplified language and adding the necessary features. The algorithms are useful, well presented, and their assumptions are laid out clearly.                                                                                                                                                                                                                                                                                                                                                                                                                              |
|1995-07-18 |US         |53011769   |R33HOJ2OWJIDQI|089145537X|8.37E8        |The Collector's Guide to Harker Pottery U.S.A.: Identification and Value Guide|Books           |5          |27           |27         |false|false            |The definitive volume on Harker Pottery               |OK, I'm biased. My mother wrote this one. Many of the pieces featured in the book are part of my collection. But it's still the ONLY book on Harker Pottery, America's Oldest Pottery. This book not only includes beautiful pictures of Harkerware, but also includes a price guide and some history of the pottery industry of the Ohio Valley. Harker is a contemporary of many currently collectible potteries such as Hall and Homer Laughlin. While not as valuable currently as Autumn Leaf or Fiestaware, Harker's Cameoware is becoming increasingly popular. Harker is also the only manufacturer of ceramic serving forks and spoons as well as beautiful ceramic rolling pins|
|1995-07-19 |US         |53096552   |R3SM9591SGTMJ |517593483 |1.14E8        |Fingerprints Of The Gods: The Evidence of Earth's Lost Civilization           |Books           |5          |0            |1          |false|false            |This book will open your mind to the past. Excellent!!|The author has a keen sense of how to unfold his entire theory over 500 pages so that you leave the book convinced of the strong possibility that it is true and want to read more about the subject of lost civilizations. I can't wait to read &quot;When the Sky fell&quot; once its released (by Ruth Ann-Flems)                                                                                                                                                                                                                                                                                                                                                                     |
+-----------+-----------+-----------+--------------+----------+--------------+------------------------------------------------------------------------------+----------------+-----------+-------------+-----------+-----+-----------------+------------------------------------------------------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
only showing top 5 rows

Data cleaning and transformation started

 Duplicate review_id List:
+-------------+-----+
|review_id    |count|
+-------------+-----+
|R2PXCLE4APVQG|2    |
+-------------+-----+


 Total Rows (After Deduplication): 150
Merging 1 partitioned CSV files...
Final cleaned CSV generated: D:\Python-Projects\DatapipelineAmazon\output_file\final_cleaned_awsdata.csv
All partitioned CSVs are preserved in ./processed_data/
Data cleaning and transformation completed
 Total Execution Time: 00:00:52
SUCCESS: The process with PID 14952 (child process of PID 17960) has been terminated.
SUCCESS: The process with PID 17960 (child process of PID 7748) has been terminated.
SUCCESS: The process with PID 7748 (child process of PID 18576) has been terminated.

Process finished with exit code 0

 
