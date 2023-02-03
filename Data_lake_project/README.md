# Data Lake Project
This project consist of reading data from `.json` files into a Spark application, performs some transformations and exporting the output tables as parquet format. 

## File Structure
**`data`:** These folders contain another two folders inside which stores the .json files from log data (log-data) and songs data (song-data). <br>

**`metastore_db`:** A Hive metastore warehouse (aka spark-warehouse) is the directory where Spark SQL persists tables whereas a Hive metastore (aka metastore_db) is a relational database to manage the metadata of the persistent relational entities, e.g. databases, tables, columns, partitions. For more information about it see [this link](https://jaceklaskowski.gitbooks.io/mastering-spark-sql/content/spark-sql-hive-metastore.html). Since the data wrangling was performed in SparkSQL this repository was automatically generated.<br>

**`data-lake-etl-process.ipynb`:** This notebook was used to perform the exploration and experiences necessary to write the `etl.py` script. In the "My Experiences With This Project" section more information is provided about some configurations performed in order to run Spark on the notebook.<br>

**`derby.log`:** File created due to the execution of the spark-shell. It records informations related to the execution of the spark-shell.

**`etl.py`:** This contains the functions and imports necessary to execute the whole ETL process.

## My Experience With This Project
Before I started the Data Engineering Nanodegree I read the book **Learning Spark - Lightning-Fast Data Analytics** from Jules S. Damji, Brooke Wenig, Tathagata Das & Denny Lee.

![Book Cover](https://m.media-amazon.com/images/I/51hh4ltGnnL._SX379_BO1,204,203,200_.jpg)

So, I already had pyspark installed on my machine. But since they recommend using Databricks for completing the examples I had to look for an alternative for running the code on Jupyter Notebooks. The article **How To Use Jupyter Notebooks with Apache Spark** writen by Shanika Wickramasinghe available in [this link](https://www.bmc.com/blogs/jupyter-notebooks-apache-spark/) was used to make it possible. Even though this is an interesting thing for the exploration part of the project I had to revert all this changes otherwise the application would look for a Jupyter Notebook for execution returning error when submitting the command `spark-submit etl.py`.