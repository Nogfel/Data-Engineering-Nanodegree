# Data Lake Project
This project consist of reading data from `.json` files into a Spark application, performing some transformations and exporting the output tables as parquet format. An important observation about this project is that even though the Udacity's Data Engineering Nanodegree program presents Spark 2, **this project was writen in Spark 3**.

## File Structure
**`data`:** These folders contain another two folders inside which stores the .json files from log data (log-data) and songs data (song-data). *When this project was ran reading and writing data using S3 this folders was copied to S3, in order to read and write data on S3.*<br>

**`metastore_db`:** A Hive metastore warehouse (aka spark-warehouse) is the directory where Spark SQL persists tables whereas a Hive metastore (aka metastore_db) is a relational database to manage the metadata of the persistent relational entities, e.g. databases, tables, columns, partitions. For more information about it see [this link](https://jaceklaskowski.gitbooks.io/mastering-spark-sql/content/spark-sql-hive-metastore.html). Since the data wrangling was performed in SparkSQL this repository was automatically generated.<br>

**`data-lake-etl-process.ipynb`:** This notebook was used to perform the exploration and experiences necessary to write the `etl.py` script. In the "My Experiences With This Project" section more information is provided about some configurations performed in order to run Spark on the notebook.<br>

**`derby.log`:** File created due to the execution of the spark-shell. It records informations related to the execution of the spark-shell.

**`etl.py`:** This contains the functions and imports necessary to execute the whole ETL process.

## My Experience With This Project
Before I started the Data Engineering Nanodegree I read the book **Learning Spark - Lightning-Fast Data Analytics** from Jules S. Damji, Brooke Wenig, Tathagata Das & Denny Lee.

![Book Cover](https://m.media-amazon.com/images/I/51hh4ltGnnL._SX379_BO1,204,203,200_.jpg)

So, I already had pyspark installed on my machine. But since they recommend using Databricks for completing the examples I had to look for an alternative for running the code on Jupyter Notebooks. The article **How To Use Jupyter Notebooks with Apache Spark** writen by Shanika Wickramasinghe available in [this link](https://www.bmc.com/blogs/jupyter-notebooks-apache-spark/) was used to make it possible. Even though this is an interesting thing for the exploration part of the project I had to revert all this changes otherwise the application would look for a Jupyter Notebook for execution returning error when submitting the command `spark-submit etl.py`.

## Execution modes

### Process, Read and Write Data Locally
For the local execution everything ran ok after writing the SparqSQL code. I just needed to use the code `spark-submit etl.py`. The Spark Session code I used to run on this mode was
```python
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark
```


### Process Data Locally, Read and Write Data on S3
This option was little bit tricky. If you simply pass your AWS access and secret key things will not work. 
In my first attempt to use the `spark-submit etl.py` command I kept getting an error because the application does not recognize the S3 storage *(my spark installation, at least)*. So, after some reaserching I found out that I need to insert some `.jar` files referent to aws and hadoop. I found the needed jar files in [this link](https://jar-download.com/artifacts/org.apache.hadoop/hadoop-aws). It is important to pay attention to the dependencies and the version of those files. After I downloaded those files and placed them in my jar folder on the Spark installation I to change my SparkSession code from:
```python
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark
```
To...
```python
SparkSession\
        .builder\
        .appName("SparkS3Example")\
        .config("spark.hadoop.fs.s3a.access.key", os.environ['AWS_ACCESS_KEY_ID'])\
        .config("spark.hadoop.fs.s3a.secret.key", os.environ['AWS_SECRET_ACCESS_KEY'])\
        .getOrCreate()
```
I also had to reference the hadoop3 jar file in the `--jar` parameter in my `spark-sumbit` command. So, at the end, the command used was this one:<br>
`spark-submit --jars /home/felipe/Documentos/pyspark/spark-3.3.0-bin-hadoop3/jars etl.py`.<br>
With all these problems related to recognizing S3 storage solved, I just had one last problem to fase. I had no authorization to access the buckets where I read and write the data. This problem was easier to solve. <br>
I just double-checked my users policies and guaranteed that I had "AdministratorAccess", "AmazonRedshiftFullAccess" and "AmazonS3FullAccess". I don't think that the RedShift one was necessary in this case. Finally, I just had to insert the policies on the two buckets I used in this project, so the code would run.
```
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AllowUserToReadWriteObjects",
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::[AWS user ID]:user/[user name]"
      },
      "Action": [
        "s3:PutObject",
        "s3:GetObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::[bucket name]/*",
        "arn:aws:s3:::[bucket name]"
      ]
    }
  ]
}
```

### Process Data on EMR Cluster, Read and Write Data on S3
I was also able to run this Spark application on an EMR cluster on AWS reading and writing data from and on a S3 bucket.<br>
This was done by submitting a `.py` file with some modifications when passing the AWS ACCESS KEY and SECRET KEY.
After creating the EMR Cluster, I needed to add two extra permissions to my user (*AmazonEMRFullAccessPolicy_v2* and *AmazonEC2FullAccess*) and create a step in the cluster where I passed the path of the `.py` file on S3.  
For automation processes I think it might be a better idea to run the cluster using SSH.