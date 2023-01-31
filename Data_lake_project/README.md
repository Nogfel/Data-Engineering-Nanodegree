# What needs to be done
## `etl.py` file
- Reads song_data and load_data from S3;
- Transforms them to create five different tables;
- Writes them to partitioned parquet files in table directories on S3.
    - Each table must have its specific folder. Those tables must have partitions as described below:
        - **Songs**: table files are partitioned by year and then artist;
        - **Time**: table files are partitioned by year and month;
        - **Songplays**: table files are partitioned by year and month. 

Each function must have a docstring        
# Create the EMR Cluster with CLI
The code necessary to do this is:
```
aws emr create-cluster --applications Name=Ganglia Name=Spark Name=Zeppelin --ebs-root-volume-size 10 --ec2-attributes '{"KeyName":"spark-cluster","InstanceProfile":"EMR_EC2_DefaultRole","SubnetId":"subnet-0302ed08fe4d595c5","EmrManagedSlaveSecurityGroup":"sg-00707ce63e11173b4","EmrManagedMasterSecurityGroup":"sg-0922f9752dd20c5f7"}' --service-role EMR_DefaultRole --enable-debugging --release-label emr-5.20.0 --log-uri 's3n://aws-logs-221451026207-us-east-1/elasticmapreduce/' --name 'spark-cluster' --instance-groups '[{"InstanceCount":2,"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":32,"VolumeType":"gp2"},"VolumesPerInstance":1}]},"InstanceGroupType":"CORE","InstanceType":"m5.xlarge","Name":"Core Instance Group"},{"InstanceCount":1,"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":32,"VolumeType":"gp2"},"VolumesPerInstance":1}]},"InstanceGroupType":"MASTER","InstanceType":"m5.xlarge","Name":"Master Instance Group"}]' --scale-down-behavior TERMINATE_AT_TASK_COMPLETION --region us-east-1
```

# Usefull Resources
This link possess lot of information on how to perform the EMR creation with boto3.
https://github.com/jaycode/short_sale_volume/blob/master/1.emrspark_lib.ipynb

Another important issue related to this project is how long does it take to read all data to S3 bucket _(which is a lot)_. So, when starting to read it, we can just performe everything by reading just a subset of the data. We can do this with the help of the page below:
https://knowledge.udacity.com/questions/488328

# My Experience With Project
Before I started the Data Engineering Nanodegree I read the book **Learning Spark - Lightning-Fast Data Analytics** from Jules S. Damji, Brooke Wenig, Tathagata Das & Denny Lee.

![Book Cover](https://m.media-amazon.com/images/I/51hh4ltGnnL._SX379_BO1,204,203,200_.jpg)

So, I already had pyspark installed on my machine. But since they recommend using Databricks for completing the examples I had to look for an alternative for running the code on Jupyter Notebooks. The article **How To Use Jupyter Notebooks with Apache Spark** writen by Shanika Wickramasinghe available in [this link](https://www.bmc.com/blogs/jupyter-notebooks-apache-spark/) was used to make it possible.<br>

Since could run everything on my machine I prefered to run code locally and create all the parquet files and just when everything was Ok to go to the cloud. 

In order to this first I have to type `pyspark` on the terminal and access the link that it provides so I can use the pyspark.