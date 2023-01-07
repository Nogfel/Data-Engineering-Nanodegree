# Data Warehouse Creation on S3
This project consists on creating a Data Warehouse on Amazon Redshift based on metadata collected on songs log files data. In order to do that a Redshift Cluster is created using Python SDK and the data is ingested and prepared using Python and SQL. The used in this project is collected from a JSON file and stored in a stage table. From there, we insert the data into a star schema composed of fact and dimension tables. At the end the data inserted will be queryed to show that all the steps were executed successfully and the cluster will be deleted.

## Creating AWS resources
Since the data will be hosted in a AWS we need to create the environment at the AWS cloud. In order to do that...
*Still need to finish this session*. 


## Comments for first attempt:
I took me sometime to get this far. Since I since it is most critical part of the project I would like it to reviwed so I can proceed to the finishing touches. I still need to create another python file to delete the cluster and the IAM information. Up to this is point, the files below need to be executed in this order so the project can run.
Code sequence execution:
- `create_cluster_aws.py`;
- `create_tables.py`;
- `etl.py`;
- `analysis.py`


GITHUB REPOSITORIES THAT HELPED ME A LITTLE:
- https://github.com/ulmefors/udacity-nd027-data-warehouse
- https://github.com/jazracherif/udacity-data-engineer-dwh