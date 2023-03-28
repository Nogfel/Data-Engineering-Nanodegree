# Udacity's Capstone Project

## Important Observation
This project consists on a series of python scripts to be executed in a specific order, to be displayed, in order to generate a Data Warehouse in Redshift containing information on imigration to the US _(project suggested by Udacity)_.

## Tables Needed
- `airport-codes_csv.csv`;<br>
- Data imigration parquet files;<br>
- `I94_SAS_Labels_Descriptions.SAS`.<br>

Since all the data in `I94_SAS_Labels_Descriptions.SAS` are formatted in a particular way we need to run the `reading_sas_file.py` first to retrieve all the data we need. After running it the files `sas_descriptive_information.csv` will be generated. This files, with the `airport-codes_csv.csv` and parquet imigration dataset, will help us generate all the staging tables needed to create the tables for our final schema.

## Execution Order
This project makes use of Reshift and S3 bucket on AWS Cloud. So, in order to proceed we need to make a few adjustments in the our local machine and AWS Console.

### Preparing the Enviroment
1) Login in the AWS Console and create an AWS User;<br>
2) In the Permissions section, grant the user created permissions to: "AdministratorAccess", "AmazonRedshiftFullAccess", "AmazonS3FullAccess" and "IAMFullAccess";<br>
3) In Security credentials section, generate the Access Keys, download them and save it in a secure place;<br>
4) Set the environment variables `AWS_ACCESS_KEY` and `AWS_SECRET_ACCESS_KEY`;


### Job Execution
1) Execute `create_cluster_aws.py` file. <br>
_Creates the necessary infracstructure on AWS Cloud_;
2) Execute `reading_sas_file` file. <br>
_Exports all the necessary id and values from the SAS file as `sas_descriptive_information` file. Important to notice that this file was placed inside the data folder, which is where we want the .csv file to be stored_;
3) Create a S3 bucket, named as `nogfel-imigration`, in the same region where the redshift cluster was created _(us-west-2)_;
4) Create folders named as `imigration_data` and `sas_data` on the root of `nogfel-imigration`;
5) Upload the content of the parquet file folder to `imigration_data` folder;
6) Upload the `sas_descriptive_information.csv` to `sas_data` folder;
7) Execute `create_and_load_tables.py` file. _Creates and loads the tables necessary for the analysis._
8) Execute `quality_checks.py` file. _Checks if there are problems with the table's keys._
9) Execute the `analytics.py` file. _Perform some analysis over the data._

## Staging Tables Exploration for Necessary Data Wrangling

### `staging_imigration`
Table generated from the imigrant information available in `immigration_data_sample.csv`/parquet files.

### `sas_descriptive_information`
Table generated from the `I94_SAS_Labels_Descriptions.SAS` using the `reading_sas_file.py`. The script in .py file generates a table containing the columns: 'id', 'description' and 'column_name' from all the information we will need from the SAS file for this project. The column 'column_name' refers to the column from the imigrant table which we are generating the information.


## Data Schema To Be Generated
In the image below we can see the tables schema to be generated with this ETL process. The tables with gray contour are the staging tables, the ones in blue are dimensions tables and the fact table is in orange.

![alt text](Tables_schema.png "Tables Schema")

## ETL Execution
In the file `etl_execution.md` in the etl_execution folder is possible to see the outputs of the execution of every file described here.
