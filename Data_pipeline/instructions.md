# Prerequisites:
- Create an IAM User in AWS.
- Follow the steps on the page Create an IAM User in AWS in the lesson Data Pipelines.
- Create a redshift cluster in AWS.
- Follow the steps on the page Create an AWS Redshift Cluster in the lesson Data Pipelines. Ensure that you are creating this cluster in the us-west-2 region. This is important as the s3-bucket that we are going to use for this project is in us-west-2.

# Setting up Connections
- Connect Airflow and AWS
- Follow the steps on the page Connect Airflow to AWS in the lesson Data Pipelines.
- Use the workspace provided on the page Project Workspace in this lesson.
- Connect Airflow to the AWS Redshift Cluster
- Follow the steps on the page Add Airflow Connections to AWS Redshift in the lesson Data Pipelines.

# Datasets
For this project, you'll be working with two datasets. Here are the s3 links for each:
- Log data: s3://udacity-dend/log_data;
- Song data: s3://udacity-dend/song_data.

# DAG parameters
The tasks dependencies should be set as this image:
`/home/felipe/Documentos/Udacity/Data Engineering Nanodegree/Data_pipeline/task-dependencies.png`
- The DAG does not have dependencies on past runs
- On failure, the task are retried 3 times
- Retries happen every 5 minutes
- Catchup is turned off
- Do not email on retry

# User and password for airflow 
The account created has the login airflow and the password airflow.

