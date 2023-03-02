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
- Adjust schedule interval to be daily
- Adjust schedule to be daily

s3://json-reading-test/mock_data.json
Insert the permission below in the bucket
```
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "AllowUserToReadWriteObjects",
            "Effect": "Allow",
            "Principal": {
                "AWS": "arn:aws:iam::644393144861:user/nogfel-udacity"
            },
            "Action": [
                "s3:PutObject",
                "s3:GetObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::nogfel-bucket-test/*",
                "arn:aws:s3:::nogfel-bucket-test"
            ]
        }
    ]
}
```

----------------------------
redshift cluster
cluster name: redshift-cluster-1
user: awsuser
password: Awsuser0
- Remember that we need to set the cluster to public access and then, at the VPC session
we need to set an IP to 0.0.0.0 (so anyone can access this thing)

Think might be a better thing to take advantage of the code writen when creating a Data warehouse and create 
the new RedShift cluster, because it will be faster.
----------------------------

----------------------------
SET ENVIRONMENT VARIABLES
----------------------------
aws_default
----------------------------
AWS Credentials (nogfel-udacity_accessKeys_2.csv)
access key: AKIAZMCGTCIO2MFR5H3D,
secret key: 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
----------------------------

----------------------------
SET CONNECTIONS
----------------------------
postgres_default
----------------------------
user: awsuser
password: Str0n9p455w0rd
host: {get_from end_point on create_cluster.py log}
port: 5439
schema: dev
----------------------------

Run Airflow UI: `/opt/airflow/start.sh`

----------------------------
ETL Issue
The column name is been inserted at the field. Look at how to solve this
id	first_name	last_name	email	gender	ip_address
{"id":2	"first_name":"Myrtie"	"last_name":"Purcell"	"email":"mpurcell1@mapy.cz"	"gender":"Female"	"ip_address":"152.175.86.70"}	

'{"id":2' is exactly what has been inserted into column `id`.
Maybe skip line =1?
----------------------------

01/03/2023: In 28/02/2023 the Airflow was working perfectly on the Udacity environment. The next day my operators, that were working perfectly in the previous night started to present error when imported. People in the FAQ said it could be resolved by deleting the cache folder. So far nothing. It was my fault actually. It was missing a comma on my operator. So, airflow could not import the *args parameters.