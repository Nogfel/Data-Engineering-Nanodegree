# Data pipeline using Apache Airflow

## Introduction
This projects consists in extracting data from Amazon S3, insert it and treat on Reshift. All this pipeline is orchestrated using Apache Airflow. It is important to say that this project was created as a prerequisite to complete the Udacity Data Engineering Nanodegree. For this specific project a workspace is provided online to interact with Apache Airflow, and we can use it. This project makes use of this workspace to run the DAG.

## How does it work
### Creating a Redshift Cluster
First, we need to create the redshift cluster on AWS. It can be done using the script on `create_cluster_aws.py`. This script will create all necessary adjustments to make possible to run a redshift instance on AWS cloud. After the cluster is created it will also monitor the cluster every 5 seconds to see if it is already ready to be used. When it is, a message will be displayed a message like this:<br>
```
Cluster status: available
End point:sparkfy-cluster-airflow.cn9fr3aedzqc.us-west-2.redshift.amazonaws.com
Role Arn: arn:aws:iam::644393144861:role/sparkfyRole
Endpoint:  sparkfy-cluster-airflow.cn9fr3aedzqc.us-west-2.redshift.amazonaws.com
IAM_ROLE_ARN:  arn:aws:iam::644393144861:role/sparkfyRole
VPC_ID:  vpc-03b0b75d5b6be573d
list_security_group: , [ec2.SecurityGroup(id='sg-08150bfb2af63fc01')]
Defaul Security Group: ec2.SecurityGroup(id='sg-08150bfb2af63fc01')
An error occurred (InvalidPermission.Duplicate) when calling the AuthorizeSecurityGroupIngress operation: the specified rule "peer: 0.0.0.0/0, TCP, from port: 5439, to port: 5439, ALLOW" already exists
```
### Creating the tables
After the cluster is ready to be used we need to access it and enter the Query Editor in the AWS Console and perform the queries available in the `create_tables.sql` file. When you run those queries all the tables necessary to perform the ELT process will be created and we can move to Airflow setup.

### Airflow setup
As said earlier, this project was conducted using Udacity's provided workspace. In order to run an Airflow instance on it we need to enter `/opt/airflow/start.sh` on the terminal and wait until the Airflow is ready. When the proper message appears we access the Airflow UI, clicking the proper button.

In the Airflow UI we need to access the green superior bar and click in 'Admin' and then 'Connections'. In the windows that will appear we need to edit two Conn Id provided by Airflow as default. The first one is `aws_default`. Here we will fill the Login and Password field with our AWS Access Key and Secret Key, in this specific order. The next one is `postgres_default`. Here we insert all the information necessary to perform the connection with our redshift cluster. _No need to worry with opening access to an external IP on redshift cluster in AWS Console. All that was taken care of with our `create_cluster_aws.py` script_.

### Running the DAG
In Airflow's DAG session we will note our DAG set to off. We need to click on 'off' to activate it. After the DAG is 'on' we will see that its jobs will start to execute. We just need to wait for it to finish and that's it. After completion, we will see an image like this one (`DAG_success_status.png`).

![DAG Run](Data_pipeline/DAG_success_status.png)
*DAG with complete status after it ran*

## My experience with the project
This was a project that made me use Udacity's Knoledge session quite a lot. I did not take notes of all the help I got from there but it did help. The Project FAQs also had some important information for this project completion. Besides this, there were somethings I had to find on my on, such as inumeral creepy errors due to forgetting to put a comma somewhere on the code.

A mistake I did that freaked me out was posting my AWS credentials on GitHub. As soon as I committed those on GitHub I imediate deleted those credentials and generated new ones. After sometime without been able to create my redshift cluster for coding I figure out that AWS put the user with leaked credentials on quarentine and block its permissions. So, I had to delete that user and create a new one and everything went fine. _I will have to learn how to remove permanently this kind of information from my GitHub repository. This mistake I did, by what I found out online seens to be quite common..._

One final point I did not understood was why my `LoadFactOperator` read my *table_name* method as a tuple in my code. To make it work I had to refer to the first element of the tuple (`self.table_name[0]`) otherwise the code would break *(line 29 of `load_fact.py`)*. At the same time, in my `LoadDimensionOperator`, if I pass the *table_name* method the same way I did with my `LoadFactOperator` on the query I would get an error.  