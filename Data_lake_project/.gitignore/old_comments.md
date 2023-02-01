This is still a work in progress meant for collecting feedback of the work done so far. The sections **Create the EMR Cluster with CLI** and **Usefull Resources** may be or not in the final version of this project, they were inserted as mere references.

## Create the EMR Cluster with CLI
The code necessary to do this is:
```
aws emr create-cluster --applications Name=Ganglia Name=Spark Name=Zeppelin --ebs-root-volume-size 10 --ec2-attributes '{"KeyName":"spark-cluster","InstanceProfile":"EMR_EC2_DefaultRole","SubnetId":"subnet-0302ed08fe4d595c5","EmrManagedSlaveSecurityGroup":"sg-00707ce63e11173b4","EmrManagedMasterSecurityGroup":"sg-0922f9752dd20c5f7"}' --service-role EMR_DefaultRole --enable-debugging --release-label emr-5.20.0 --log-uri 's3n://aws-logs-221451026207-us-east-1/elasticmapreduce/' --name 'spark-cluster' --instance-groups '[{"InstanceCount":2,"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":32,"VolumeType":"gp2"},"VolumesPerInstance":1}]},"InstanceGroupType":"CORE","InstanceType":"m5.xlarge","Name":"Core Instance Group"},{"InstanceCount":1,"EbsConfiguration":{"EbsBlockDeviceConfigs":[{"VolumeSpecification":{"SizeInGB":32,"VolumeType":"gp2"},"VolumesPerInstance":1}]},"InstanceGroupType":"MASTER","InstanceType":"m5.xlarge","Name":"Master Instance Group"}]' --scale-down-behavior TERMINATE_AT_TASK_COMPLETION --region us-east-1
```

## Usefull Resources
This link possess lot of information on how to perform the EMR creation with boto3.
https://github.com/jaycode/short_sale_volume/blob/master/1.emrspark_lib.ipynb

Another important issue related to this project is how long does it take to read all data to S3 bucket _(which is a lot)_. So, when starting to read it, we can just performe everything by reading just a subset of the data. We can do this with the help of the page below:
https://knowledge.udacity.com/questions/488328