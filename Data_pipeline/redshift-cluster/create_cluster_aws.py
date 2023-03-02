import configparser
import os
import json

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

KEY = os.environ['AWS_ACCESS_KEY_ID']
SECRET = os.environ['AWS_SECRET_ACCESS_KEY']
IAM_ROLE_NAME = config.get('CLUSTER', 'IAM_ROLE_NAME')
CLUSTER_TYPE = config.get('CLUSTER', 'CLUSTER_TYPE')
CLUSTER_IDENTIFIER = config.get('CLUSTER', 'CLUSTER_IDENTIFIER')
DB_USER = config.get('CLUSTER', 'DB_USER')
DB_PASSWORD = config.get('CLUSTER', 'DB_PASSWORD')
DB_PORT = config.get('CLUSTER', 'DB_PORT')
NODE_TYPE = config.get('CLUSTER', 'NODE_TYPE')
NUM_NODES = config.get('CLUSTER', 'NUM_NODES')


def create_aws_resources():
    '''Creates the ec2, s3, iam and redshift instances necessary to
    complete the project'''
    import boto3
    ec2 = boto3.resource('ec2',
                        region_name="us-west-2",
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET
                        )

    s3 = boto3.resource('s3',
                        region_name="us-west-2",
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET
                    )

    iam = boto3.client('iam',aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET,
                        region_name='us-west-2'
                    )

    redshift = boto3.client('redshift',
                        region_name="us-west-2",
                        aws_access_key_id=KEY,
                        aws_secret_access_key=SECRET
                        )
    
    return ec2, s3, iam, redshift


def create_iam_role(iam):
    '''Create the IAM role and attach role policy to perform the tasks'''
    from botocore.exceptions import ClientError

    try:
        print("Creating a new IAM Role") 
        sparkfyRole = iam.create_role(
            Path='/',
            RoleName=IAM_ROLE_NAME,
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                'Effect': 'Allow',
                'Principal': {'Service': 'redshift.amazonaws.com'}}],
                'Version': '2012-10-17'})
        )
        iam.attach_role_policy(RoleName=IAM_ROLE_NAME,
                       PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                      )['ResponseMetadata']['HTTPStatusCode']
    except Exception as e:
        print(e)

    roleArn = iam.get_role(RoleName=IAM_ROLE_NAME)['Role']['Arn']

    return roleArn

def create_cluster(redshift, roleArn):
    '''Creates the Redshift cluster in the AWS cloud'''
    import time
    try:
        redshift.create_cluster(
            ClusterType = CLUSTER_TYPE,
            ClusterIdentifier = CLUSTER_IDENTIFIER,
            NodeType=NODE_TYPE,
            NumberOfNodes=int(NUM_NODES),
            MasterUsername = DB_USER,
            MasterUserPassword = DB_PASSWORD,

            IamRoles=[roleArn]
        )
        print("Creating cluster...")

        i = 0
        while i == 0:
            sparkfy_cluster = redshift.describe_clusters(ClusterIdentifier=CLUSTER_IDENTIFIER)['Clusters'][0]
            if sparkfy_cluster['ClusterStatus'] != 'available':
                print('Cluster status: {}. Next check in 5 seconds'.format(sparkfy_cluster['ClusterStatus']))
                time.sleep(5)
            else:
                END_POINT = sparkfy_cluster['Endpoint']['Address']
                IAM_ROLE_ARN = sparkfy_cluster['IamRoles'][0]['IamRoleArn']
                VPC_ID = sparkfy_cluster['VpcId']
                print('Cluster status: {}'.format(sparkfy_cluster['ClusterStatus']))
                print('End point:{}'.format(END_POINT))
                print('Role Arn: {}'.format(IAM_ROLE_ARN))
                i = 1

    except Exception as e:
        print(e)
    
    return END_POINT, IAM_ROLE_ARN, VPC_ID

def open_tcp_port(VPC_ID, ec2):
    '''Make the adjustments in the VPC so the security group 
    can make changes in the cluster from anywhere in the globe'''
    try:
        vpc = ec2.Vpc(id=VPC_ID)
        print("list_security_group: ,", list(vpc.security_groups.all()))
        defaultSg = list(vpc.security_groups.all())[0]
        print('Defaul Security Group: {}'.format(defaultSg))
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(DB_PORT),
            ToPort=int(DB_PORT)
        )
    except Exception as e:
        print(e)


# def delete_cluster(redshift):
#     '''The the redshift cluster'''
#     try:
#         redshift.delete_cluster( ClusterIdentifier=CLUSTER_IDENTIFIER,  SkipFinalClusterSnapshot=True)
#     except Exception as e:
#         print(e)


# def delete_role(iam):
#     '''Detach the role policy and delete the IAM role'''
#     iam.detach_role_policy(RoleName=IAM_ROLE_NAME, PolicyArn=IAM_ROLE_ARN)
#     iam.delete_role(RoleName=IAM_ROLE_NAME)
#     print('Role and Role Arn deleted')


def main():
    ec2, s3, iam, redshift = create_aws_resources()
    roleArn = create_iam_role(iam)
    END_POINT, IAM_ROLE_ARN, VPC_ID = create_cluster(redshift, roleArn)
    print('Endpoint: ',END_POINT)
    print('IAM_ROLE_ARN: ',IAM_ROLE_ARN)
    print('VPC_ID: ',VPC_ID)
    open_tcp_port(VPC_ID, ec2)

if __name__ == '__main__':
    main()