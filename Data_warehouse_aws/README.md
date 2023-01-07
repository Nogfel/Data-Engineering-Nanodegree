# Data Warehouse Creation on S3
This project consists on creating a Data Warehouse on Amazon Redshift based on metadata collected on songs log files data. In order to do that a Redshift Cluster is created using Python SDK and the data is ingested and prepared using Python and SQL. The used in this project is collected from a JSON file and stored in a stage table. From there, we insert the data into a star schema composed of fact and dimension tables. At the end the data inserted will be queryed to show that all the steps were executed successfully and the cluster will be deleted.

## Creating AWS resources
Since the data will be hosted in a AWS we need to create the environment at the AWS cloud. In order to do that 

## *** COLOCAR NA DOCUMENTAÇÃO ***
- Precisei alterar a porta de 5439 para 5440 por causa da mensagem de erro abaixo:<br>
```
An error occurred (InvalidPermission.Duplicate) when calling the AuthorizeSecurityGroupIngress operation: the specified rule "peer: 0.0.0.0/0, TCP, from port: 5439, to port: 5439, ALLOW" already exists
```
Após esse ajuste eu consegui criar o cluster de boa.
Referência: https://knowledge.udacity.com/questions/447189

TODA VEZ QUE RODAR O CÓDIGO DELETAR O CLUSTER NO REDSHIFT NA MÃO E APAGAR O ROLE sparkfyRole em IAM > Role e digitar "sparkfyRole" que vai aparecer.

*********************************************************
05/01/2026: problema agora está na execução do `etl.py`
Antes de encerrar eu verifiquei e não havia nenhum cluster rodando
*********************************************************


Sequencia de execução:
- `create_cluster_aws.py`;
- `create_tables.py`;
- `etl.py`


REPOSITÓRIOS DO GITHUB QUE ESTÃO ME AJUDANDO:
- https://github.com/ulmefors/udacity-nd027-data-warehouse
- https://github.com/jazracherif/udacity-data-engineer-dwh