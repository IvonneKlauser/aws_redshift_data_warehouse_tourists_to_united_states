# Analysis on tourists to the United States
### Udacity Data Engineering Capstone Project

#### Project Summary
The project provides data in a data warehouse located on Redshift to report on and analyze tourist travel behavior and patterns in relation to a state's characteristics. The scope is limited to tourists/immigrants entering the United States in April 2016.

The project combines data from US immigration authories detailing when, why and how people came to the United States as well as U.S. demographics and airport codes.

The setup is aimed at giving flexibility in using the data while ensuring high quality. This way different target groups can address a variety of questions and can get value from the data. However, the focus is on providing reporting capabilities to U.S. authorities. They can e.g. report on the number of tourists who came to the U.S. and relate visitors' travel patterns to a state's characteristics.

Since the target groups and their goals are quite diverse, it is important to keep the skill threshold necessary to interact with the data low. For this reason, descriptions on all codes, e.g. travel or airport codes are provided. It saves the user time when creating a report. Additionally, Redshift is used since fewer skills are needed than when using e.g. Apache Spark. 

Since the data is saved in S3 buckets, it allows for the data to be easily scaled if the project meets the approval of the users.

Please find a detailed list of steps with rationale behind the choices in the Jupyter Notebook

#### Overview

Data is gathered and cleaned with the help of Apache Spark since it is a fast engine for large-scale data processing. The output is saved in parquet files. Data is then loaded into S3 buckets since it consitutes low-cost storage and provides the possibility to be loaded into a data lake if need be. In a last step the data is loaded into Redshift since it is optimized for OLAP workloads. Data checks are performed to ensure the ETL pipeline executed correctly. 

#### Data Model

A modified version of the start schema is used to model the data since analyses on travel data is the goal. The star schema simplifies queries and allows for fast aggregations which fit the goals of this project. There are two fact tables and five dimensional tables.

A data dictionary can be found in the folder 'data_dictionary'

![Visualization of Data Model](/DAG.png)

#### How to run the project

1. Enter an AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY of an IAM user in the dwh.cfg file which has the permissions AmazonS3FullAccess and AdministratorAccess
2. Create a redshift cluster: this can be done using the file Infrastructure_as_code_redshift_cluster.ipynb. The configuration settings are in dwh.cfg 
3. Populate ARN, DB_PASSWORD, DWH_DB_PASSWORD and HOST created in step 2 in dwh.cfg with HOST being the endpoint of the cluster
4. Populate output_data in dwh.cfg. It can be an S3 bucket but it is proposed to write parquet files to the workspace and copying the files to S3 manually since it costs less time. The S3 folders necessary for the S3 bucket are specified under S3_redshift in dwh.cfg
5. Run the cells in etl.ipynb to run the project. A more detailed list of steps on the ETL pipeline can be found in the Jupyter Notebook under step 3.2
6. Compare the output of the data check to expectations