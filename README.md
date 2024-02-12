# pinterest-data-pipeline201

# Pinterest Data Pipeline Project

## Table of Contents

1. [Description](#description)
2. [Instructions](#Instructions)
3. [Usage](#usage)
4. [Project Follow](#project_follow)

## Description

The Pinterest Data Pipeline project involves setting up a Kafka data pipeline on AWS using MSK (Managed Streaming for Apache Kafka). The goal is to ingest, process, and analyse Pinterest data in real-time. This README file documents the installation process, usage instructions, and provides insights into the project structure.

## Instructions

### Step 1: installing Pinterest infrastructure

you need to get your hands on some infrastructure similar to that which you'd find if you were a data engineer working at Pinterest. [Download the zip package from this link] https://aicore-portal-public-prod-307050600709.s3.eu-west-1.amazonaws.com/project-files/eec4e4d1-56ca-4ce9-aa4b-bedb3c84f31f/user_posting_emulation.py.

Inside, you will find the `user_posting_emulation.py`, which contains login credentials for an RDS database. The RDS database contains three tables with data resembling data received by the Pinterest API when a POST request is made by a user uploading data to Pinterest:

- `pinterest_data`: Contains data about posts being updated to Pinterest
- `geolocation_data`: Contains data about the geolocation of each Pinterest post found in `pinterest_data`
- `user_data`: Contains data about the user that has uploaded each post found in `pinterest_data`

Run the provided script (`user_posting_emulation.py`) and print out `pin_result`, `geo_result`, and `user_result`. These each represent one entry in their corresponding table. Get familiar with the data, as you will be working with it throughout the project.


### Step 2: get the key pair .pem file for the EC2 (milestone 3 task 2)
 Using your KeyPairId (you can locate this information within the email containing your AWS login credentials) find the specific key pair associated with your EC2 instance. Select this key pair and under the Value field select Show.This will reveal the content of your key pair. Copy its entire value (including the BEGIN and END header) and paste it in the .pem file in VSCode.


then:
Navigate to the EC2 console and identify the instance with your unique UserId. Select this instance, and under the Details section find the Key pair name and make a note of this. Save the previously created file in the VSCode using the following format: Key pair name.pem.

finally:
connect to your EC2 instance. Follow the Connect instructions (SSH client) on the EC2 console to do this.

### Step 3: Kafka and IAM MSK Authentication Package (milestone 3 task 3)

#### if you've been granted access to an IAM authenticated MSK (Managed Streaming for Apache Kafka) cluster on AWS, so you don't need to create your own. Here's what you need to do to connect to it:

- Install Kafka on your EC2 client machine, ensuring it's the same version as the cluster (2.12-2.8.1).

- Install the IAM MSK authentication package on your EC2 client machine.

- Configure IAM for cluster authentication by navigating to the IAM console, selecting the appropriate role (<your_UserId>-ec2-access-role), copying its ARN, and modifying its trust policy to allow IAM roles as the principal type.

- Step 4: Modify the client.properties file in your Kafka installation's bin directory to enable AWS IAM authentication for the cluster.


### Step 4: Create Topics on MSK Cluster (milestone 3 task 4)

#### Step 1:
- Retrieve Bootstrap servers string and Plaintext Apache Zookeeper connection string from MSK Management Console.
- Set up the CLASSPATH environment variable.
- Run Kafka commands to create the required topics on the MSK cluster.

#### Step 2: 
Create three topics using specific naming conventions: <your_UserId>.pin for Pinterest posts data, <your_UserId>.geo for post geolocation data, and <your_UserId>.user for post user data. Set your CLASSPATH environment variable correctly and replace the BootstrapServerString in the Kafka command with the value obtained in Step 1. Only create topics with the exact specified names to avoid permission errors.

### Step 5: Create your custom plugin in the MSK Connect cluster

1. Locate the S3 bucket named user-<your_UserId>-bucket in the S3 console and note its name for later use.

2. Download the Confluent.io Amazon S3 Connector on your EC2 client and transfer it to the identified S3 bucket. You can use this code to do so:
sudo -u ec2-user -i
mkdir kafka-connect-s3 && cd kafka-connect-s3
wget https://d1i4a15mxbxib1.cloudfront.net/api/plugins/confluentinc/kafka-connect-s3/versions/10.0.3/confluentinc-kafka-connect-s3-10.0.3.zip
aws s3 cp ./confluentinc-kafka-connect-s3-10.0.3.zip s3://<BUCKET_NAME>/kafka-connect-s3/

#### add this to the Connector configuration settings:
connector.class=io.confluent.connect.s3.S3SinkConnector

s3.region=us-east-1
flush.size=1
schema.compatibility=NONE
tasks.max=3
topics.regex=<YOUR_UUID>.*
format.class=io.confluent.connect.s3.format.json.JsonFormat
partitioner.class=io.confluent.connect.storage.partitioner.DefaultPartitioner
value.converter.schemas.enable=false
value.converter=org.apache.kafka.connect.json.JsonConverter
storage.class=io.confluent.connect.s3.storage.S3Storage
key.converter=org.apache.kafka.connect.storage.StringConverter
s3.bucket.name=<BUCKET_NAME>


3. Create your custom plugin in the MSK Connect console.


### Step 5: Batch processing: Configuration of an API Gateway:

- Create a resource enabling PROXY integration for the API, which should share the same name as your UserId.

- Within the created resource, establish a HTTP ANY method. Ensure the Endpoint URL reflects the correct PublicDNS from the EC2 instance, named after your UserId, used in prior milestones.

- Deploy the API and record the Invoke URL for future use.

#### To set up the Kafka REST Proxy on your EC2 client machine:

- Install the Confluent package for Kafka REST Proxy on your EC2 client machine. we can use the following commands:
sudo wget https://packages.confluent.io/archive/7.2/confluent-7.2.0.tar.gz
tar -xvzf confluent-7.2.0.tar.gz 

- Modify the kafka-rest.properties file to allow the REST proxy to perform IAM authentication to the MSK cluster. the file will have the following format:

client.security.protocol = SASL_SSL
client.sasl.mechanism = AWS_MSK_IAM
client.sasl.jaas.config = software.amazon.msk.auth.iam.IAMLoginModule required awsRoleArn="Your Access Role";
client.sasl.client.callback.handler.class = software.amazon.msk.auth.iam.IAMClientCallbackHandler

- Start the REST proxy on the EC2 client machine. navigate to the confluent-7.2.0/bin then run:
./kafka-rest-start /home/ec2-user/confluent-7.2.0/etc/kafka-rest/kafka-rest.properties

#### Send data to the API:

- Modify the user_posting_emulation.py script to transmit data to your Kafka topics via your API Invoke URL. Ensure data from the three tables is sent to their respective Kafka topics in the file user_posting_emulation.py.

- Verify that data is being sent to the cluster by running a Kafka consumer (one per topic). Successful setup should result in observable message consumption.

- Confirm if data is being stored in the S3 bucket. Observe the folder structure (e.g., topics/<your_UserId>.pin/partition=0/) created by your connector in the bucket.

### Step 6: Batch Processing in Databricks:
#### To mount the S3 bucket to Databricks and read data into DataFrames:

1. Mount the S3 bucket:
   - As the Databricks account already has full access to S3, you don't need to create new access keys.
   - Mount the desired S3 bucket to the Databricks account.

2. Read data from the Delta table:
   - Read data from the Delta table located at dbfs:/user/hive/warehouse/authentication_credentials.

3. Read JSONs from S3:
   - Include the complete path to the JSON objects in the S3 bucket (e.g., topics/<your_UserId>.pin/partition=0/).
   - Create three different DataFrames:
     - df_pin for Pinterest post data.
     - df_geo for geolocation data.
     - df_user for user data.


### Step 7: Batch Processing: Spark on Databricks:

#### in the file Spark_on_databricks.ipynb there is the list of commands in spark, you can take it to Databricks and try oit out.
## Spark Queries

#### Query 1
Find the most popular Pinterest category people post to based on their country.

#### Query 2
Find how many posts each category had between 2018 and 2022.

#### Query 3
Find the user with the most followers in each country.

#### Query 4
Find the country with the user with the most followers.

#### Query 5
Find the most popular category for different age groups.

#### Query 6
Find the median follower count for different age groups.

#### Query 7
Find how many users have joined each year.

#### Query 8
Find the median follower count of users have joined between 2015 and 2020.

#### Query 9
Find the most popular category for different age groups.

#### Query 10
Find the median follower count for different age groups.

#### Query 11
Find the median follower count of users that have joined between 2015 and 2020, based on which age group they are part of.





## Project Follow:

pinterest_data_pipeline/
│
├── kafka_setup/
│ ├── install_kafka.sh
│ ├── configure_iam_auth.sh
│ └── install confulunet
│
├── msk_connect/
│ ├── create_custom_plugin.sh
│ ├── create_connector.sh
│ └── connect to S3
│
├── databricks/
│ ├── get the data
│ ├── clean_data.py
│ └── analyze_data.py as needed
│
├── airflow_dag/
│ ├── databricks_dag.py
│ └── create the Dag
│
└── AWS_kinesis
  ├── Creating data streams
  ├── configuring API by creating all resorces
  ├── send stream
  └── read stream



