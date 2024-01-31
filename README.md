# pinterest-data-pipeline201

# Pinterest Data Pipeline Project

## Table of Contents

1. [Description](#description)
2. [Installation](#installation)
3. [Usage](#usage)
4. [File Structure](#file-structure)

## Description

The Pinterest Data Pipeline project involves setting up a Kafka data pipeline on AWS using MSK (Managed Streaming for Apache Kafka). The goal is to ingest, process, and analyse Pinterest data in real-time. This README file documents the installation process, usage instructions, and provides insights into the project structure.

## Installation

### Step 1: installing Pinterest infrastructure

you need to get your hands on some infrastructure similar to that which you'd find if you were a data engineer working at Pinterest. [Download the zip package from this link] https://aicore-portal-public-prod-307050600709.s3.eu-west-1.amazonaws.com/project-files/eec4e4d1-56ca-4ce9-aa4b-bedb3c84f31f/user_posting_emulation.py.

Inside, you will find the `user_posting_emulation.py`, which contains login credentials for an RDS database. The RDS database contains three tables with data resembling data received by the Pinterest API when a POST request is made by a user uploading data to Pinterest:

- `pinterest_data`: Contains data about posts being updated to Pinterest
- `geolocation_data`: Contains data about the geolocation of each Pinterest post found in `pinterest_data`
- `user_data`: Contains data about the user that has uploaded each post found in `pinterest_data`

Run the provided script (`user_posting_emulation.py`) and print out `pin_result`, `geo_result`, and `user_result`. These each represent one entry in their corresponding table. Get familiar with the data, as you will be working with it throughout the project.


### Step 2: get the key pair .pem file for the EC2
 Using your KeyPairId (you can locate this information within the email containing your AWS login credentials) find the specific key pair associated with your EC2 instance. Select this key pair and under the Value field select Show.This will reveal the content of your key pair. Copy its entire value (including the BEGIN and END header) and paste it in the .pem file in VSCode.


then:
Navigate to the EC2 console and identify the instance with your unique UserId. Select this instance, and under the Details section find the Key pair name and make a note of this. Save the previously created file in the VSCode using the following format: Key pair name.pem.

### Step 3: Kafka and IAM MSK Authentication Package

- Install Kafka on your EC2 instance.
- Download and install the IAM MSK authentication package.
- Follow the steps outlined in the README to configure your Kafka client for IAM authentication.

### Step 4: Create Topics on MSK Cluster

- Retrieve Bootstrap servers string and Plaintext Apache Zookeeper connection string from MSK Management Console.
- Set up the CLASSPATH environment variable.
- Run Kafka commands to create the required topics on the MSK cluster.

## Usage

Document how to use the Pinterest Data Pipeline. Include any specific instructions for running the pipeline, monitoring, or troubleshooting.

## File Structure

Outline the structure of your project, detailing the purpose of each significant directory or file. For example:

## Custom Plugin

### Step 1: Find the S3 Bucket

1. Go to the [S3 console](https://s3.console.aws.amazon.com/).

2. Find the bucket that contains your UserId. The bucket name should have the format: `user-<your_UserId>-bucket`. Make a note of the bucket name.

### Step 2: Download and Copy Confluent.io Amazon S3 Connector

1. On your EC2 client, download the Confluent.io Amazon S3 Connector. You can typically find this connector on the Confluent Hub or the Confluent website.

   ```bash
   wget https://d1i4a15mxbxib1.cloudfront.net/api/plugins/confluentinc/kafka-connect-s3/versions/10.0.3/confluentinc-kafka-connect-s3-10.0.3.zip

2. Copy the downloaded Confluent.io Amazon S3 Connector JAR file to the S3 bucket you identified in Step 1.

    ```bash
    aws s3 cp ./confluentinc-kafka-connect-s3-10.0.3.zip s3://<BUCKET_NAME>/kafka-connect-s3/
