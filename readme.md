# AWS Glue PySpark - Apache Iceberg Quick Start Guide

## Disclaimer:
This is a quick start guide for the Apache Iceberg with Python Spark, running on AWS Glue.

It's also specifically configured for the following Glue version:
- AWS Glue 3.0
    * Spark 3.1.1 
    * Python 3.7

Glue Configuration Reference: https://docs.aws.amazon.com/glue/latest/dg/add-job.html

Apache Hudi Reference: https://hudi.apache.org/docs/quick-start-guide/ for more information

### Prerequisites:
    - Python 3.6 or higher
    - AWS CLI - Profile named 'dev' with Administrator Access (https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-profiles.html)
    
### Folder Structure:

```
glue-iceberg-hello
├── README.md
├── cloud-formation
│   ├── command.md
│   └── GlueJobPySparkIceberg.yaml
├── jars
│   ├── command.md
│   ├── iceberg-spark3-extensions-0.12.1.jar
│   ├── url-connection-client-2.17.100.jar
│   ├── iceberg-spark3-runtime-0.12.1.jar
│   ├── bundle-2.17.100.jar
│   └── upload_jar.py

├── job
│   ├── command.md
│   └── job.py
│   └── upload_job.py
├── requirements.txt

```

### Step 0: Download the required JAR:

Download the AWS bundle-2.17.100.jar from the maven repository web page:

https://mvnrepository.com/artifact/software.amazon.awssdk/bundle/2.17.100

https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.17.100/bundle-2.17.100.jar

### Step 1: Create and activate a virtualenv:

Create a new virtual environment for the project in its root directory:

```
python3 -m venv venv
```

Activate it:

```
source venv/bin/activate
```

Run from the root directory the pip install to get boto3.

```
pip install -r requirements.txt
```

### Step 2: Create the AWS Resources:

Now, with a aws configured profile named as dev, cd into the cloud-formation folder and run the command in command.md.

As a AWS Cloud Formation exercise, read the command Parameters and how they are used on the GlueJobPySparkIceberg.yaml file to dynamically create the Glue Job and S3 Bucket.

### Step 3: Upload the Job and Jars to S3:
cd into the job folder and run the command in command.md.

cd into the jars folder and run the commands in command.md. Note: There is one command for each jar.

### Step 4: Check AWS Resources results:

Log into aws console and check the Glue Job and S3 Bucket.

On the AWS Glue console, you can run the Glue Job by clicking on the job name.

After the job is finished, you can check the Glue Data Catalog and query the new database from AWS Athena.

On AWS Athena check for the database: iceberg_demo and for the table: iceberg_employee.


