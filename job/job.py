import pyspark
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, asc, desc
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
from awsglue.context import GlueContext
from faker import Faker



import os
import sys

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Cloud Formation Template Parameters
args = getResolvedOptions(sys.argv,
                          ['base_s3_path','s3_iceberg_path', 'fake_row_count'])

# Simulation of Delta Lake Layers
base_s3_path = args['base_s3_path']

s3_iceberg_path = args['s3_iceberg_path']

# Initialize Faker for performance evaluation
fake_row_count = int(args['fake_row_count'])

fake = Faker()

fake_workers = [(
        x,
        fake.name(),
        fake.random_element(elements=('IT', 'HR', 'Sales', 'Marketing')),
        fake.random_element(elements=('CA', 'NY', 'TX', 'FL', 'IL', 'RJ')),
        fake.random_int(min=10000, max=150000),
        fake.random_int(min=18, max=60),
        fake.random_int(min=0, max=100000),
        fake.unix_time()
      ) for x in range(fake_row_count)]


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config('spark.sql.extensions','org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions') \
        .config('spark.sql.catalog.my_catalog', 'org.apache.iceberg.spark.SparkCatalog') \
        .config('spark.sql.catalog.my_catalog.warehouse', s3_iceberg_path) \
        .config('spark.sql.catalog.my_catalog.catalog-impl', "org.apache.iceberg.aws.glue.GlueCatalog") \
        .config('spark.sql.catalog.my_catalog.io-impl', 'org.apache.iceberg.aws.s3.S3FileIO') \
        .config('spark.sql.catalog.my_catalog.lock-impl',  'org.apache.iceberg.aws.glue.DynamoLockManager') \
        .config('spark.sql.catalog.my_catalog.lock.table', 'iceberg_employee_lock') \
        .getOrCreate()
    return spark


# Spark + Glue context configuration
spark = create_spark_session()
sc = spark.sparkContext
glueContext = GlueContext(sc)


table_name = "iceberg_employee"

columns= ["emp_id", "employee_name","department","state","salary","age","bonus","ts"]
emp_df = spark.createDataFrame(data = fake_workers, schema = columns)

print("DF from fake workers: \n")
print(emp_df.show(10))

employee_target_path = "{base_s3_path}/tmp/iceberg_employee".format(
    base_s3_path=base_s3_path)

emp_df.write.format("parquet").mode("overwrite").save(employee_target_path)

print("Employee Write Results:\n")
emp_df_read = spark.read.format("parquet").load(employee_target_path)
print(emp_df_read.show())

emp_df_read.createOrReplaceTempView("iceberg_employee_view")
table_name = "iceberg_employee"

# Create Iceberg Table and show its results
spark.sql(f"CREATE DATABASE IF NOT EXISTS my_catalog.iceberg_demo")

spark.sql("DROP TABLE IF EXISTS my_catalog.iceberg_demo.{table_name}".format(table_name=table_name))
existing_tables = spark.sql(f"SHOW TABLES IN my_catalog.iceberg_demo;")

df_existing_tables = existing_tables.select('tableName').rdd.flatMap(lambda x:x).collect()

if table_name not in df_existing_tables:
    print("Table iceberg_employee does not exist in Glue Catalog. Creating it now.")
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS my_catalog.iceberg_demo.iceberg_employee USING iceberg as (SELECT * from iceberg_employee_view)")

# Show Created Iceberg tables:
print("Iceberg Tables:\n")
spark.sql(f"SHOW TABLES IN my_catalog.iceberg_demo;").show()

iceberg_df = spark.sql(f"SELECT * FROM my_catalog.iceberg_demo.iceberg_employee")
print("Iceberg Table Results:\n")
print(iceberg_df.show())

# Upsert records into Iceberg table
simpleDataUpd = [
    (3, "Gabriel","Sales","RJ",81000,30,23000,827307999), \
    (7, "Paulo","Engineering","RJ",79000,53,15000,1627694678), \
  ]

columns= ["emp_id", "employee_name","department","state","salary","age","bonus","ts"]
emp_up_df = spark.createDataFrame(data = simpleDataUpd, schema = columns)


print("Employee Updates:")
print(emp_up_df.show())

emp_up_df.createOrReplaceTempView("iceberg_employee_updt")

print("Spark SQL Merge Into Iceberg Table:")

spark.sql("""MERGE INTO my_catalog.iceberg_demo.iceberg_employee 
    USING iceberg_employee_updt ON iceberg_employee.emp_id = iceberg_employee_updt.emp_id 
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT * """)

iceberg_merge_df = spark.sql(f"SELECT * FROM my_catalog.iceberg_demo.iceberg_employee")
print("Iceberg Table Results after merge:\n")
print(iceberg_merge_df.show())
