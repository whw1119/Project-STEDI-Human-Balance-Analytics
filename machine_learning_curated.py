import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="whw",
    table_name="step_trainer_trusted",
    transformation_ctx="S3bucket_node1",
)

# Script generated for node Amazon S3
AmazonS3_node1693829179375 = glueContext.create_dynamic_frame.from_catalog(
    database="whw",
    table_name="accelerometer_trusted",
    transformation_ctx="AmazonS3_node1693829179375",
)

# Script generated for node Join
Join_node1693829199716 = Join.apply(
    frame1=S3bucket_node1,
    frame2=AmazonS3_node1693829179375,
    keys1=["sensorreadingtime"],
    keys2=["timestamp"],
    transformation_ctx="Join_node1693829199716",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=Join_node1693829199716,
    connection_type="s3",
    format="json",
    connection_options={"path": "s3://whw-project/aggregated/", "partitionKeys": []},
    transformation_ctx="S3bucket_node3",
)

job.commit()
