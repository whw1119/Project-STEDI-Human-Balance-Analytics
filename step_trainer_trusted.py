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

# Script generated for node Amazon S3
AmazonS3_node1693837789873 = glueContext.create_dynamic_frame.from_catalog(
    database="whw",
    table_name="customers_curated",
    transformation_ctx="AmazonS3_node1693837789873",
)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="whw",
    table_name="step_trainer_landing",
    transformation_ctx="S3bucket_node1",
)

# Script generated for node Join
Join_node1693837830722 = Join.apply(
    frame1=S3bucket_node1,
    frame2=AmazonS3_node1693837789873,
    keys1=["serialnumber"],
    keys2=["serialnumber"],
    transformation_ctx="Join_node1693837830722",
)

# Script generated for node Drop Fields
DropFields_node1693837855174 = DropFields.apply(
    frame=Join_node1693837830722,
    paths=[
        "email",
        "phone",
        "birthday",
        "customername",
        "`.serialnumber`",
        "registrationdate",
        "lastupdatedate",
        "sharewithresearchasofdate",
        "sharewithpublicasofdate",
    ],
    transformation_ctx="DropFields_node1693837855174",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1693837855174,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://whw-project/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
