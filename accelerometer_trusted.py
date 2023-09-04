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
S3bucket_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://whw-project/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="S3bucket_node1",
)

# Script generated for node Amazon S3
AmazonS3_node1693820781325 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://whw-project/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1693820781325",
)

# Script generated for node Join
Join_node1693820801158 = Join.apply(
    frame1=AmazonS3_node1693820781325,
    frame2=S3bucket_node1,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="Join_node1693820801158",
)

# Script generated for node Drop Fields
DropFields_node1693821011329 = DropFields.apply(
    frame=Join_node1693820801158,
    paths=[
        "phone",
        "email",
        "serialNumber",
        "shareWithPublicAsOfDate",
        "birthDay",
        "registrationDate",
        "shareWithResearchAsOfDate",
        "customerName",
        "lastUpdateDate",
        "shareWithFriendsAsOfDate",
    ],
    transformation_ctx="DropFields_node1693821011329",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1693821011329,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://whw-project/accelerometer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
