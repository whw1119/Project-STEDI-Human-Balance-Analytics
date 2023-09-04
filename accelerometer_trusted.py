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
    table_name="accelerometer_landing",
    transformation_ctx="S3bucket_node1",
)

# Script generated for node Amazon S3
AmazonS3_node1693820781325 = glueContext.create_dynamic_frame.from_catalog(
    database="whw",
    table_name="customer_trusted",
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
        "customername",
        "sharewithresearchasofdate",
        "registrationdate",
        "birthday",
        "sharewithpublicasofdate",
        "serialnumber",
        "lastupdatedate",
    ],
    transformation_ctx="DropFields_node1693821011329",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.getSink(
    path="s3://whw-project/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="S3bucket_node3",
)
S3bucket_node3.setCatalogInfo(
    catalogDatabase="whw", catalogTableName="accelerometer_trusted"
)
S3bucket_node3.setFormat("json")
S3bucket_node3.writeFrame(DropFields_node1693821011329)
job.commit()
