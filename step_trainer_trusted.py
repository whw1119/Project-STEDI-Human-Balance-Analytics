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
AmazonS3_node1693823775464 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://whw-project/step_trainer/landing/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1693823775464",
)

# Script generated for node Amazon S3
AmazonS3_node1693825927468 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://whw-project/customer/curated/"],
        "recurse": True,
    },
    transformation_ctx="AmazonS3_node1693825927468",
)

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1693825959667 = ApplyMapping.apply(
    frame=AmazonS3_node1693825927468,
    mappings=[
        ("serialNumber", "string", "right_serialNumber", "string"),
        ("timeStamp", "long", "right_timeStamp", "long"),
        ("birthDay", "string", "right_birthDay", "string"),
        ("shareWithPublicAsOfDate", "long", "right_shareWithPublicAsOfDate", "long"),
        (
            "shareWithResearchAsOfDate",
            "long",
            "right_shareWithResearchAsOfDate",
            "long",
        ),
        ("registrationDate", "long", "right_registrationDate", "long"),
        ("customerName", "string", "right_customerName", "string"),
        ("email", "string", "right_email", "string"),
        ("lastUpdateDate", "long", "right_lastUpdateDate", "long"),
        ("phone", "string", "right_phone", "string"),
        ("shareWithFriendsAsOfDate", "long", "right_shareWithFriendsAsOfDate", "long"),
    ],
    transformation_ctx="RenamedkeysforJoin_node1693825959667",
)

# Script generated for node Join
Join_node1693823812562 = Join.apply(
    frame1=AmazonS3_node1693823775464,
    frame2=RenamedkeysforJoin_node1693825959667,
    keys1=["serialNumber"],
    keys2=["right_serialNumber"],
    transformation_ctx="Join_node1693823812562",
)

# Script generated for node Drop Fields
DropFields_node1693823923280 = DropFields.apply(
    frame=Join_node1693823812562,
    paths=[
        "right_serialNumber",
        "right_timeStamp",
        "right_birthDay",
        "right_shareWithPublicAsOfDate",
        "right_shareWithResearchAsOfDate",
        "right_registrationDate",
        "right_customerName",
        "right_email",
        "right_lastUpdateDate",
        "right_phone",
        "right_shareWithFriendsAsOfDate",
    ],
    transformation_ctx="DropFields_node1693823923280",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.getSink(
    path="s3://whw-project/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="LOG",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="S3bucket_node3",
)
S3bucket_node3.setCatalogInfo(
    catalogDatabase="whw", catalogTableName="step_trainer_trusted"
)
S3bucket_node3.setFormat("json")
S3bucket_node3.writeFrame(DropFields_node1693823923280)
job.commit()
