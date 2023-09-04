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
AmazonS3_node1693834541823 = glueContext.create_dynamic_frame.from_catalog(
    database="whw",
    table_name="step_trainer_landing",
    transformation_ctx="AmazonS3_node1693834541823",
)

# Script generated for node Amazon S3
AmazonS3_node1693834553665 = glueContext.create_dynamic_frame.from_catalog(
    database="whw",
    table_name="customers_curated",
    transformation_ctx="AmazonS3_node1693834553665",
)

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1693834586922 = ApplyMapping.apply(
    frame=AmazonS3_node1693834553665,
    mappings=[
        ("customername", "string", "right_customername", "string"),
        ("email", "string", "right_email", "string"),
        ("phone", "string", "right_phone", "string"),
        ("birthday", "string", "right_birthday", "string"),
        ("serialnumber", "string", "right_serialnumber", "string"),
        ("registrationdate", "long", "right_registrationdate", "long"),
        ("lastupdatedate", "long", "right_lastupdatedate", "long"),
        (
            "sharewithresearchasofdate",
            "long",
            "right_sharewithresearchasofdate",
            "long",
        ),
        ("sharewithpublicasofdate", "long", "right_sharewithpublicasofdate", "long"),
    ],
    transformation_ctx="RenamedkeysforJoin_node1693834586922",
)

# Script generated for node Join
Join_node1693834386712 = Join.apply(
    frame1=AmazonS3_node1693834541823,
    frame2=RenamedkeysforJoin_node1693834586922,
    keys1=["serialnumber"],
    keys2=["right_serialnumber"],
    transformation_ctx="Join_node1693834386712",
)

# Script generated for node Drop Fields
DropFields_node1693834497401 = DropFields.apply(
    frame=Join_node1693834386712,
    paths=[
        "right_customername",
        "right_email",
        "right_phone",
        "right_birthday",
        "right_serialnumber",
        "right_registrationdate",
        "right_lastupdatedate",
        "right_sharewithresearchasofdate",
        "right_sharewithpublicasofdate",
    ],
    transformation_ctx="DropFields_node1693834497401",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.getSink(
    path="s3://whw-project/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="S3bucket_node3",
)
S3bucket_node3.setCatalogInfo(
    catalogDatabase="whw", catalogTableName="step_trainer_trusted"
)
S3bucket_node3.setFormat("json")
S3bucket_node3.writeFrame(DropFields_node1693834497401)
job.commit()
