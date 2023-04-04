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

# Script generated for node step_trainer_landing
step_trainer_landing_node1680574149080 = glueContext.create_dynamic_frame.from_catalog(
    database="tri_spark_lakehouse",
    table_name="step_trainer_landing",
    transformation_ctx="step_trainer_landing_node1680574149080",
)

# Script generated for node customer_curated
customer_curated_node1680574150421 = glueContext.create_dynamic_frame.from_catalog(
    database="tri_spark_lakehouse",
    table_name="customer_curated",
    transformation_ctx="customer_curated_node1680574150421",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = Join.apply(
    frame1=step_trainer_landing_node1680574149080,
    frame2=customer_curated_node1680574150421,
    keys1=["serialnumber"],
    keys2=["serialnumber"],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node Drop Fields
DropFields_node1680562106780 = DropFields.apply(
    frame=ApplyMapping_node2,
    paths=[
        "customername",
        "sharewithfriendsasofdate",
        "phone",
        "birthday",
        "email",
        "registrationdate",
        "lastupdatedate",
        "sharewithpublicasofdate",
        "sharewithresearchasofdate",
    ],
    transformation_ctx="DropFields_node1680562106780",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.getSink(
    path="s3://tri-lakehouse/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="S3bucket_node3",
)
S3bucket_node3.setCatalogInfo(
    catalogDatabase="tri_spark_lakehouse", catalogTableName="step_trainer_trusted"
)
S3bucket_node3.setFormat("json")
S3bucket_node3.writeFrame(DropFields_node1680562106780)
job.commit()
