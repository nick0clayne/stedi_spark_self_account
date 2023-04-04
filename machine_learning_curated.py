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

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1680584828936 = glueContext.create_dynamic_frame.from_catalog(
    database="tri_spark_lakehouse",
    table_name="step_trainer_trusted",
    transformation_ctx="AWSGlueDataCatalog_node1680584828936",
)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1680584830621 = glueContext.create_dynamic_frame.from_catalog(
    database="tri_spark_lakehouse",
    table_name="accelerometer_trusted",
    transformation_ctx="AWSGlueDataCatalog_node1680584830621",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = Join.apply(
    frame1=AWSGlueDataCatalog_node1680584828936,
    frame2=AWSGlueDataCatalog_node1680584830621,
    keys1=["timestamp"],
    keys2=["timestamp"],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.getSink(
    path="s3://tri-lakehouse/step_trainer/machine_learning_curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="S3bucket_node3",
)
S3bucket_node3.setCatalogInfo(
    catalogDatabase="tri_spark_lakehouse", catalogTableName="machine_learning_curated"
)
S3bucket_node3.setFormat("json")
S3bucket_node3.writeFrame(ApplyMapping_node2)
job.commit()
