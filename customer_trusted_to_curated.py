import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
from pyspark.sql import functions as SqlFuncs


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="tri_spark_lakehouse",
    table_name="accelerometer_landing",
    transformation_ctx="S3bucket_node1",
)

# Script generated for node Amazon S3
AmazonS3_node1680559962479 = glueContext.create_dynamic_frame.from_catalog(
    database="tri_spark_lakehouse",
    table_name="customer_trusted",
    transformation_ctx="AmazonS3_node1680559962479",
)

# Script generated for node ApplyMapping
ApplyMapping_node2 = Join.apply(
    frame1=S3bucket_node1,
    frame2=AmazonS3_node1680559962479,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node SQL Query
SqlQuery0 = """
select * from myDataSource
where timestamp >= shareWithResearchAsOfDate
"""
SQLQuery_node1680560612941 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={"myDataSource": ApplyMapping_node2},
    transformation_ctx="SQLQuery_node1680560612941",
)

# Script generated for node Drop Fields
DropFields_node1680560073711 = DropFields.apply(
    frame=SQLQuery_node1680560612941,
    paths=["user", "timestamp", "x", "y", "z"],
    transformation_ctx="DropFields_node1680560073711",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1680561345636 = DynamicFrame.fromDF(
    DropFields_node1680560073711.toDF().dropDuplicates(["email"]),
    glueContext,
    "DropDuplicates_node1680561345636",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropDuplicates_node1680561345636,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://tri-lakehouse/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
