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

# Script generated for node Accelerometer Landing Node
AccelerometerLandingNode_node1 = glueContext.create_dynamic_frame.from_catalog(
    database="databased",
    table_name="accelerometer_landing",
    transformation_ctx="AccelerometerLandingNode_node1",
)

# Script generated for node Customer Trusted Zone
CustomerTrustedZone_node1695125174557 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://dingus-dongus/customer/trusted/run-1695122630085-part-r-00000"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrustedZone_node1695125174557",
)

# Script generated for node Join
Join_node1695124982399 = Join.apply(
    frame1=AccelerometerLandingNode_node1,
    frame2=CustomerTrustedZone_node1695125174557,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1695124982399",
)

# Script generated for node Drop Fields
DropFields_node1695125892332 = DropFields.apply(
    frame=Join_node1695124982399,
    paths=["user", "timestamp", "x", "y", "z"],
    transformation_ctx="DropFields_node1695125892332",
)

# Script generated for node Accelerometer Trusted Node
AccelerometerTrustedNode_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1695125892332,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://dingus-dongus/customer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="AccelerometerTrustedNode_node3",
)

job.commit()
