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

# Script generated for node accelerometer trusted
accelerometertrusted_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://dingus-dongus/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="accelerometertrusted_node1",
)

# Script generated for node trainer landing
trainerlanding_node1695125174557 = glueContext.create_dynamic_frame.from_catalog(
    database="databased",
    table_name="trainer_landing",
    transformation_ctx="trainerlanding_node1695125174557",
)

# Script generated for node Join
Join_node1695124982399 = Join.apply(
    frame1=accelerometertrusted_node1,
    frame2=trainerlanding_node1695125174557,
    keys1=["timeStamp"],
    keys2=["sensorreadingtime"],
    transformation_ctx="Join_node1695124982399",
)

# Script generated for node Drop Fields
DropFields_node1695125892332 = DropFields.apply(
    frame=Join_node1695124982399,
    paths=[],
    transformation_ctx="DropFields_node1695125892332",
)

# Script generated for node machine_learning_curated
machine_learning_curated_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1695125892332,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://dingus-dongus/step_trainer/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="machine_learning_curated_node3",
)

job.commit()
