import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from awsglue.dynamicframe import DynamicFrame
import pyspark.sql.functions as F

# Import the data quality transform
from awsgluedq.transforms import EvaluateDataQuality

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# source dataset location
S3_location = "s3://learn-share-repeat-us-west-2/amazon-reviews-pds/product_category_watches_unpartitioned/"

# create aws glue dynamicframe using create_dynamic_frame_from_options by
# reading the source s3 location.
datasource0 = glueContext.create_dynamic_frame_from_options(
            connection_type="s3",
            connection_options = {
                "paths": [S3_location]
            },
            format="json",
            additional_options={"useS3ListImplementation": True},
            transformation_ctx="datasource0")

#print(datasource0.printSchema())
#print("--------------------------")
#print(datasource0.show(2))

# Create data quality ruleset
ruleset = """Rules = [
      ColumnExists "marketplace"
      , IsComplete "customer_id" 
      , ColumnValues "star_rating" in [ 1, 2, 3, 4, 5 ]
      , DistinctValuesCount "marketplace" >= 5
      , Completeness "review_date" > 0.95
]"""

# Evaluate data quality
dqResults = EvaluateDataQuality.apply(
    frame=datasource0,
    ruleset=ruleset,
    publishing_options={
        "dataQualityEvaluationContext": "datasource0",
        "enableDataQualityCloudWatchMetrics": True,
        "enableDataQualityResultsPublishing": True,
        "resultsS3Prefix": "s3://learn-share-repeat-us-west-2/EvaluateDataQuality/GlueDataQualityEvaluation/",
    },
)

# Inspect data quality results
dqResults.printSchema()
print("--------------------------")
dqResults.toDF().show()

# Check if the result has any failed outcome
containFailedResult = dqResults.toDF().selectExpr('case when any(Outcome == "Failed") then 1 else 0 end AS failed_outcome')

# Exit the Glue job with error message if the result has any failed outcome
if containFailedResult.select('failed_outcome').first()[0] == 1:
    sys.exit("EvaluateDataQuality: Failed Data Quality Check")
else:
    print("All Quality Checks Passed") 
    job.commit()