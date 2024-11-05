import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
import re
from pyspark.sql import functions as SqlFuncs

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
def sparkAggregate(glueContext, parentFrame, groups, aggs, transformation_ctx) -> DynamicFrame:
    aggsFuncs = []
    for column, func in aggs:
        aggsFuncs.append(getattr(SqlFuncs, func)(column))
    result = parentFrame.toDF().groupBy(*groups).agg(*aggsFuncs) if len(groups) > 0 else parentFrame.toDF().agg(*aggsFuncs)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Amazon S3
AmazonS3_node1730493417762 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://ximple-test/data_engineering_challenge_dataset.csv"]}, transformation_ctx="AmazonS3_node1730493417762")

# Script generated for node step 1 - schema check
step1schemacheck_node1730494308839_ruleset = """


    Rules = [
        ColumnExists "loan_id",
        ColumnExists "application_date",
        ColumnExists "loan_amount",
        ColumnExists "interest_rate",
        ColumnExists "term_months",
        ColumnExists "customer_income",
        ColumnExists "customer_credit_score",
        ColumnExists "loan_status",
        ColumnExists "employment_length",
        ColumnDataType  "loan_id" = "INTEGER",
        ColumnDataType  "application_date" = "DATE",
        ColumnDataType  "loan_amount" = "FLOAT",
        ColumnDataType  "interest_rate" = "FLOAT",
        ColumnDataType  "term_months" = "INTEGER",
        ColumnDataType  "customer_income" = "DOUBLE",
        ColumnDataType "customer_credit_score" = "INTEGER",
        ColumnCount = 9,
        IsPrimaryKey "loan_id",
        ColumnValues "loan_status" in ["Approved","Defaulted","Pending","Rejected"],
        ColumnValues "employment_length" in ["<1 year","1-3 years","3-5 years","5-10 years", ">10 years"],
        ColumnValues "customer_credit_score" between 1 and 1000
        
    ]
"""

step1schemacheck_node1730494308839 = EvaluateDataQuality().process_rows(frame=AmazonS3_node1730493417762, ruleset=step1schemacheck_node1730494308839_ruleset, publishing_options={"dataQualityEvaluationContext": "step1schemacheck_node1730494308839", "enableDataQualityCloudWatchMetrics": True, "enableDataQualityResultsPublishing": True}, additional_options={"observations.scope":"ALL","performanceTuning.caching":"CACHE_NOTHING"})

# Script generated for node step 1 - erroneos or anomalous data check
step1erroneosoranomalousdatacheck_node1730494333223_ruleset = """
    # Example rules: Completeness "colA" between 0.4 and 0.8, ColumnCount > 10

    Analyzers = [
    ColumnValues "loan_amount",
    ColumnValues "interest_rate"
    ]
"""

step1erroneosoranomalousdatacheck_node1730494333223 = EvaluateDataQuality().process_rows(frame=AmazonS3_node1730493417762, ruleset=step1erroneosoranomalousdatacheck_node1730494333223_ruleset, publishing_options={"dataQualityEvaluationContext": "step1erroneosoranomalousdatacheck_node1730494333223", "enableDataQualityCloudWatchMetrics": True, "enableDataQualityResultsPublishing": True}, additional_options={"observations.scope":"ALL","performanceTuning.caching":"CACHE_NOTHING"})

# Script generated for node Change Schema
ChangeSchema_node1730663768034 = ApplyMapping.apply(frame=AmazonS3_node1730493417762, mappings=[("loan_id", "string", "loan_id", "bigint"), ("application_date", "string", "application_date", "date"), ("loan_amount", "string", "loan_amount", "float"), ("interest_rate", "string", "interest_rate", "float"), ("term_months", "string", "term_months", "int"), ("loan_status", "string", "loan_status", "string"), ("customer_income", "string", "customer_income", "decimal"), ("customer_credit_score", "string", "customer_credit_score", "int"), ("employment_length", "string", "employment_length", "string")], transformation_ctx="ChangeSchema_node1730663768034")

# Script generated for node ruleOutcomes
ruleOutcomes_node1730673041915 = SelectFromCollection.apply(dfc=step1schemacheck_node1730494308839, key="ruleOutcomes", transformation_ctx="ruleOutcomes_node1730673041915")

# Script generated for node ruleOutcomes
ruleOutcomes_node1730671486513 = SelectFromCollection.apply(dfc=step1erroneosoranomalousdatacheck_node1730494333223, key="ruleOutcomes", transformation_ctx="ruleOutcomes_node1730671486513")

# Script generated for node step 1 - missing values check
step1missingvaluescheck_node1730663890798_ruleset = """
    # Example rules: Completeness "colA" between 0.4 and 0.8, ColumnCount > 10
    Rules = [Completeness "loan_amount" = 1,  
    Completeness "interest_rate" = 1,
    Completeness "loan_id" = 1,
    Completeness "application_date" = 1,
    Completeness "term_months" = 1,
    Completeness "customer_income" = 1,
    Completeness "customer_credit_score" = 1
    ]
"""

step1missingvaluescheck_node1730663890798 = EvaluateDataQuality().process_rows(frame=ChangeSchema_node1730663768034, ruleset=step1missingvaluescheck_node1730663890798_ruleset, publishing_options={"dataQualityEvaluationContext": "step1missingvaluescheck_node1730663890798", "enableDataQualityCloudWatchMetrics": True, "enableDataQualityResultsPublishing": True}, additional_options={"observations.scope":"ALL","performanceTuning.caching":"CACHE_NOTHING"})

# Script generated for node originalData
originalData_node1730666370401 = SelectFromCollection.apply(dfc=step1missingvaluescheck_node1730663890798, key="originalData", transformation_ctx="originalData_node1730666370401")

# Script generated for node ruleOutcomes
ruleOutcomes_node1730671555326 = SelectFromCollection.apply(dfc=step1missingvaluescheck_node1730663890798, key="ruleOutcomes", transformation_ctx="ruleOutcomes_node1730671555326")

# Script generated for node step 2 - normalize by Z-score
SqlQuery0 = '''
select *,(term_months - mean(term_months) over()) / std(term_months) over () as Z_score 
from myDataSource
'''
step2normalizebyZscore_node1730666392226 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":originalData_node1730666370401}, transformation_ctx = "step2normalizebyZscore_node1730666392226")

# Script generated for node Step 3 - transform
SqlQuery1 = '''
select *,
(POWER(customer_credit_score, 3) + EXP(customer_credit_score)) as ccs_transformed
from myDataSource
'''
Step3transform_node1730672636821 = sparkSqlQuery(glueContext, query = SqlQuery1, mapping = {"myDataSource":step2normalizebyZscore_node1730666392226}, transformation_ctx = "Step3transform_node1730672636821")

# Script generated for node step 2 - Filter
step2Filter_node1730667657631 = Filter.apply(frame=step2normalizebyZscore_node1730666392226, f=lambda row: (bool(re.match("Pending", row["loan_status"]))), transformation_ctx="step2Filter_node1730667657631")

# Script generated for node step 3 - Aggregate
step3Aggregate_node1730674131160 = sparkAggregate(glueContext, parentFrame = Step3transform_node1730672636821, groups = ["loan_status"], aggs = [["loan_id", "countDistinct"]], transformation_ctx = "step3Aggregate_node1730674131160")

# Script generated for node step 2 - group date
step2groupdate_node1730672408536 = sparkAggregate(glueContext, parentFrame = step2Filter_node1730667657631, groups = ["application_date"], aggs = [["loan_id", "count"]], transformation_ctx = "step2groupdate_node1730672408536")

# Script generated for node load to S3 - decd_converted_ds
loadtoS3decd_converted_ds_node1730674462093 = glueContext.getSink(path="s3://ximple-challenge/og_transformed/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="loadtoS3decd_converted_ds_node1730674462093")
loadtoS3decd_converted_ds_node1730674462093.setCatalogInfo(catalogDatabase="ximple-challenge-db",catalogTableName="og_transformed0001")
loadtoS3decd_converted_ds_node1730674462093.setFormat("glueparquet", compression="snappy")
loadtoS3decd_converted_ds_node1730674462093.writeFrame(ChangeSchema_node1730663768034)
# Script generated for node Amazon S3
AmazonS3_node1730678357279 = glueContext.getSink(path="s3://ximple-challenge/final_transformed_normalized/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1730678357279")
AmazonS3_node1730678357279.setCatalogInfo(catalogDatabase="ximple-challenge-db",catalogTableName="final_transformed_normalized0001")
AmazonS3_node1730678357279.setFormat("glueparquet", compression="snappy")
AmazonS3_node1730678357279.writeFrame(Step3transform_node1730672636821)
job.commit()
