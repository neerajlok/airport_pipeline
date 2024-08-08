import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Reading the fact table from redshift
print('Preparing to read raw table')
fact_df = glueContext.create_dynamic_frame.from_catalog(database="airline_db", table_name="raw_data",redshift_tmp_dir="s3://temp-buck-neer", transformation_ctx="fact_df")

# Reading the dimension table from redshift
print('Preparing to read airport table')
dim_df = glueContext.create_dynamic_frame.from_catalog(database="airline_db", table_name="dev_airlines_airport", redshift_tmp_dir="s3://temp-buck-neer",transformation_ctx="dim_df")

# Performing join
join_df1 = Join.apply(frame1=dim_df, frame2=fact_df, keys1=["airport_id"], keys2=["originairportid"], transformation_ctx="join_df1")
print("first join done")

# Changing schema 
transformed_df1 = ApplyMapping.apply(frame=join_df1, mappings=[("city", "string", "depp_city", "string"), ("name", "string", "depp_name", "string"), ("state", "string", "depp_state", "string"), ("carrier", "string", "carrier", "string"), ("destairportid", "long", "destairportid", "long"), ("depdelay", "long", "depdelay", "long"), ("arrdelay", "long", "arrdelay", "long")], transformation_ctx="transformed_df1")
print('first schema change done')

# Performing join
join_df2 = Join.apply(frame1=transformed_df1, frame2=dim_df, keys1=["destairportid"], keys2=["airport_id"], transformation_ctx="join_df2")
print("second join done")

# Changing schema
output_df = ApplyMapping.apply(frame=join_df2, mappings=[("depp_name", "string", "depp_name", "string"), ("carrier", "string", "carrier", "string"), ("state", "string", "airr_state", "string"), ("arrdelay", "long", "arr_delay", "long"), ("city", "string", "airr_city", "string"), ("name", "string", "airr_name", "string"), ("depp_state", "string", "depp_state", "string"), ("depp_city", "string", "depp_city", "string"), ("depdelay", "long", "depp_delay", "long")], transformation_ctx="output_df")
print('second schema change done')

# Pushing the df to redshift table and running post action scripts 
op = glueContext.write_dynamic_frame.from_catalog(frame=output_df, database="airline_db", table_name="dev_airlines_flight_dets", redshift_tmp_dir="s3://temp-buck-neer",additional_options={"aws_iam_role": "arn:aws:iam::992382771476:role/service-role/AmazonRedshift-CommandsAccessRole-20240701T204437","postactions": "REFRESH MATERIALIZED VIEW airlines.avg_delays_by_airline;REFRESH MATERIALIZED VIEW airlines.high_delays;REFRESH MATERIALIZED VIEW airlines.delays_by_state;REFRESH MATERIALIZED VIEW airlines.most_frequent_cities;REFRESH MATERIALIZED VIEW airlines.on_time_performance;REFRESH MATERIALIZED VIEW airlines.flight_count_by_airport;"}, transformation_ctx="op")
print('data pushed to redshift')

job.commit()