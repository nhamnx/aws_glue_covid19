import sys
from pyspark.context import SparkContext
from pyspark.sql.functions import col, to_date, regexp_replace, when, count, to_date, min, sum
import pyspark.sql.functions as F
from pyspark.sql.types import StructField, StringType, IntegerType, DoubleType, DateType, StructType
from awsglue.context import GlueContext
from awsglue.transforms import *
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions
from awsglue.job import Job


sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

confirmed_input_loc = "s3://covid19-confirmed/time_series_covid19_confirmed_global.csv"
deaths_input_loc = "s3://covid19-deaths/time_series_covid19_deaths_global.csv"
recovered_input_loc = "s3://covid19-recovered/time_series_covid19_recovered_global.csv"

parquet_output_loc = "s3://covid19-recovered/covid19-cleaned-ouput/"


confirmed = glueContext.create_dynamic_frame_from_options(\
    connection_type = "s3", \
    connection_options = { 
        "paths": [confirmed_input_loc]}, \
    format = "csv",
    format_options={
        "withHeader": True,
        "separator": ","
    })

deaths = glueContext.create_dynamic_frame_from_options(\
    connection_type = "s3", \
    connection_options = { 
        "paths": [deaths_input_loc]}, \
    format = "csv",
    format_options={
        "withHeader": True,
        "separator": ","
    })

recovered = glueContext.create_dynamic_frame_from_options(\
    connection_type = "s3", \
    connection_options = { 
        "paths": [recovered_input_loc]}, \
    format = "csv",
    format_options={
        "withHeader": True,
        "separator": ","
    })
confirmed_df = confirmed.toDF()
deaths_df = deaths.toDF()
recovered_df = recovered.toDF()

confirmed_df = confirmed_df.withColumnRenamed("Province/State", "province_state")\
                                    .withColumnRenamed("Country/Region", "country_region")\
                                    .withColumnRenamed("Lat", "lat")\
                                    .withColumnRenamed("long", "long")
deaths_df = deaths_df.withColumnRenamed("Province/State", "province_state")\
                                    .withColumnRenamed("Country/Region", "country_region")\
                                    .withColumnRenamed("Lat", "lat")\
                                    .withColumnRenamed("long", "long")
recovered_df = recovered_df.withColumnRenamed("Province/State", "province_state")\
                                    .withColumnRenamed("Country/Region", "country_region")\
                                    .withColumnRenamed("Lat", "lat")\
                                    .withColumnRenamed("long", "long")  
dates = confirmed_df.columns[4:]

confirmed_df_long = confirmed_df.to_koalas()\
    .melt(id_vars=['province_state', 'country_region', 'lat', 'long'], value_vars=dates, var_name='date', value_name='confirmed')\
    .to_spark()
deaths_df_long  = deaths_df.to_koalas()\
    .melt(id_vars=['province_state', 'country_region', 'lat', 'long'], value_vars=dates, var_name='date', value_name='deaths')\
    .to_spark()
recovered_df_long  = recovered_df.to_koalas()\
    .melt(id_vars=['province_state', 'country_region', 'lat', 'long'], value_vars=dates, var_name='date', value_name='recovered')\
    .to_spark()

full_table = confirmed_df_long.join(
  deaths_df_long, 
  (confirmed_df_long['province_state'].eqNullSafe(deaths_df_long['province_state'])) &\
  (confirmed_df_long['country_region'].eqNullSafe(deaths_df_long['country_region'])) &\
  (confirmed_df_long['date'].eqNullSafe(deaths_df_long['date'])) & \
  (confirmed_df_long['lat'].eqNullSafe(deaths_df_long['lat'])) & \
  (confirmed_df_long['long'].eqNullSafe(deaths_df_long['long'])),
  'left'
).select(confirmed_df_long['*'], deaths_df_long['deaths'])

full_table = full_table.join(
  recovered_df_long, 
  (full_table['province_state'].eqNullSafe(recovered_df_long['province_state'])) &\
  (full_table['country_region'].eqNullSafe(recovered_df_long['country_region'])) &\
  (full_table['date'].eqNullSafe(recovered_df_long['date'])) &\
  (full_table['lat'].eqNullSafe(recovered_df_long['lat'])) & \
  (full_table['long'].eqNullSafe(recovered_df_long['long'])),
  'left'
).select(full_table['*'], recovered_df_long['recovered'])

full_table = full_table.withColumn('date', to_date(col('date'),'m/d/yy'))

full_table = full_table.filter('lat is NOT NULL or long is NOT NULL')               

full_table = full_table.fillna({'recovered':'0'})

full_table = full_table.withColumn('recovered', regexp_replace('recovered', '-1', '0'))

full_table = full_table.filter(~(full_table.province_state.eqNullSafe('Grand Princess')) | \
                        ~(full_table.province_state.eqNullSafe('Diamond Princess')))
full_table = full_table.filter((full_table.country_region != 'Diamond Princess') | (full_table.country_region != 'MS Zaandam'))

full_table = full_table \
  .withColumn("province_state" ,
              full_table["province_state"]
              .cast(StringType())) \
  .withColumn("country_region",
              full_table["country_region"]
              .cast(StringType())) \
  .withColumn("lat"  ,
              full_table["lat"]
              .cast(DoubleType())) \
  .withColumn("long"  ,
            full_table["long"]
            .cast(DoubleType())) \
  .withColumn("date"  ,
            full_table["date"]
            .cast(DateType())) \
  .withColumn("confirmed"  ,
            full_table["confirmed"]
            .cast(IntegerType())) \
  .withColumn("deaths"  ,
            full_table["deaths"]
            .cast(IntegerType())) \
  .withColumn("recovered"  ,
            full_table["recovered"]
            .cast(IntegerType())) \
                        
full_table = full_table.withColumn("active",col("confirmed") - col("deaths") - col('recovered'))

group_cols = ["country_region", "date"]
country_level = full_table.groupBy(group_cols)\
    .agg(sum("confirmed").alias("total_confirmed"), \
         sum("deaths").alias("total_deaths"), \
         sum("recovered").alias("total_recovered"), \
         sum("active").alias("total_active"))\
    .orderBy(['date', 'country_region'])

full_table_frame = DynamicFrame.fromDF(
    dataframe=full_table,
    glue_ctx=glueContext,
)

country_level_frame = DynamicFrame.fromDF(
    dataframe=country_level,
    glue_ctx=glueContext,
)

output_fulltable = glueContext.write_dynamic_frame.from_options(\
    frame = full_table_frame, \
    connection_type = "s3", \
    connection_options = {"path": parquet_output_loc+'covid_19_clean' \
        }, format = "parquet")   

output_country_level = glueContext.write_dynamic_frame.from_options(\
    frame = full_table_frame, \
    connection_type = "s3", \
    connection_options = {"path": parquet_output_loc+'country_level' \
        }, format = "parquet")  
        
job.commit()