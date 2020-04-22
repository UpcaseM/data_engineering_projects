#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr 19 10:10:00 2020

@author: UpcaseM

This is the etl pipline to process the raw data into dimential tables.
"""

import os
import logging
import configparser

config = configparser.ConfigParser()
cfg_path = os.path.dirname(os.path.abspath(__file__))
config.read_file(open(cfg_path + '/dl.cfg'))

# Logging
logger = logging.getLogger()
fhandler = logging.FileHandler(filename='spark_app.log', mode='w+')
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fhandler.setFormatter(formatter)
logger.addHandler(fhandler)
logger.setLevel(logging.INFO)

os.environ['REGION']=config['AWS']['REGION']
os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['ACCESS_KEY']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['SECRET_KEY']


from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Window


def create_spark_session():
    '''
    Create or get a Spark Session.

    Returns
    -------
    spark : spark session

    '''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages","org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    logger.info('spark session is created.')
    return spark

def process_weather_data(spark, input_data, output_data):
    '''
    Import data from input_data, change data type and transform the data. 
    Save the dimentional tables to output_data as parquet format.
    
    Parameters:
        spark       : Spark Session.
        input_data  : Path of the input data.
        output_data : Path of the ouput data.
    
    Return nothing.
    
    '''
    logger.info('Importing weather data...')
    df_weather = spark.read.csv(input_data + '/weather-data/*.csv', 
                                header = True)
    logger.info('Weather data is imported')
    
    # Change data type
    df_weather = df_weather \
        .drop('_c0') \
        .dropDuplicates() \
        .withColumn('date', F.date_format(F.col('date'),'yyyy-MM-dd').cast('date')) \
        .withColumn('hour', F.col('hour').cast('integer')) \
        .withColumn('temp', F.col('temp').cast('double')) \
        .withColumn('dew_point_f', F.col('dew_point_f').cast('double')) \
        .withColumn('humidity%', F.col('humidity%').cast('double')) \
        .withColumn('pressure', F.col('pressure').cast('double')) \
        .withColumn('precip_hrly', F.col('precip_hrly').cast('double')) \
        .withColumn('feels_like', F.col('feels_like').cast('double')) \
        .withColumn('rn', F.row_number().over(Window.partitionBy(['station', 'date', 'hour']).orderBy('station_name'))) \
        .filter(F.col('rn')==1) \
        .drop('rn') \
        .withColumn('temp_c', F.lit('ABC')) \
        .withColumn('id', F.row_number().over(Window.partitionBy('temp_c').orderBy(F.lit('A')))) \
        .drop('temp_c')
    
    logger.info('Running data quality check...')
    data_quality_check(df_weather,
                       ['id'],
                       ['id','station'])
    #Save it to output_data
    df_weather \
        .write \
        .mode('overwrite') \
        .partitionBy('station') \
        .parquet(output_data + '/weather')
    logger.info('[Success]: Table weather is created!')
    
def process_business_cat_data(spark, input_data, output_data):
    '''
    Import data from input_data, change data type and transform the data. 
    Save the dimentional tables to output_data as parquet format.
    
    Parameters:
        spark       : Spark Session.
        input_data  : Path of the input data.
        output_data : Path of the ouput data.
    
    Return nothing.
    
    '''
    logger.info('Importing business, category and geo data...')
    #Import bussiness data, category data and geo location data.
    df_business = spark.read.json(input_data + '/yelp-dataset/yelp_academic_dataset_business.json')
    df_category = spark.read.csv(input_data + '/yelp-dataset/yelp_category.csv', header = True)
    df_geo = spark.read.csv(input_data + '/yelp-dataset/geo_location.csv', header = True)
    logger.info('Data is imported...')
    
# =============================================================================
#     Create category table
# =============================================================================
    # change data type and rename index
    df_category = df_category \
        .withColumn('index', F.col('_c0').cast('integer')) \
        .drop('_c0')
    
    
    logger.info('Running data quality check...')
    data_quality_check(df_category)
    #Save df_category to output_data
    df_category \
        .write \
        .mode('overwrite') \
        .parquet(output_data + '/category')
    logger.info('[Success]: Table category is created!')
    
# =============================================================================
#     Create df_business_category_link
# =============================================================================
    # Create table for category level 
    w = Window.partitionBy('subcategories').orderBy('category')
    df_cat=df_category.select('subcategories', 'category') \
        .dropna(how = 'any') \
        .withColumn('rn', F.row_number().over(w)) \
        .filter(F.col('rn')==1) \
        .drop('rn')
    df_subcat = df_category.select(df_category['subcategories'].alias('category')) \
        .dropDuplicates() \
        .withColumn('level', F.lit(2)) \
        .select('category', 'level')
    
    # Create category table by business_id
    df_business_cat = df_business \
        .select('business_id',F.col('categories').alias('category'), 'state') \
        .withColumn('category', F.split('category', ', ')) \
        .withColumn('category', F.explode('category'))
    
    # Filter out all rows except ones with level 2 (subcategories).
    df_business_cat = df_business_cat \
        .join(df_subcat, on='category', how='left_outer') \
        .filter(F.col('level'). isNotNull())
    
    # Look up the first main category based on each subcategory
    df_business_cat = df_business_cat.select('business_id', 
                                             F.col('category').alias('subcategories'),
                                             'state') \
        .join(df_cat, on= 'subcategories', how='left_outer') \
        .select('business_id', 'category', 'subcategories', 'state')
    
    
    # Create join table for table business and table category.
    df_business_category_link = df_business_cat \
        .join(df_category, on =['category', 'subcategories'], how = 'left_outer') \
        .select('business_id', 'index', 'state') \
        .withColumn('state', F.when(F.col('state') == '', None).otherwise(F.col('state'))) \
        .withColumn('state', F.when(F.col('state').isNull(), 'OTHER').otherwise(F.col('state')))
    
    logger.info('Running data quality check...')
    data_quality_check(df_business_category_link)
    #Save df_category to output_data
    df_business_category_link \
        .write \
        .mode('overwrite') \
        .partitionBy('state') \
        .parquet(output_data + '/business_category_link')
    logger.info('[Success]: Table business_category_link is created!')
    
# =============================================================================
#     Create df_business
# =============================================================================
    
    # Change data type for df_geo
    df_geo = df_geo \
        .withColumn('latit_s', F.col('latit_s').cast('double')) \
        .withColumn('longi_s', F.col('longi_s').cast('double'))

    # Since there are too many missing values in attributes columns we will drop all these columns
    df_business = df_business.drop(*(c[0] for c in df_business.dtypes if ('attributes' in c[0])))
    # Convert is_open to boolean
    df_business = df_business \
        .withColumn('is_open',df_business['is_open'].cast(T.BooleanType())) \
        .withColumn('state', F.when(F.col('state') == '', None).otherwise(F.col('state'))) \
        .withColumn('state', F.when(F.col('state').isNull(), 'OTHER').otherwise(F.col('state'))) \
        .withColumn('latit_s', F.round(F.col('latitude'),1)) \
        .withColumn('longi_s', F.round(F.col('longitude'),1)) \
        .join(df_geo, on=['latit_s', 'longi_s'], how = 'left') \
        .drop('_c0', 'categories')
    
    logger.info('Running data quality check...')
    data_quality_check(df_business,
                       ['business_id'],
                       ['business_id'])
    # Save df_business to output_data
    df_business \
        .write \
        .mode('overwrite') \
        .partitionBy('state') \
        .parquet(output_data + '/businesses')
    logger.info('[Success]: Table businesses is created!')
    
def process_users_data(spark, input_data, output_data):
    '''
    Import data from input_data, change data type and transform the data. 
    Save the dimentional tables to output_data as parquet format.
    
    Parameters:
        spark       : Spark Session.
        input_data  : Path of the input data.
        output_data : Path of the ouput data.
    
    Return nothing.
    
    '''

    df_user = spark.read.json(input_data + '/yelp-dataset/yelp_academic_dataset_user.json')
    
    #udf to split and count elements for elite and friends
    def count_num(astr):
        return len(list(filter(None, astr.split(','))))
    colSize = F.udf(lambda x: count_num(x), T.IntegerType())
    
    # Change data type and create new features
    df_user = df_user \
        .withColumn('yelping_since',
                    F.unix_timestamp('yelping_since', 'yyyy-MM-dd HH:mm:ss') \
                        .cast(T.TimestampType())) \
        .withColumn('year', F.year('yelping_since').alias('year')) \
        .withColumn('month', F.month('yelping_since').alias('month')) \
        .withColumn('num_elite', colSize('elite')) \
        .withColumn('num_friends', colSize('friends')) \
        .drop('elite', 'friends')
    
    
    logger.info('Running data quality check...')
    data_quality_check(df_user,
                       ['user_id'],
                       ['user_id'])
    # Save df_user to output_data
    df_user \
        .write \
        .mode('overwrite') \
        .partitionBy('year', 'month') \
        .parquet(output_data + '/users')
    logger.info('[Success]: Table users is created!')

def process_review_data(spark, input_data, output_data):
    '''
    Import data from input_data, change data type and transform the data. 
    Save the dimentional tables to output_data as parquet format.
    
    Parameters:
        spark       : Spark Session.
        input_data  : Path of the input data.
        output_data : Path of the ouput data.
    
    Return nothing.
    
    '''
    logger.info('Importing reveiw data...')
    # Read review data
    df_review = spark.read.json(input_data + '/yelp-dataset/yelp_academic_dataset_review.json')
    logger.info('Review data is imported!')
    # We will only focus on the businesses in state AZ
    logger.info('Importing business data from dimential table...')
    df_business = spark.read \
        .format('parquet') \
        .option('basePath', output_data + '/businesses') \
        .load(output_data + '/businesses/state=AZ')
    logger.info('Business data is imported')
    
    # Use /weather instead of /weather/*/* with wildcard.
    # The second option will not load the partition column.
    logger.info('Importing weather data from dimential table...')
    df_weather = spark.read.parquet(output_data + '/weather')
    logger.info('Weather data is imported')
# =============================================================================
#     Create table reviews and time table combined.
# =============================================================================
    # udf to join strings in a list
    concat_str = F.udf(lambda x : ','.join(x))
    # Change data type
    df_review = df_review \
        .withColumn('time_stamp',
                    F.unix_timestamp('date', 'yyyy-MM-dd HH:mm:ss') \
                        .cast(T.TimestampType())) \
        .drop('date') \
        .select('business_id',
                'cool',
                'funny',
                'review_id',
                'stars',
                'text',
                'useful',
                'user_id',
                F.year('time_stamp').alias('year'),
                F.month('time_stamp').alias('month'),
                F.date_format(F.col('time_stamp'),'yyyy-MM-dd').alias('date').cast('date'),
                F.hour('time_stamp').alias('hour'),
                F.dayofmonth('time_stamp').alias('day'),
                F.quarter('time_stamp').alias('quarter'),
                F.dayofweek('time_stamp').alias('weekday'),
                F.dayofyear('time_stamp').alias('dayofyear'),
                F.weekofyear('time_stamp').alias('weekofyear')
                ) \
        .join(df_business.select('business_id', 'station'), on = 'business_id', how = 'right') \
        .join(df_weather.select('id', 'station', 'date', 'hour'), 
              on = ['station', 'date', 'hour'], 
              how = 'left') \
        .withColumn('word_split', F.split(F.regexp_replace('text', '[^a-zA-Z0-9-]+', ' '), ' ')) \
        .withColumn('word_count', F.size(F.col('word_split'))) \
        .withColumn('word_split', concat_str(F.col('word_split'))) \
        .withColumn('weather_id', F.col('id')) \
        .drop('text', 'id', 'station')

    logger.info('Running data quality check...')
    data_quality_check(df_review,
                       ['review_id'],
                       ['user_id','business_id','review_id'])
    
    # Save df_review to output_data
    # Add overwrite mode if needed.
    df_review \
        .write \
        .mode('overwrite') \
        .partitionBy('year', 'month') \
        .parquet(output_data + '/reviews')
    logger.info('[Success]: Table reviews is created!')

def data_quality_check(spark_df, lst_unique=[], lst_no_missing=[]):
    '''
    Data quality check.
    
    Parameters
    ----------
    spark_df : spark dataframe
    lst_unique : list
        list of columns you want to check for unique value.
    lst_no_missing : list
        list of columns you want to check for missing value.
        the column types can not be timestamp or date
    Raises
    ------
    Exception
        Debug the ETL process.

    Returns
    -------
    None.

    '''
    # Count check
    if spark_df.count()<1:
        logger.warning('No data is in the dataframe!')
        #raise Exception('No data is in the dataframe! Please check the ETL process!')
    
    # Check unique
    if len(lst_unique)>0:
        for c in lst_unique:
            if spark_df.count() > spark_df.select(c).dropDuplicates().count():
                logger.warning(f'Column {c} has duplicates!')
                raise Exception(f'Columns {lst_unique} is not unique! Please check the ETL process!')
    
    # Check missing values
    if len(lst_no_missing)>0:
        df=spark_df \
            .select([F.count(F.when(F.isnan(c)|F.isnull(c),c)).alias(c) for c in lst_no_missing])
        if sum(df.take(1)[0])>0:
            logger.warning(f'There are duplicats in {lst_no_missing}!')
            logger.warngin(df.take(1)[0])
            raise Exception('There are missing values! Please check the ETL process!')
    logger.info('[Success]: Data quality check passed!')
    
def main():
    '''
    Extract yelp and weather data from S3. Transform it to dimensional tables and load it back to S3.
    '''
    spark = create_spark_session()
    input_data = 's3a://sparkify-de'
    output_data = 's3a://sparkify-de/data-lake'
     
    process_weather_data(spark, input_data, output_data)
    process_business_cat_data(spark, input_data, output_data)
    process_users_data(spark, input_data, output_data)
    process_review_data(spark, input_data, output_data)
    
    spark.stop()
    logger.info('The ETL process is completed!')
    # Save log file to the bucket.
    with open('spark_app.log', 'w') as f:
        f.write(output_data + '/spark_app.log')
    
if __name__ == '__main__':
    main()
