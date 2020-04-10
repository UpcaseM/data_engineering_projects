import os
import configparser
config = configparser.ConfigParser()
cfg_path = os.path.dirname(os.path.abspath(__file__))
config.read_file(open(cfg_path + '/dl.cfg'))

os.environ['REGION']=config['AWS']['REGION']
os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
# Queries to transform the data
from sql_queries import sql_songplays, sql_users, sql_artists, sql_time, sql_songs


def create_spark_session():
    '''
    Create or get a Spark Session.
    '''
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_log_song_data(spark, input_data, output_data):
    '''
    Import data from input_data, change data type and transform the data. 
    Save the dimentional tables to output_data as parquet format.
    
    Parameters:
        spark       : Spark Session.
        input_data  : Path of the input data.
        output_data : Path of the ouput data.
    
    Return nothing.
    
    '''
    # Set up schema and read tables
    log_schema = T.StructType([
        T.StructField('artist', T.StringType()),
        T.StructField('auth', T.StringType()),
        T.StructField('firstName', T.StringType()),
        T.StructField('gender', T.StringType()),
        T.StructField('itemInSession', T.IntegerType()),
        T.StructField('lastName', T.StringType()),
        T.StructField('length', T.DoubleType()),
        T.StructField('level', T.StringType()),
        T.StructField('location', T.StringType()),
        T.StructField('method', T.StringType()),
        T.StructField('page', T.StringType()),
        T.StructField('registration', T.StringType()),
        T.StructField('sessionId', T.IntegerType()),
        T.StructField('song', T.StringType()),
        T.StructField('status', T.IntegerType()),
        T.StructField('ts', T.DoubleType()),
        T.StructField('userAgent', T.StringType()),
        T.StructField('userId', T.StringType())
    ])
    song_schema = T.StructType([
        T.StructField('num_songs', T.IntegerType()),
        T.StructField('artist_id', T.StringType()),
        T.StructField('artist_latitude', T.StringType()),
        T.StructField('artist_longitude', T.StringType()),
        T.StructField('artist_location', T.StringType()),
        T.StructField('artist_name', T.StringType()),
        T.StructField('song_id', T.StringType()),
        T.StructField('title', T.StringType()),
        T.StructField('duration', T.DoubleType()),
        T.StructField('year', T.IntegerType())
    ])

    df_log = spark.read.json(input_data + 'log_data/*/*/*.json', schema = log_schema)
    df_song = spark.read.json(input_data + 'song_data/*/*/*/*.json', schema = song_schema)
    
    # Add a column to df_log convert the epoch time to timestamp
    df_log = df_log.withColumn('time', F.to_timestamp((df_log['ts']/1000).cast(T.TimestampType())))
    
    # Create table views to use spark sql
    df_log.createOrReplaceTempView("tb_log")
    df_song.createOrReplaceTempView("tb_song")
    
    # Create songplays and save it to S3
    spark.sql(sql_songplays) \
        .withColumn('songplay_id', F.monotonically_increasing_id()) \
        .write.partitionBy('year', 'month') \
        .parquet(output_data + 'songplays')
    print('Table songplays is created!')
    
    # Create users and save it to S3
    spark.sql(sql_users) \
        .write.parquet(output_data + 'users')
    print('Table users is created!')
    
    # Create artists and save it to S3
    spark.sql(sql_artists) \
        .write.parquet(output_data + 'artists')
    print('Table artists is created!')
    
    # Create songs and save it to S3
    spark.sql(sql_songs) \
        .write.partitionBy('year', 'artist_id') \
        .parquet(output_data + 'songs')
    print('Table songs is created!')
    
    # Create time and save it to S3
    spark.sql(sql_time) \
        .write.partitionBy('year', 'month') \
        .parquet(output_data + 'time')
    print('Table time is created!')
    
def main():
    '''
    Extract songs and events data from S3. Transform it to dimensional tables and load it back to S3.
    '''
    spark = create_spark_session()
    input_data = 's3a://udacity-dend/'
    output_data = 's3a://sparkify-de/data-lake/'
    
    process_log_song_data(spark, input_data, output_data)
    spark.stop()
    print('The ETL process is completed!')

if __name__ == "__main__":
    main()
