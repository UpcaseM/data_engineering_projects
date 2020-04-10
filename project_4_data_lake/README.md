# Data Lake

This is the project submission for the Project: Data Lake. 

The project contains practice for:
1. Designed analytics tables.
2. Used Pyspark to build an ETL pipline: Extract data from a data lake hosted on S3, transform the data and load the data back into the data lake.
3. Automated the process of running Pyspark scripts on AWS EMR using AWS Python SDK.

## Project Introduction
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As a data engineer, my task is to build an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

## Project Breakdown
- Undstand the data scource
- Design dimentional tables
- Build an ETL pipline using Pyspark
- Deploy an AWS EMR and run Pyspark scripts programmably

### Project files

Files used on the project:
1. **setup.sh** is a bash file to setup files and requirements needed for running Pyspark files on AWS EMR cluster.
2. **terminate_idle_cluster.sh** is a bash file to termindate an idle cluster after a period time.
3. **auto_deploy_spark_app.py** is a Python file to deploy Pyspark files to EMR cluster programmably.
4. **etl.py** is where we load raw data from data lake hosted on s3, process the data into the analytics tables on AWS EMR cluster, and load the tables into the data lake. 
5. **README.md** current file, provides discussion on my project.
6. **sql_queries.py** is where we define all SQL statements.


## Understanding the Datasets
- **Song datasets**: all json files are nested in subdirectories under *s3://udacity-dend/song_data*. A sample of this files is:

```
{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
```

- **Log datasets**: all json files are nested in subdirectories under *s3://udacity-dend/log_data*. A sample of a single row of each files is:

```
{"artist":"Slipknot","auth":"Logged In","firstName":"Aiden","gender":"M","itemInSession":0,"lastName":"Ramirez","length":192.57424,"level":"paid","location":"New York-Newark-Jersey City, NY-NJ-PA","method":"PUT","page":"NextSong","registration":1540283578796.0,"sessionId":19,"song":"Opium Of The People (Album Version)","status":200,"ts":1541639510796,"userAgent":"\"Mozilla\/5.0 (Windows NT 6.1) AppleWebKit\/537.36 (KHTML, like Gecko) Chrome\/36.0.1985.143 Safari\/537.36\"","userId":"20"}
```

## Designing the dimentional tables

**songplays** - Fact table, song plays records in data log.

* songplay_id 	(INT) PRIMARY KEY: ID of each user song play
* start_time 	(TIMESTAMP): Timestamp of beggining of user activity
* month		(INT): Month the record, used to store data by partition
* year		(INT): Year of the record, used to store data by partition
* user_id 	(INT): ID of user
* level 	(TEXT): User level {free | paid}
* song_id 	(TEXT): NOT NULL: ID of Song played
* artist_id 	(TEXT): NOT NULL: ID of Artist of the song played
* session_id 	(INT): ID of the user Session
* location 	(TEXT): User location
* user_agent 	(TEXT): Agent used by user to access Sparkify platform

**users** - users in the app

* user_id 	(INT) PRIMARY KEY: ID of user
* first_name 	(TEXT): First name of user
* last_name 	(TEXT): Last Name of user
* gender 	(TEXT): Gender of user {M | F}
* level 	(TEXT): User level {free | paid}

**songs** - songs in music database

* song_id 	(TEXT) PRIMARY KEY: ID of Song
* title 	(TEXT): NOT NULL: Title of Song
* artist_id 	(TEXT): NOT NULL: ID of song Artist
* year 		(INT): Year of song released
* duration 	(FLOAT): Song duration

**artists** - artists in music database

* artist_id 	(TEXT) PRIMARY KEY: ID of Artist
* name 		(TEXT): Name of Artist
* location 	(TEXT): Name of Artist city
* lattitude 	(TEXT): Lattitude location of artist
* longitude 	(TEXT): Longitude location of artist

**time** - timestamps of records in songplays broken down into specific units

* start_time 	(TIMESTAMP) PRIMARY KEY: Timestamps
* hour 		(INT): Hour associated to start_time
* day 		(INT): Day associated to start_time
* week 		(INT): Week of year associated to start_time
* month 	(INT): Month associated to start_time
* year 		(INT): Year associated to start_time
* weekday 	(TEXT): Name of week day associated to start_time

## ETL Pipeline

1. Create a config file *df.cfg* for AWS region, access key id, and secret access key.

2. Write [queries](spark_app/sql_queries.py) for etl.

3. build an [etl](spark_app/etl.py) pipline to process the data.
 * Load credentials
 * Read data from data lake(s3)
 * process the data using Pyspark
 * Load it back to date lake

   All five tables are written to parquet files. Songplays table files are partitioned by year and month. Time table files are partitioned by year and month. Songs table files are partitioned by year and artist.

## Deploy Pyspark files

It's always good to have a solution deployed programmably instead of manually!
The [auto deploy spark app](auto_deploy_spark_app.py) contains steps below.

1. Create a boto3(AWS Python SDK) session and s3 connection.

2. Compress the Python files to a tar.gz file.

3. Upload bash files and Python files to s3(data lake).

4. Create an AMS EMR cluster, and run python files through Steps.

5. Check states of the cluster and make sure it's termindated when the job is done.

6. Remove python files in s3.

## Author
**Rick Wu**
