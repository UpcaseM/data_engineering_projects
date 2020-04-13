# Data Pipline with Airflow

This is the project submission for project: Data Pipline. 

The project contains practice for:
1. Designed a data warehouse in Redshift.
2. Wrote query statements including DROP, CREATE, COPY, INSERT, and SELECT.
3. Created custom Airflow operaters to build ETL pipline to load data from S3 to AWS Redshift on a schedule.

## Project Introduction
A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

My task is to to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. Also, the data quality plays a big part when analyses are executed on top the data warehouse.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

## Project Breakdown
We will focus on the building and deploying the Airflow solution in this project.
- Set up Airflow and connections to AWS and redshift
- Create the Project Strucute
- Design and configure the Sparkify DAG
- Build custom operators

### Airflow Setup
1. Install Airflow
	```
	conda install -c conda-forge airflow
	#~/airflow is the default home
	# You can lay home somewhere else if you perfer by
	#export AIRFLOW_HOME=~/airflow
	```
2. Place dags and plugins in AIRFLOW_HOME dir
3. Initialize the database, and start web server and scheduler.
	```
	airflow initdb
	airflow webserver -p 8080
	airflow scheduler
	#Visit localhost:8080 in the browser
	```
4. Set up AWS credentials and redshift connection.

**AWS credentials**
* **Conn Id**: Enter *aws_credentials*.
* **Conn** Type: Enter *Amazon Web Services*.
* **Login**: Enter your **Access key ID** from the IAM User credentials you downloaded earlier.
* **Password**: Enter your **Secret access key** from the IAM User credentials you downloaded earlier.

**Redshift**
* **Conn Id**: Enter *redshift*.
* **Conn Type**: Enter *Postgres*.
* **Host**: Enter the *endpoint* of your Redshift cluster, excluding the port at the end.
* **Schema**: Enter *dev*. This is the Redshift database you want to connect to.
* **Login**: Enter *awsuser*.
* **Password**: Enter the **password** you created when launching your Redshift cluster.
* **Port**: Enter *5439*.

### Project Strucute

create_tables.py		(Python script which creates tables in Redshift)

sql_queries.py			(Python script which creates tables in Redshift)

airflow

├──dags
│  ├── sparkify_dag.py		(The Sparkfiy DAG)

│  └── var.json			(Varibles used in sparkify_dag.py in json format)

└──plugins

   ├── __ init__.py

   ├── helpers

   │   ├── __ init__.py

   │   └── sql_queries.py	(Contains all sql queries)

   └── operators

       ├── __ init__.py

       ├── data_quality.py  	(Contains class DataQualityOperator)

       ├── load_dimension.py	(Contains class LoadDimensionOperator)

       ├── load_fact.py		(Contains class LoadFactOperator)

       └── stage_redshift.py	(Contains class StageToRedshiftOperator)


*Configure files are not listed.*

### Sparkify DAG
**DAG parameters**:

* The DAG does not have dependencies on past runs
* DAG has schedule interval set to hourly
* On failure, the task are retried 3 times
* Retries happen every 5 minutes
* Catchup is turned off
* Email are not sent on retry

The task dependencies are shown below.

![dependencies](dag.png)

### Custom Operators
In this project, we build 4 custom operators to create necessary tables, stage the data, transform the data, and check data quality.

#### Stage Operator
The stage operator is to load any JSON formatted files from S3 to Amazon Redshift. The operator creates and runs a SQL COPY statement based on the parameters provided. The operator's parameters specify where in S3 the file is loaded and what is the target table.

Also the stage operator is containing a templated field that allows it to load timestamped files from S3 based on the execution time and run backfills.

#### Fact and Dimension Operators
The dimension and fact operators make use of the SQL helper class to run data transformations. The operators take as input a SQL statement and target database on which to run the query against. A target table is also defined that contains the results of the transformation.

Dimension loads are done with the truncate-insert pattern where the target table is emptied before the load. Thus, we have a parameter that allows switching between insert modes when loading dimensions. Fact tables are usually so massive that they should only allow append type functionality.

#### Data Quality Operator
The data quality operator is used to run checks on the data itself. The operator's main functionality is to receive one or more SQL based test cases along with the expected results and execute the tests. For each the test, the test result and expected result will be checked and if there is no match, the operator would raise an exception and the task is retried and fails eventually.

<font color="red"> Make sure to delete the redshift cluster when finished to prevent huge cost.</font>

## Author
**Rick Wu**
