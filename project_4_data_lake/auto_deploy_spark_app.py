"""
Created on April 9, 2020
@author: UpcaseM

This is a script to deploy an AWS EMR cluster programmably, and terminate it when the spark job is done.
"""

import os
import boto3
import time
import tarfile
import configparser
config = configparser.ConfigParser()
os.chdir(os.path.dirname(os.path.realpath(__file__)))
config.read_file(open('spark_app/dl.cfg'))

os.environ['REGION'] = config['AWS']['REGION']
os.environ['AWS_ACCESS_KEY_ID']= config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']= config['AWS']['AWS_SECRET_ACCESS_KEY']

class DeplySparkApp:
    '''
    This is a class to deploy a local spark app on an AWS EMR cluster.
    '''
    
    def __init__(self):
        self.app_name = 'spark_app'                         #Name of the app
        self.key_name = 'spark-cluster'                     #Name of the key pair
        self.app_dir = 'spark_app/'                         #Dir of the app folder
        self.tar_file = 'scripts.tar.gz'                    #Name of the tar.gz file
        self.my_bucket = 'sparkify-de'                      #Name of the buck in s3 you will use
        self.bucket_file_key = 'data-lake/scripts'          #Key to store spark-app files
        self.job_flow_id = None                             #Return by AWS in create_cluster_run_job()
        
        
    def run(self, app_files, arguments):
        '''
        The function to run the deployment.
        Param
            app_files      : a list of file names in the spark app that need to be uploaded.
            arguments      : The argumets for EMR steps.
        '''
        session = boto3.Session()                           #Create a boto3 session
        s3 = session.resource('s3')                         #Open s3 connection  
        self.make_tarfile(                                  #Create a tar.gz file for spark app
            self.tar_file, 
            self.app_dir,
            app_files)
        self.upload_files(                                  #Update bash files and app files to s3
            s3,
            self.my_bucket, 
            self.bucket_file_key)
        c = session.client('emr')                           #Open an EMR connection
        self.create_cluster_run_job(                        #Create a cluster and run the spark app
            c,
            arguments)        
        self.wait_till_termindate(c)                        #Print states until the cluster termindate
        self.remove_s3_files(                               #Remove spark app files in s3
            s3,
            self.my_bucket,
            self.bucket_file_key)                            
        
        
    def make_tarfile(self, output_filename, source_dir, file_list):
        '''
        Create tarfile for files given.
        Pparam:
            output_filename : Name of the tarfile to be created.
            source_dir      : Source dir
            file_list       : File names, files that will be added to the tarfile.
        '''
        with tarfile.open(output_filename, "w:gz") as tar:
            files = file_list
            for file in files:
                tar.add(source_dir + file, arcname= file)

                
    def upload_files(self, s3, buck_name, script_folder):
        """
        Move the PySpark script files to the S3 bucket
        param   s3            : s3 resource
                buck_name     : buck to store the scripts
                script_folder : folder to store the scripts
        return:
        """
        # Shell file: setup (download S3 files to local machine)
        s3.Object(buck_name, script_folder + '/setup.sh')\
          .put(Body=open('setup.sh', 'rb'), ContentType='text/x-sh')
        # Shell file: Terminate idle cluster
        s3.Object(buck_name, script_folder + '/terminate_idle_cluster.sh')\
          .put(Body=open('terminate_idle_cluster.sh', 'rb'), ContentType='text/x-sh')
        # Compressed Python script files (tar.gz)
        s3.Object(buck_name, script_folder + '/scripts.tar.gz')\
          .put(Body=open('scripts.tar.gz', 'rb'), ContentType='application/x-tar')
        
    def create_cluster_run_job(self, c, arguments):
        """
        Function to create a cluster and run spark jobs.
        - Create a cluster
        - Run BootstrapActions from s3
        - Run the main file of the spark app
        
        param c         : EMR client
              arguments : The arguments for EMR steps
        return:
        """
        response = c.run_job_flow(
            Name=self.app_name,
            LogUri='s3://{}/myLog/'.format(self.my_bucket),
            ReleaseLabel="emr-5.29.0",
            Instances={
                'InstanceGroups': [
                    {
                        'Name': 'EmrMaster',
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'MASTER',
                        'InstanceType': 'm5.xlarge',
                        'InstanceCount': 1,
                    },
                    {
                        'Name': 'EmrCore',
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'CORE',
                        'InstanceType': 'm5.xlarge',
                        'InstanceCount': 3,
                    },
                ],
                'Ec2KeyName': self.key_name, 
                'KeepJobFlowAliveWhenNoSteps': False # The cluster will terminate after all steps
            },
            Applications=[{'Name': 'Hadoop'}, {'Name': 'Spark'}],
            JobFlowRole='EMR_EC2_DefaultRole',
            ServiceRole='EMR_DefaultRole',
            VisibleToAllUsers=True,
            BootstrapActions=[
                {
                    'Name': 'setup',
                    'ScriptBootstrapAction': {
                        'Path': 's3://{}/{}/setup.sh' \
                            .format(self.my_bucket, self.bucket_file_key)
                    }
                },
                # Always add terminate_idle_cluster.sh to prevent from unexpected AWS charges
                # if you set KeepJobFlowAliveWhenNoSteps to True.
                {
                    'Name': 'idle timeout',
                    'ScriptBootstrapAction': {
                        'Path':'s3://{}/{}/terminate_idle_cluster.sh' \
                            .format(self.my_bucket, self.bucket_file_key),
                        'Args': ['1800', '300']
                    }
                },
            ],
            Steps=[
                {
                    'Name': 'Spark Application',
                    'ActionOnFailure': 'CONTINUE',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': arguments
                    }
                },
            ]
        )
        # Get the job_flow_id
        response_code = response['ResponseMetadata']['HTTPStatusCode']
        if response_code == 200:
            self.job_flow_id = response['JobFlowId']
        else:
            print("Could not create EMR cluster (status code {})".format(response_code))
    
    def remove_s3_files(self, s3, buck_name, script_folder):
        """
        Remove Spark files from temporary bucket
        param   s3            :s3 resource
                buck_name     :Bucket name in s3
                script_folder :the script key in s3 bucket.
        return:
        """
        bucket = s3.Bucket(buck_name)
        bucket.objects.filter(Prefix=script_folder).delete()
    
    def wait_till_termindate(self, c):
        """
        Wait until the EMR finish the job and termindate.
        Meanwhile, print state every 30 sec.
        param   c: the EMR session.
        return:
        """
        stop = False
        while stop is False:
            description = c.describe_cluster(ClusterId=self.job_flow_id)
            state = description['Cluster']['Status']['State']
            if state == 'TERMINATED' or state == 'TERMINATED_WITH_ERRORS':
                stop = True
            print(state)
            # Prevent ThrottlingException by limiting number of requests
            time.sleep(30)
        print(state)
            
        
if __name__ == '__main__':
    spark_app_files = ['etl.py', 'dl.cfg', 'sql_queries.py']
    arguments = [
                 'spark-submit',
                 '/home/hadoop/etl.py'
                 ]
    DeplySparkApp().run(spark_app_files, arguments)
    print('The job is completed!')
        
