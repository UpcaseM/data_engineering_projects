#!/bin/bash

# Parse arguments
s3_bucket_script="s3://sparkify-de/data-lake/scripts/scripts.tar.gz"

# Download compressed script tar file from S3
aws s3 cp $s3_bucket_script /home/hadoop/scripts.tar.gz

# Untar file
tar zxvf "/home/hadoop/scripts.tar.gz" -C /home/hadoop/

# Install requirements for Python script
sudo pip install configparser
