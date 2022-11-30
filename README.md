# Sparkify ETL Pipeline on AWS(EMR Cluster)

## Introduction

A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.


## Author

- [@Mohamed S. Elaraby](https://github.com/Aboalarbe)


## Role Description

we will build an an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables in parquet format. This will allow their analytics team to continue finding insights in what songs their users are listening to. we will be able to test your database and ETL pipeline by running queries given to you by the analytics team from Sparkify and compare your results with their expected results.


## How to run the ETL script

you should create EMR Cluster on AWS and configure it with spark and hadoop

you have 2 opthions to run your ETL job:-

first:

```bash
create jupyter notebook on your EMR cluster then run your etl code
```
second:

```bash
connect to the cluster using SSH and then run spark-submit etl.py
```

    
## Description of files in the repository

### etl.py
contains script for extracting data from JSON files in S3, making transformation then load them back to another S3 bucket.

### dl.cfg
contains AWS key and AWS secret key.

## Database Schema

![Schema](https://firebasestorage.googleapis.com/v0/b/plantsexpertsystem-f6812.appspot.com/o/Untitled%20Workspace.png?alt=media&token=52f7a554-c5db-4f01-94d7-a46e38645fde)
this schema created using "creatly.com"


## Feedback

If you have any feedback, please reach out to us at mhuss073@uottawa.ca


## 🔗 Links
[![portfolio](https://img.shields.io/badge/my_portfolio-000?style=for-the-badge&logo=ko-fi&logoColor=white)](https://www.credential.net/profile/mohamedaboalarbe/wallet)
[![linkedin](https://img.shields.io/badge/linkedin-0A66C2?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/mohammed-elaraby/)
