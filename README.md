# BigData2017Project
BigData2017Project

### File Explaination
The repo consists of three file:

1. ```spark_job_script.py``` This is used for generating validation check and output 24 files for each column.
2. ```spark_validity_statistics.py``` spark script for summarizing how many Valid/Invalid/Null in each category.
3. ```sparksql_script``` code for using pysparkSQL to check how many distinct values in each category.


### Steps for running jobs
To get the result required, i.e. check for base_type, semantic_type, validity, simply run the spark_job_script.py on your dumbo as follows,

1. Upload ```spark_job_script.py``` to your hpc storage with command:
```
scp  dir/spark_job_script.py  NetID@dumbo.es.its.nyu.edu:/home/NetID
```

2. Upload ```NYPD_Complaint_Data_Historic.csv``` to hpc like above then upload it to hdfs with command:
```
hadoop fs -copyFromLocal NYPD_Complaint_Data_Historic.csv
```

3. Since we need to use some python packages, first set up your python environment with below sentences

```
module load python/gnu/3.4.4
export PYSPARK_PYTHON=/share/apps/python/3.4.4/bin/python
export PYTHONHASHSEED=0
export SPARK_YARN_USER_ENV=PYTHONHASHSEED=0
```

4. Submit spark job
```
spark-submit spark_job_script.py
```

5. Get output 
```
hadoop fs -getmerge name.out name.out
```

### Please note:
1. We assume user won't change the name of the CSV file and keep it as NYPD_Complaint_Data_Historic.csv;
2. Please make sure the previously output on your HPC is removed so it won't influence running the script.


