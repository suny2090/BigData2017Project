
# coding: utf-8

#Another approach is using SparkSession and readin directly
#
#spark = SparkSession.builder.appName("Python Spark SQL basic example").config("spark.some.config.option", "some-value").getOrCreate()
#df = spark.read.csv('NYPD_Complaint_Data_Historic.csv',header=True)
#schema_sd = spark.createDataFrame(Noheader,str(header.take(1)[0]).split(','))
#schema_sd.createOrReplaceTempView("df") 

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum
from operator import add
from pyspark import SparkContext
from pyspark.sql import SQLContext
from csv import reader
from pyspark.sql.types import *




data=sc.textFile('NYPD_Complaint_Data_Historic.csv')
header=data.filter(lambda l: 'CMPLNT_NUM' in l)
Noheader=data.subtract(header)
Noheader=Noheader.mapPartitions(lambda x: reader(x))




# ### Create Dataframe to search

# In[74]:

schema_sd = spark.createDataFrame(Noheader,str(header.take(1)[0]).split(','))
schema_sd.createOrReplaceTempView("df") 
#The missing value now would be turned into ''




# In[86]:

spark.sql("select count(distinct CMPLNT_NUM) from df where CMPLNT_NUM <> ''").show() 

spark.sql("select count(DISTINCT CMPLNT_FR_DT) from df where CMPLNT_FR_DT <> ''").show()

spark.sql("select count(distinct CMPLNT_FR_TM) from df where CMPLNT_FR_TM <> ''").show()

spark.sql("select count(distinct CMPLNT_TO_DT) from df where CMPLNT_TO_DT <> ''").show()

spark.sql("select count(distinct CMPLNT_TO_TM) from df where CMPLNT_TO_TM <> ''").show()

spark.sql("select count(distinct RPT_DT) from df where RPT_DT <> ''").show()

spark.sql("select count(distinct KY_CD) from df where KY_CD <> ''").show()

spark.sql("select count(distinct OFNS_DESC) from df where OFNS_DESC <> ''").show()

spark.sql("select count(distinct PD_CD) from df where PD_CD <> ''").show()

spark.sql("select count(distinct PD_DESC) from df where PD_DESC <> ''").show()

spark.sql("select count(distinct CRM_ATPT_CPTD_CD) from df where CRM_ATPT_CPTD_CD <> ''").show()

spark.sql("select count(distinct LAW_CAT_CD) from df where LAW_CAT_CD <> ''").show()

spark.sql("select count(distinct JURIS_DESC) from df where JURIS_DESC <> ''").show()

spark.sql("select count(distinct BORO_NM) from df where BORO_NM <> ''").show()

spark.sql("select count(distinct ADDR_PCT_CD) from df where ADDR_PCT_CD <> ''").show()

spark.sql("select count(distinct LOC_OF_OCCUR_DESC) from df where LOC_OF_OCCUR_DESC <> ''").show()

spark.sql("select count(distinct PD_CD) from df where PD_CD <> ''").show()

spark.sql("select count(distinct PREM_TYP_DESC) from df where PREM_TYP_DESC <> ''").show()

spark.sql("select count(distinct PARKS_NM) from df where PARKS_NM <> ''").show()

spark.sql("select count(distinct HADEVELOPT) from df where HADEVELOPT <> '' ").show()

spark.sql("select count(distinct X_COORD_CD) from df where X_COORD_CD <> ''").show()

spark.sql("select count(distinct Y_COORD_CD) from df where Y_COORD_CD <> ''").show()

spark.sql("select count(distinct Latitude) from df where Latitude <> ''").show()

spark.sql("select count(distinct Longitude) from df where Longitude <> ''").show()

spark.sql("select count(distinct Lat_Lon) from df where Lat_Lon <> ''").show()

spark.sql("select count(distinct LOC_OF_OCCUR_DESC) from df where LOC_OF_OCCUR_DESC <> ' '").show()

spark.sql("select distinct LOC_OF_OCCUR_DESC from df where LOC_OF_OCCUR_DESC <> ' '").show()

spark.sql("select distinct ADDR_PCT_CD from df where ADDR_PCT_CD <> ''").show(50)

'''The output is as below

spark.sql("select count(distinct Longitude) from df where Longitude <> ''").show()

+--------------------------+                                                    
|count(DISTINCT CMPLNT_NUM)|
+--------------------------+
|                   5101231|
+--------------------------+

>>> 
>>> spark.sql("select count(DISTINCT CMPLNT_FR_DT) from df where CMPLNT_FR_DT <> ''").show()
+----------------------------+                                                  
|count(DISTINCT CMPLNT_FR_DT)|
+----------------------------+
|                        6371|
+----------------------------+

>>> 
>>> spark.sql("select count(distinct CMPLNT_FR_TM) from df where CMPLNT_FR_TM <> ''").show()
+----------------------------+                                                  
|count(DISTINCT CMPLNT_FR_TM)|
+----------------------------+
|                        1442|
+----------------------------+

>>> 
>>> spark.sql("select count(distinct CMPLNT_TO_DT) from df where CMPLNT_TO_DT <> ''").show()
+----------------------------+                                                  
|count(DISTINCT CMPLNT_TO_DT)|
+----------------------------+
|                        4827|
+----------------------------+

>>> 
>>> spark.sql("select count(distinct CMPLNT_TO_TM) from df where CMPLNT_TO_TM <> ''").show()
+----------------------------+                                                  
|count(DISTINCT CMPLNT_TO_TM)|
+----------------------------+
|                        1441|
+----------------------------+

>>> 
>>> spark.sql("select count(distinct RPT_DT) from df where RPT_DT <> ''").show()
+----------------------+                                                        
|count(DISTINCT RPT_DT)|
+----------------------+
|                  3652|
+----------------------+

>>> 
>>> spark.sql("select count(distinct KY_CD) from df where KY_CD <> ''").show()
+---------------------+                                                         
|count(DISTINCT KY_CD)|
+---------------------+
|                   74|
+---------------------+

>>> 
>>> spark.sql("select count(distinct OFNS_DESC) from df where OFNS_DESC <> ''").show()
+-------------------------+                                                     
|count(DISTINCT OFNS_DESC)|
+-------------------------+
|                       70|
+-------------------------+

>>> 
>>> spark.sql("select count(distinct PD_CD) from df where PD_CD <> ''").show()
+---------------------+                                                         
|count(DISTINCT PD_CD)|
+---------------------+
|                  415|
+---------------------+

>>> 
>>> spark.sql("select count(distinct PD_DESC) from df where PD_DESC <> ''").show()
+-----------------------+                                                       
|count(DISTINCT PD_DESC)|
+-----------------------+
|                    403|
+-----------------------+

>>> 
>>> spark.sql("select count(distinct CRM_ATPT_CPTD_CD) from df where CRM_ATPT_CPTD_CD <> ''").show()
+--------------------------------+                                              
|count(DISTINCT CRM_ATPT_CPTD_CD)|
+--------------------------------+
|                               2|
+--------------------------------+

>>> 
>>> spark.sql("select count(distinct LAW_CAT_CD) from df where LAW_CAT_CD <> ''").show()
+--------------------------+                                                    
|count(DISTINCT LAW_CAT_CD)|
+--------------------------+
|                         3|
+--------------------------+

>>> 
>>> spark.sql("select count(distinct JURIS_DESC) from df where JURIS_DESC <> ''").show()
+--------------------------+                                                    
|count(DISTINCT JURIS_DESC)|
+--------------------------+
|                        25|
+--------------------------+

>>> 
>>> spark.sql("select count(distinct BORO_NM) from df where BORO_NM <> ''").show()
+-----------------------+                                                       
|count(DISTINCT BORO_NM)|
+-----------------------+
|                      5|
+-----------------------+

>>> 
>>> spark.sql("select count(distinct ADDR_PCT_CD) from df where ADDR_PCT_CD <> ''").show()
+---------------------------+                                                   
|count(DISTINCT ADDR_PCT_CD)|
+---------------------------+
|                         77|
+---------------------------+

>>> 
>>> spark.sql("select count(distinct LOC_OF_OCCUR_DESC) from df where LOC_OF_OCCUR_DESC <> ''").show()
+---------------------------------+                                             
|count(DISTINCT LOC_OF_OCCUR_DESC)|
+---------------------------------+
|                                6|
+---------------------------------+

>>> 
>>> spark.sql("select count(distinct PD_CD) from df where PD_CD <> ''").show()
+---------------------+                                                         
|count(DISTINCT PD_CD)|
+---------------------+
|                  415|
+---------------------+

>>> 
>>> spark.sql("select count(distinct PREM_TYP_DESC) from df where PREM_TYP_DESC <> ''").show()
+-----------------------------+                                                 
|count(DISTINCT PREM_TYP_DESC)|
+-----------------------------+
|                           70|
+-----------------------------+

>>> 
>>> spark.sql("select count(distinct PARKS_NM) from df where PARKS_NM <> ''").show()
+------------------------+                                                      
|count(DISTINCT PARKS_NM)|
+------------------------+
|                     863|
+------------------------+

>>> 
>>> spark.sql("select count(distinct HADEVELOPT) from df where HADEVELOPT <> '' ").show()
+--------------------------+                                                    
|count(DISTINCT HADEVELOPT)|
+--------------------------+
|                       278|
+--------------------------+

>>> 
>>> spark.sql("select count(distinct X_COORD_CD) from df where X_COORD_CD <> ''").show()
+--------------------------+                                                    
|count(DISTINCT X_COORD_CD)|
+--------------------------+
|                     69532|
+--------------------------+

>>> 
>>> spark.sql("select count(distinct Y_COORD_CD) from df where Y_COORD_CD <> ''").show()
+--------------------------+                                                    
|count(DISTINCT Y_COORD_CD)|
+--------------------------+
|                     72316|
+--------------------------+

>>> 
>>> spark.sql("select count(distinct Latitude) from df where Latitude <> ''").show()
+------------------------+                                                      
|count(DISTINCT Latitude)|
+------------------------+
|                  112803|
+------------------------+

>>> 
>>> spark.sql("select count(distinct Longitude) from df where Longitude <> ''").show()
+-------------------------+                                                     
|count(DISTINCT Longitude)|
+-------------------------+
|                   112807|
+-------------------------+

>>> 
>>> spark.sql("select count(distinct Lat_Lon) from df where Lat_Lon <> ''").show()
+-----------------------+                                                       
|count(DISTINCT Lat_Lon)|
+-----------------------+
|                 112826|
+-----------------------+



'''




