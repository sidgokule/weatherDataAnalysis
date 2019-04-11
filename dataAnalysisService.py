# -*- coding: utf-8 -*-
"""
Created on Sun Mar 24 16:04:02 2019

@author: siddh
"""
import sys
#from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as sf
from pyspark.sql.functions import col, unix_timestamp, to_date
import pyspark.sql.functions as func

df=[]
class CustomException(Exception):
    pass


# =============================================================================
# Method to filter out the data for parameterized dates
# Input: station_name, startDate, endDate
# Output: Filtered data frame as per given arguments
# If station_name is not given the data for all stations in that interval is returned
# If startDate and endDate are not given default start and end dates available in dataset are used
# =============================================================================
def getIntervalData(station_name=None,startDate='2010-01-01',endDate='2012-12-31'):
    if station_name!=None:
        df1=df[(df.station_name==station_name) & (df.date>=startDate) & (df.date<=endDate)]
    else:
        df1=df[(df.date>=startDate) & (df.date<=endDate)]
        df2=df1.orderBy('date').collect()
    #print(df2)
    #print('len df2=',len(df2))

# =============================================================================
# Method to calculate the sum of a given dataset attribute column with optional station_name and interval filtering   
# Input: station_name,parameter to aggregate, startDate,endDate
# Output: Dataframe grouped by station_name and with column of sum of specified paramter in given interval
# =============================================================================
def calcSum(station_name=None,parameter=None,startDate='2010-01-01',endDate='2012-12-31'):
    try:
        if parameter!=None:
            if station_name!=None:
                df1=df[(df.station_name==station_name) & (df.date>=startDate) & (df.date<=endDate)]
            else:
                df1=df[(df.date>=startDate) & (df.date<=endDate)]
                
            parameter=parameter.lower()
            df2=df1.groupBy('station_name').agg(func.sum(parameter).alias('Sum_'+parameter))
            #df2.show()
            df2.toPandas().to_csv('mycsv.csv')
        else:
            raise CustomException
    except CustomException:
        print('Encountered Null Parameter value')
 
# =============================================================================
# Method to calculate the average of a given dataset attribute column with optional station_name and interval filtering   
# Input: station_name,parameter to average, startDate,endDate
# Output: Dataframe grouped by station_name and with column of average of specified paramter in given interval
# =============================================================================       
def calcAverage(station_name=None,parameter=None,startDate=None,endDate=None):
    try:
        if parameter!=None:
            
            if startDate!=None and endDate!=None and parameter!=None:
                df1=df[(df.date>=startDate) & (df.date<=endDate)]
                df2=df1.groupBy('station_name').agg(func.avg(parameter).alias('Average_'+parameter))
                df2.show()
            else:
                df1=df.groupBy('station_name').agg(func.avg(parameter).alias('Average_'+parameter))
                df1.show()
        else:
            raise CustomException
    except CustomException:
        print('Encountered Null Parameter value')
    return


#Method to read the file passed as parameter into dataframe    
def readFile(fileName):
    try:
      df=spark.read.csv(fileName,inferSchema=True,header=True)
      
      #Data preprocessing: Merge the Day,Month,Year columns into one and typecast it to date format
      df=df.withColumn('date_str',sf.concat(sf.col('year'),sf.lit('/'),sf.col('month'),sf.lit('/'),sf.col('day')))
      df = df.withColumn('date', 
                   to_date(unix_timestamp(col('date_str'), 'yyyy/MM/dd').cast("timestamp")))
      df=df.drop('date_str')
      return df
    except CustomException:
        print('File not found!')
        sys.exit()
if __name__=="__main__":
    #Create a Spark Session and provide a name to distinguish your Spark Application
    spark=SparkSession.builder.appName('WeatherApp').getOrCreate()
    
    df=readFile('C:/Users/siddh/Desktop/Intuit Project/weatherData.csv')
    
    getIntervalData(startDate='2010-01-01',endDate='2010-01-02')
    calcAverage(parameter='rainfall',startDate='2010-01-01',endDate='2012-01-01')
    calcSum(parameter='avg_temp',startDate='2010-01-01',endDate='2012-01-01')
    
    spark.stop()#Terminate the Spark session
    
    
    
    
    