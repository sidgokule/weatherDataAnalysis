# -*- coding: utf-8 -*-
"""
Created on Thu Apr 11 14:48:41 2019

@author: siddh
"""
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as sf
from pyspark.sql.functions import col, unix_timestamp, to_date
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression
df=[]
class CustomException(Exception):
    pass


def logisticRegression():
    df1=df.select(['station_name','avg_temp','avg_wind','sun'])
    df1=df.dropna(subset=['avg_temp','avg_wind','sun'])
    assembler= VectorAssembler(inputCols=['avg_wind','sun'],outputCol='features')
    output=assembler.transform(df1)
    finalData=output.select('features','avg_temp')
    trainData,testData=finalData.randomSplit([0.7,0.3])
    lr=LinearRegression(labelCol='avg_temp')
    lrModel=lr.fit(trainData)
    testResults=lrModel.evaluate(testData)
    testResults.predictions.show()
    print('testResults.rootMeanSquaredError=',testResults.rootMeanSquaredError)
    
    
    

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
    df
    logisticRegression()
    spark.stop()