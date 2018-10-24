import re
import sys
import glob
import os
import urllib.request as request, urllib.error as error
from bs4 import BeautifulSoup
from scrape import ScrapeCollegeStats, ScrapeNflDraftData, ScrapeCombineData, getYears, writeCSV, getData
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType
from pyspark.ml.feature import Imputer


#Takes in the path to a draft file and returns a cleander version as a DataFrame (or a RDD)
def cleanDraftData(postion):
    '''
        [X] need to fill in nulls for the Age with the AVG, or with the median of all the ages  --> opted out for the medium 
    '''
    unCleanData = spark.read.format("csv").option("header", "true").option("inferSchema","true").load("./data/NflDraftData/draftData.csv")

    # drop columns we don't need
    unCleanData = unCleanData.select("Rnd", "Pick", "Player Name", "Pos", 'Age', 'College', 'Draft Year')

    if(postion == "RB" or postion == "QB" or postion == "WR"):
        unCleanData = unCleanData.where(unCleanData["Pos"] == postion)
    else: # Retrun all of the skill offensive players (WR, RB, TE, QB, FB)
        #drop lineman both offense and defense as well as defensive players and special teams
        droppedPostions = ['DE', 'DT', 'T', 'O', 'G', 'C', 'K','NT', 'DL', 'OL', 'LS', 'LB', 'DB','P', 'OLB', 'CB', 'S', 'ILB'] # With only O players we are down to 2000 data pints
        for postion in droppedPostions:
            unCleanData = unCleanData.where(unCleanData["Pos"] != postion)
    
    # Cast values to doubles
    doubleCols = ['Age', 'Rnd', 'Pick', 'Draft Year']
    for c in doubleCols:
        unCleanData = unCleanData.withColumn(c, unCleanData[c].cast(DoubleType()))

    # Used to fill in Null values with the medium
    imputer = Imputer(
    inputCols=["Age"], 
    outputCols=["Age"]
    )   
    cleanData = imputer.setStrategy("median").fit(unCleanData).transform(unCleanData)
    cleanData.show()
    return cleanData

def cleanCombineData(postion): # 2688 --> RB 509
    '''
        [ ] need to fill these rows with the mean or avg if the data is null -->  [AV, Height, Wt, 40yd, Vertical, BechReps, Broad Jump, Shuttle ]
        [ ] need to fill drafted with false if it null and true with is anything else
    '''
    unCleanData = spark.read.format("csv").option("header", "true").option("inferSchema","true").load("./data/CombineData/OffensePlayersData.csv")
    unCleanData = unCleanData.select("Rk", "Year16", "Player", "Pos", 'AV', 'School', 'Height', 'Wt', '40yd', 'Vertical', 'BenchReps', 'Broad Jump', 'Shuttle', 'Drafted (Tm / Rd/ Yr)')
    unCleanData = unCleanData.withColumnRenamed("Year16", 'Year').withColumnRenamed("Drafted (Tm / Rd/ Yr)", "Drafted")

    if(postion == "RB" or postion == "QB" or postion == "WR"):
        unCleanData = unCleanData.where(unCleanData["Pos"] == postion)

    cleanData = unCleanData
    print(cleanData.count())
    cleanData.show()
    return cleanData

def main():
    getData()

main()

#Config
conf = SparkConf()
sc = SparkContext(conf=conf)
spark = SparkSession.builder.appName("NflDraftApp").config("spark.some.config.option", "some-value").getOrCreate()
nflDF =  cleanDraftData("RB") # takes in either RB, QB, WR since these are the main postions we can analytics on 
                               # Note from 2000 - 2018 we have 371 RBs, 223 QBs, 585 WRs
#combineDF = cleanCombineData("RB")
print("############ " + str(NflDataFrame.count()))