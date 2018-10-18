import re
import sys
import glob
import os
import urllib.request as request, urllib.error as error
from bs4 import BeautifulSoup
from scrape import ScrapeCollegeStats, ScrapeNflDraftData, ScrapeCombineData, getYears, writeCSV, getData
# from pyspark import SparkConf, SparkContext
# from pyspark.sql import SparkSession
# from pyspark.sql import functions as F
# from pyspark.sql.types import IntegerType

# #Takes in the path to a draft file and returns a cleander version as a DataFrame (or a RDD)
# def cleanNFLDraftFile(path):
#     unCleanData = spark.read.format("csv").option("header", "true").option("inferSchema","true").load(path)
#     unCleanData.cache()

#     #drop row we don't need
#     dropList = ['To', 'AP1', 'PB', 'St', 'CarAV', 'DrAV', 'G', 'Cmp', 'Att14', 'Yds15', 'TD16', 'Int17', 'Att18', 'Yds19', 'TD20',
#     'Rec', 'Yds22', 'TD23', 'Tkl', 'Int25', 'Sk']
#     unCleanData = unCleanData.select([column for column in unCleanData.columns if column not in dropList])

#     #Clean Data of all Linemen
#     unCleanData = unCleanData.where(unCleanData['Pos'] != 'C') #Centers
#     unCleanData = unCleanData.where(unCleanData['Pos'] != 'G') #Guards
#     unCleanData = unCleanData.where(unCleanData['Pos'] != 'T') #Tackles

#     unCleanData.show()
#     return unCleanData

def cleanDraftData():
    unCleanData = spark.read.format("csv").option("header", "true").option("inferSchema","true").load("./data/NflDraftData/draftData.csv")
    for c in unCleanData.columns:
        print(c)
    # drop columns we don't nee
    unCleanData = unCleanData.select("Rnd", " Pick", " Player Name", " Pos", ' Age', ' SK', ' College', ' Draft Year')

    #drop lineman both offense and defense
    droppedPostions = ['DE', 'DT', 'T', 'O', 'G', 'C', 'K','NT', 'DL', 'OL', 'LS'] # ? Ls, 
    for postion in droppedPostions:
        unCleanData = unCleanData.where(unCleanData[" Pos"] != postion)

    print("###############")
    print(unCleanData.count())

    # drop special teams
    unCleanData

def main():
    getData()

main()


# def getNFLData():
#     cleanDraftData = []
#     paths = getNFLDraftPaths()

#     #Clean the data
#     for path in paths:
#         print(path)
#         cleanDraftData.append(cleanNFLDraftFile(path))

#     #Join each dataframe together
#     NflDataFrame = cleanDraftData.pop()
#     for dataframe in cleanDraftData:
#         NflDataFrame = NflDataFrame.union(dataframe)
    
#     return NflDataFrame

# #Method used to grab all of the NFL Draft Data and clean lineman from it
# def getNFLDraftPaths():
#     csvFilesPath = []
#     for x in glob.glob("./NflDraftData/*.csv"):
#         csvFilesPath.append(x)
    
#     return csvFilesPath


#Config
# conf = SparkConf()
# sc = SparkContext(conf=conf)
# spark = SparkSession.builder.appName("NflDraftApp").config("spark.some.config.option", "some-value").getOrCreate()
# cleanDraftData()
# NflDataFrame = getNFLData()
# NflDataFrame.show()
# print("############ " + str(NflDataFrame.count()))