import re
import sys
import glob
import urllib.request as request, urllib.error as error
from bs4 import BeautifulSoup
from scrape import ScrapeCollegeStats, ScrapeNflDraftData, ScrapeCombineData, getYears
# from pyspark import SparkConf, SparkContext
# from pyspark.sql import SparkSession
# from pyspark.sql import functions as F
# from pyspark.sql.types import IntegerType

def main():
    print("####### Scrapping College Data #######")
    years = getYears(2017,2018) # Max range 1956 - 2018 Note add one extra year to end
    dataTypes = ['Rushing', 'Passing', 'Receiving']
    #RushingPlayersCollege, PassingPlayersCollege, ReceivingPlayersCollege =  ScrapeCollegeStats(years, dataTypes)

    print("####### Scrapping NFL Data #######")
    years = getYears(2017,2018) # Max range 1937 - 2018
    NFLPlayers = ScrapeNflDraftData(years) 

    print("####### Scrapping Combine Data #######")
    years = getYears(2017,2018) # Max range 2000 - 2018
    dataTypes = ['Offense', 'Defense', 'Special'] 
    #OffensePlayers, DefensePlayers, SpecailPlayers = ScrapeCombineData(years,["Offense","Defense", "Special"]) 

main()


def getNFLData():
    cleanDraftData = []
    paths = getNFLDraftPaths()

    #Clean the data
    for path in paths:
        print(path)
        cleanDraftData.append(cleanNFLDraftFile(path))

    #Join each dataframe together
    NflDataFrame = cleanDraftData.pop()
    for dataframe in cleanDraftData:
        NflDataFrame = NflDataFrame.union(dataframe)
    
    return NflDataFrame

#Method used to grab all of the NFL Draft Data and clean lineman from it
def getNFLDraftPaths():
    csvFilesPath = []
    for x in glob.glob("./NflDraftData/*.csv"):
        csvFilesPath.append(x)
    
    return csvFilesPath

#Takes in the path to a draft file and returns a cleander version as a DataFrame (or a RDD)
def cleanNFLDraftFile(path):
    unCleanData = spark.read.format("csv").option("header", "true").option("inferSchema","true").load(path)
    unCleanData.cache()

    #drop row we don't need
    dropList = ['To', 'AP1', 'PB', 'St', 'CarAV', 'DrAV', 'G', 'Cmp', 'Att14', 'Yds15', 'TD16', 'Int17', 'Att18', 'Yds19', 'TD20',
    'Rec', 'Yds22', 'TD23', 'Tkl', 'Int25', 'Sk']
    unCleanData = unCleanData.select([column for column in unCleanData.columns if column not in dropList])

    #Clean Data of all Linemen
    unCleanData = unCleanData.where(unCleanData['Pos'] != 'C') #Centers
    unCleanData = unCleanData.where(unCleanData['Pos'] != 'G') #Guards
    unCleanData = unCleanData.where(unCleanData['Pos'] != 'T') #Tackles

    unCleanData.show()
    return unCleanData

#Config
# conf = SparkConf()
# sc = SparkContext(conf=conf)
# spark = SparkSession.builder.appName("NflDraftApp").config("spark.some.config.option", "some-value").getOrCreate()
# NflDataFrame = getNFLData()
# NflDataFrame.show()
# print("############ " + str(NflDataFrame.count()))