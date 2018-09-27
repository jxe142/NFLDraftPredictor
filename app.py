import re
import sys
import urllib.request as request, urllib.error as error
from bs4 import BeautifulSoup
import glob
import glob
# from pyspark import SparkConf, SparkContext
# from pyspark.sql import SparkSession
# from pyspark.sql import functions as F
# from pyspark.sql.types import IntegerType
 

'''
We have about 40364  nodes in the college data in total 

TODO:
    * Remove Kicking and specials team data from the NFL Data Set
'''
################################################ METHODS FOR THE College STATS DATA ################################################
'''
    * Pulls in the collage data for passing yards 
    * The years are from 1956 until 2017 TODO may have to drop some years to align with NFL data
    
    * Table structure Passing :
    Rnk | Player Name | School | Conf | Games Played | Cmp (passing) | Att (passing) | Percent (passing) | Yards (passing) | Avg Yards (passing) | 
    TD (passing) | Interceptins (passing) | Rate (passing) | Att (rushing) | Yds (rushing) | Avg Yards (rushing) | TD (rushing) | Year |
    
    * Table structure Rushing :
    Rnk | Player | School | Conf | Games Played | Att (rushing) | Yds (rushing) | Avg (rushing) | TD (rushing) | Rec (receiving) | Yds (receiving) |
    Avg (receiving) | TD (receiving) | Plays (scrimmage)| Yds (scrimmage) | Avg (scrimmage) | TD (scrimmage) | Year |

    * Table structure Receiving :
    Rnk | Player | School | Conf | Games Played | Rec (receiving) | Yds (receiving) | Avg (receiving) | TD (receiving) |  Att (rushing) | Yds (rushing) | 
    Avg (rushing) | TD (rushing) | Plays (scrimmage)| Yds (scrimmage) | Avg (scrimmage) | TD (scrimmage) | Year |

'''
def ScrapeCollegeStats(years,dataTypes):
    RushingPlayers = [] # Holds the data for the running backs
    PassingPlayers = [] # Holds the data for the QB's
    ReceivingPlayers = [] # Holds the data for Recviers 

    for year in years:
        print("Getting data fro " + year + ":")
        for infoType in dataTypes: 
            if (infoType == "Rushing"):
                url = "https://www.sports-reference.com/cfb/years/"+year+"-rushing.html"
                print("Rushing")
            elif (infoType == "Passing"):
                url = "https://www.sports-reference.com/cfb/years/"+year+"-passing.html"
                print("Passing")
            elif (infoType == "Receiving"):
                url = "https://www.sports-reference.com/cfb/years/"+year+"-receiving.html"
                print("Receiving")

            try:
                page = request.urlopen(url)
            except HTTPError as e: 
                print(e)
            except URLError:
                print("The url didn't work the server could be down or the domain no longer exists ")
            else: 
                soup = BeautifulSoup(page, 'html.parser')

                # Get the data from the table 
                if (infoType == "Rushing"):
                    table = soup.find('table', attrs={'id': 'rushing'})
                elif (infoType == "Passing"):
                    table = soup.find('table', attrs={'id': 'passing'})
                elif (infoType == "Receiving"):
                    table = soup.find('table', attrs={'id': 'receiving'})

                tableBody = table.find('tbody').select("tr")

                for row in tableBody :
                    text = row.getText(",")
                    if(text[0].isdigit()): # Makes sure we don't get any table headers in the data
                        text += "," + year
                        text = text.replace(",*,", ",") # cleans the data form something we scrapped 
                        if (infoType == "Rushing"):
                            RushingPlayers.append(text)
                        elif (infoType == "Passing"):
                            PassingPlayers.append(text)
                        elif (infoType == "Receiving"):
                            ReceivingPlayers.append(text)

    print("Rushing Years : \n".join(RushingPlayers))
    print("Passing Years : \n".join(PassingPlayers))
    print("Receiving Years : \n".join(ReceivingPlayers))
    totalPlayers = len(RushingPlayers) + len(PassingPlayers)  + len(ReceivingPlayers)
    print("Total " + str(totalPlayers))

def getYears():
    years = []
    for x in range(1956,2018):
        years.append(str(x))
    return years

years = getYears()
dataTypes = ['Rushing', 'Passing', 'Receiving']
ScrapeCollegeStats(years, dataTypes)


################################################ METHODS FOR THE NFL DATA ################################################
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