import re
import sys
import glob
import glob
# from pyspark import SparkConf, SparkContext
# from pyspark.sql import SparkSession
# from pyspark.sql import functions as F
# from pyspark.sql.types import IntegerType
# from pyspark.ml.feature import Imputer
# from pyspark.ml.feature import HashingTF, IDF, Tokenizer, StringIndexer
# from pyspark.ml.classification import LogisticRegression, DecisionTreeClassifier, NaiveBayes
# from pyspark.ml import Pipeline
# from pyspark.ml.evaluation import MulticlassClassificationEvaluator
# from pyspark.ml.linalg import Vectors
# from pyspark.ml.feature import VectorAssembler

#Method used to grab all of the NFL Draft Data and clean lineman from it
def getNFLDraftData():
    csvFilesPath = []
    for x in glob.glob("./NflDraftData/*.csv"):
        csvFilesPath.append(x)
    
    print(csvFiles)


def getCollegeFootballStats(year):
    players2005 = spark.read.format("csv").option("header", "true").load("collegefootballstatistics/"+year+"/player.csv")
    rushing2005 = spark.read.format("csv").option("header", "true").load("collegefootballstatistics/"+year+"/rush.csv")
    recv2005 = spark.read.format("csv").option("header", "true").load("collegefootballstatistics/"+year+"/reception.csv")
    team2005 = spark.read.format("csv").option("header", "true").load("collegefootballstatistics/"+year+"/team.csv")
    conference2005  = spark.read.format("csv").option("header", "true").load("collegefootballstatistics/"+year+"/conference.csv")
    rushing2005.cache()
    recv2005.cache()

    #get the running backs and rushing Data
    players2005 = players2005.where(players2005.Position == 'RB').drop('Home Town','Home State','Home Country','Last School')
    rushing2005 = rushing2005.withColumn("Attempt", rushing2005.Attempt.cast('float'))
    rushing2005 = rushing2005.withColumn("Yards", rushing2005.Yards.cast('float'))
    rushing2005 = rushing2005.withColumn("Touchdown", rushing2005.Touchdown.cast('float'))
    rushing2005 = rushing2005.withColumn("1st Down", rushing2005['1st Down'].cast('float'))
    rushing2005 = rushing2005.withColumn("Fumble", rushing2005.Fumble.cast('float'))
    rushing2005 = rushing2005.groupBy('Player Code').sum('Attempt', 'Yards', 'Touchdown', '1st Down', 'Fumble')
    rushing2005 = rushing2005.select(rushing2005['Player Code'], rushing2005['sum(Attempt)'].alias('Rush Attempts'), rushing2005['sum(Yards)'].alias('Rushing Yards'), rushing2005['sum(Touchdown)'].alias('Rushing Touchdowns'),
    rushing2005['sum(1st Down)'].alias('Rushing 1st Downs'), rushing2005['sum(Fumble)'].alias('Rushing Fumbles'))
    players2005 = players2005.join(rushing2005, players2005['Player Code'] == rushing2005['Player Code'], 'left_outer' ).drop(rushing2005['Player Code'])

    #Get the recving yards
    recv2005 = recv2005.withColumn("Reception", recv2005.Reception.cast('float'))
    recv2005 = recv2005.withColumn("Yards", recv2005.Yards.cast('float'))
    recv2005 = recv2005.withColumn("Touchdown", recv2005.Touchdown.cast('float'))
    recv2005 = recv2005.withColumn("1st Down", recv2005['1st Down'].cast('float'))
    recv2005 = recv2005.withColumn("Fumble", recv2005['Fumble'].cast('float'))
    recv2005 = recv2005.groupBy('Player Code').sum('Reception', 'Yards', 'Touchdown', '1st Down', 'Fumble')
    recv2005 = recv2005.select(recv2005['Player Code'], recv2005['sum(Reception)'].alias('Receptions'), recv2005['sum(Yards)'].alias('Reception Yards'), recv2005['sum(Touchdown)'].alias('Reception Touchdowns'),
    recv2005['sum(1st Down)'].alias('Reception 1st Downs'), recv2005['sum(Fumble)'].alias('Reception Fumbles'))
    players2005 = players2005.join(recv2005, players2005['Player Code'] == recv2005['Player Code'], 'left_outer' ).drop(recv2005['Player Code'])

    #Git rid of the people who had no rushing or passing yards. Essenatially they didn't play
    #Get the teams and conferences everyone played for 
    players2005 = players2005.dropna(thresh=2, subset= ['Receptions', 'Rush Attempts'])
    players2005 = players2005.join(team2005, players2005['Team Code'] == team2005['Team Code'], 'left_outer' ).withColumn('Team Name', team2005['Name']).drop('Team Code', 'Name')
    players2005 = players2005.join(conference2005, players2005['Conference Code'] == conference2005['Conference Code'], 'left_outer' ).withColumn('Team Conference', conference2005['Name']).drop('Conference Code', 'Name')
    players2005 = players2005.withColumn("Name", F.concat(players2005['First Name'],F.lit(" "),players2005['Last Name'])).drop('First Name', 'Last Name')
    players2005 = players2005.withColumn("Year", F.lit(year))
    return players2005


#Add the college game statics and preprocess the data


#Config
conf = SparkConf()
sc = SparkContext(conf=conf)
spark = SparkSession.builder.appName("Assignment06").config("spark.some.config.option", "some-value").getOrCreate()

draftData = spark.read.format("csv").option("header", "true").load("nfl_draft.csv")
combineData = spark.read.format("csv").option("header", "true").load("combine.csv")

#Filter out and get just the running backs
draftData = draftData.where(draftData.Pos == "RB")
combineData = combineData.where(combineData.position == "RB")


#Get rid of the columns we don't need
draftData = draftData.drop('Player_Id', 'Tm', 'Cmp', 'Pass_Att', 'Pass_TD')
draftData = draftData.drop('Pass_Int', 'Tkl', 'Def_Int', 'Sk', 'Pass_Yds', )
combineData = combineData.drop('arms', 'hands', 'twentyyd', 'tenyd')
#print(draftData.columns)

#Merge the data together to get a list of people who made the draft and undrafted make the draft
drafted = draftData.join(combineData, draftData.Player == combineData.name, 'left_outer')
drafted = drafted.drop('year', 'name', 'firstname', 'lastname', 'position', 'round', 'college', 'pick', 'wonderlic', 'Position Standard', '_c32')
#print("---------------------Count---------------" + str(draftData.count()))
#print("---------------------Count---------------" + str(drafted.count()))
combineView = combineData.createTempView("combine")
draftDataView = draftData.createTempView("draftData")
unDraftedCombine = spark.sql("SELECT * FROM combine LEFT JOIN draftData ON combine.name = draftData.Player WHERE draftData.Player IS NULL")
unDraftedCombine = unDraftedCombine.where(unDraftedCombine.picktotal == 0)
unDraftedCombine = unDraftedCombine.drop('pick', 'pickround', 'picktotal', 'wonderlic', 'Year', 'Rnd', 'Pick', 'Player', 'Pos', 'Position Standard', 'First4AV', 'Age', 'To', 'AP1', 'PB', 'St', 'CarAV', 'DrAV', 'G',
'Rush_Att', 'Rush_Yds', 'Rush_TDs', 'Rec', 'Rec_Yds', 'Rec_Tds', 'College/Univ', '_c32')
# unDrafted = combineData.where(combineData.pickround == 0).where(combineData.picktotal == 0).show(250)
#print("---------------------Filter Count---------------" + str(unDraftedCombine.count()))

#Gather the data from the total college footaball files
players2005 = getCollegeFootballStats("2005")
players2006 = getCollegeFootballStats("2006")
players2007 = getCollegeFootballStats("2007")
players2008 = getCollegeFootballStats("2008")
players2009 = getCollegeFootballStats("2009")
players2010 = getCollegeFootballStats("2010")
players2011 = getCollegeFootballStats("2011")
players2012 = getCollegeFootballStats("2012")
players2013 = getCollegeFootballStats("2013")

#merge all the data together into one big database
allPlayersCollgeStats = players2005.union(players2006)
allPlayersCollgeStats = allPlayersCollgeStats.union(players2007)
allPlayersCollgeStats = allPlayersCollgeStats.union(players2008)
allPlayersCollgeStats = allPlayersCollgeStats.union(players2009)
allPlayersCollgeStats = allPlayersCollgeStats.union(players2010)
allPlayersCollgeStats = allPlayersCollgeStats.union(players2011)
allPlayersCollgeStats = allPlayersCollgeStats.union(players2012)
allPlayersCollgeStats = allPlayersCollgeStats.union(players2013)



#___________TODO____________ DROP FRESHMEN STATS FROM ALL OF THE DATA B/C THEY CANT GO TO THE NFL
#MEGERE ALL DATA TOGETHER INTO ONE BIG DATA SET that has the combine information and the player college stats 
#The issue with above is we will only see who has the chance of being drafted absed off 2005-2013 Data rather than the more recent data

#Find out the collge stats for drafted Players note from 2005 - 2013
drfatedNames = draftData.select('Player')
draftedCollegeStats = allPlayersCollgeStats.join(drfatedNames, drfatedNames['Player'] == allPlayersCollgeStats['Name']).drop('Player')



#Find out the collge stats for the people who went to the combine
combineNames = combineData.select('name')
combineCollegeStats = allPlayersCollgeStats.join(combineNames, combineNames['name'] == allPlayersCollgeStats['Name']).drop(combineNames['name'])


#Find out college stats for Drafted and went to the combine
draftedCombinePlayers = drfatedNames.join(combineNames, drfatedNames['Player'] == combineNames['name']).drop('Player')
draftedtCombineCollegeStats = combineCollegeStats.join(draftedCombinePlayers, draftedCombinePlayers['name'] == combineCollegeStats['Name']).drop(draftedCombinePlayers['name'])

#Find out college stats for unDrafted and went to the combine
unDrafteCombineNames = unDraftedCombine.select('Name')
unDraftedCombineCollegeStats = combineCollegeStats.join(unDrafteCombineNames, unDrafteCombineNames['Name'] == combineCollegeStats['Name']).drop(unDrafteCombineNames['Name'])

#Find out the players who didn't go to the comibne or draft college stats
allPlayersCollgeStats = allPlayersCollgeStats.withColumnRenamed("Name", "Full_Name")
allPlayersCollgeStatsView = allPlayersCollgeStats.createTempView("allPlayersCollgeStats")
drfatedNamesView = drfatedNames.createTempView('draftNames')
combineNamesView = combineNames.createTempView('combineNames')
didntMakeItCollegeStats = spark.sql("SELECT * FROM allPlayersCollgeStats LEFT JOIN draftNames ON allPlayersCollgeStats.Full_Name = draftNames.Player WHERE draftNames.Player IS NULL")
didntMakeItCollegeStatsView = didntMakeItCollegeStats.createTempView("didntMakeItCollegeStats")
didntMakeItCollegeStats = spark.sql("SELECT * FROM didntMakeItCollegeStats LEFT JOIN combineNames ON didntMakeItCollegeStats.Full_Name = combineNames.name WHERE combineNames.name IS NULL")
# didntMakeItCollegeStats = didntMakeItCollegeStats.drop('Player Code', 'Uniform Number','name','Player').show()
#didntMakeItCollegeStats.where(didntMakeItCollegeStats['Height'].isNotNull()).show()

#Merege the simlar data sets
#People who went to the combine and draft
draftedtCombineCollegeStats = draftedtCombineCollegeStats.drop('Player Code','Uniform Number')
unDraftedCombineCollegeStats = unDraftedCombineCollegeStats.drop('Player Code','Uniform Number')

#need to drop the freshmen for a dta set with combine stats and collge stats for people who got drafted and didn't form 2005-2013 or 2003-2015
finalDraft = draftedtCombineCollegeStats
finalUnDraft = unDraftedCombineCollegeStats

#Replace null values with the avg for height and weight
avgHeightDraft = finalDraft.select(F.avg(finalDraft['Height'])).collect()
avgHeightDraft = int(avgHeightDraft[0]['avg(Height)'])
avgWeightDraft =  finalDraft.select(F.avg(finalDraft['Weight'])).collect()
avgWeightDraft = int(avgWeightDraft[0]['avg(Weight)'])
finalDraft = finalDraft.fillna({'Height': avgHeightDraft})
finalDraft = finalDraft.fillna({'Weight': avgWeightDraft})

avgHeightUnDraft = finalUnDraft.select(F.avg(finalUnDraft['Height'])).collect()
avgHeightUnDraft = int(avgHeightUnDraft[0]['avg(Height)'])
avgWeightUnDraft =  finalUnDraft.select(F.avg(finalUnDraft['Weight'])).collect()
avgWeightUnDraft = int(avgWeightUnDraft[0]['avg(Weight)'])
finalUnDraft = finalUnDraft.fillna({'Height': avgHeightUnDraft})
finalUnDraft = finalUnDraft.fillna({'Weight': avgWeightUnDraft})

#Make indentifers for people who got drafted adn people who didnt get drafted
finalDraft = finalDraft.withColumn("Drafted", finalDraft["Rushing Touchdowns"]-finalDraft["Rushing Touchdowns"]+1) #367
finalUnDraft = finalUnDraft.withColumn("Drafted", finalUnDraft["Rushing Touchdowns"]-finalUnDraft["Rushing Touchdowns"]) #219

#need to drop the freshmen for a dta set with combine stats and collge stats for people who got drafted and didn't form 2005-2013 or 2003-2015
# finalDraft = draftedtCombineCollegeStats.where(draftedtCombineCollegeStats["Class"] != "FR")
# finalUnDraft = unDraftedCombineCollegeStats.where(unDraftedCombineCollegeStats["Class"] != "FR")

#Merge the two datasets together
df = finalDraft.union(finalUnDraft)
df.cache() #Before merge by names 586 after there is 233

#merge people with the same names together
df = df.groupBy('Name').agg({'Height': 'mean', 'Weight': 'mean', 'Rush Attempts':'sum', 'Rushing Yards':'sum', 'Rushing Touchdowns': 'sum', 'Rushing 1st Downs':'sum', 'Rushing Fumbles': 'sum',
        'Receptions':'sum', 'Reception Yards': 'sum', 'Reception Touchdowns':'sum', 'Reception 1st Downs':'sum', 'Reception Fumbles':'sum', 'Drafted':'avg'})

#Add  Team Name, Subdivision, Team Conference
otherData = combineCollegeStats.select('Team Name', 'Subdivision', 'Team Conference', 'Name')
df = df.join(otherData, df['Name'] == otherData['Name']).drop(otherData['Name'])
df = df.dropDuplicates()

#properly rename all feilds
df = df.withColumnRenamed('sum(Rush Attempts)', 'Rush Attempts')
df = df.withColumnRenamed('sum(Reception Yards)', 'Reception Yards')
df = df.withColumnRenamed('sum(Rushing 1st Downs)', 'Rushing 1st Downs')
df = df.withColumnRenamed('sum(Rushing Yards)', 'Rushing Yards')
df = df.withColumnRenamed('sum(Rushing Fumbles)', 'Rushing Fumbles')
df = df.withColumnRenamed('sum(Reception Touchdowns)', 'Reception Touchdowns')
df = df.withColumnRenamed('avg(Weight)', 'Weight')
df = df.withColumnRenamed('sum(Reception 1st Downs)', 'Reception 1st Downs')
df = df.withColumnRenamed('avg(Drafted)', 'Drafted')
df = df.withColumnRenamed('sum(Receptions)', 'Receptions')
df = df.withColumnRenamed('sum(Rushing Touchdowns)', 'Rushing Touchdowns')
df = df.withColumnRenamed('avg(Height)', 'Height')
df = df.withColumnRenamed('sum(Reception Fumbles)', 'Reception Fumbles')

df = df.withColumn("Subdivision", F.when(df['Subdivision'] == "FBS", 1).otherwise(2))
#Michael Smith


#NOTE MAY HAVE TO FIND YARDS PER CARY


####Apply several ML functions on the dataset and see which yeilds best results####

#Chagne strigns to indexs
indexer1 = StringIndexer(inputCol="Team Name", outputCol="TeamIndex")
indexer3 = StringIndexer(inputCol="Team Conference", outputCol="ConferenceIndex")
df = indexer1.fit(df).transform(df)
df = indexer3.fit(df).transform(df)

df.show()
df = df.drop('Name')
assembler = VectorAssembler(
    inputCols=["Rush Attempts", "Reception Yards", "Rushing 1st Downs", "Rushing Yards", "Rushing Fumbles", "Reception Touchdowns", "Weight", "Reception 1st Downs", "Receptions",
                "Rushing Touchdowns", "Height", "Reception Fumbles", "Subdivision", "TeamIndex", "ConferenceIndex"],
    outputCol="features")



# assembler = VectorAssembler(
#     inputCols=["Rush Attempts", "Reception Yards", "Rushing 1st Downs", "Rushing Yards", "Rushing Fumbles", "Reception Touchdowns", "Weight", "Reception 1st Downs", "Drafted", "Receptions",
#                 "Rushing Touchdowns", "Height", "Reception Fumbles", "Subdivision", "TeamIndex", "ConferenceIndex"],
#     outputCol="features")    
df = assembler.transform(df)


(trainData,  testData) = df.randomSplit([0.7,0.3])
trainData.cache()
testData.cache()

#-----------------------LogisticRegression--------------------------------
#Modles to test on the dataset
# lr = LogisticRegression(maxIter=5, regParam=0.3, elasticNetParam=0.8, labelCol="Drafted")
# #fit model
# lrModel = lr.fit(trainData)

# #make predictions
# predictions = lrModel.transform(testData)
# #predictions = lrModel.evaluate(testData)
# predictions.show(100)
# evaluator = MulticlassClassificationEvaluator(
#     labelCol="Drafted", predictionCol="prediction", metricName="accuracy")
# accuracy = evaluator.evaluate(predictions)
# print("Test set accuracy = " + str(accuracy))
# print("Test Error = %g " % (1.0 - accuracy))

#-----------------------NaiveBayes--------------------------------
# nb = NaiveBayes(smoothing=1.0, labelCol="Drafted")
# nbModel = nb.fit(trainData)
# predictions = nbModel.transform(testData)
# predictions.show(100)
# evaluator = MulticlassClassificationEvaluator(labelCol="categoryIndex", predictionCol="prediction", metricName="accuracy")
# accuracy = evaluator.evaluate(predictions)
# print("Test set accuracy = " + str(accuracy))
# print("Test Error = %g " % (1.0 - accuracy))

#-----------------------Decision tree--------------------------------
dt = DecisionTreeClassifier(labelCol="Drafted",maxBins=150)
dtModel = dt.fit(trainData)
# Make predictions.
predictions = dtModel.transform(testData)

# Select example rows to display.
predictions.show(100)

# Select (prediction, true label) and compute test error
evaluator = MulticlassClassificationEvaluator(labelCol="Drafted", predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print("Test set accuracy = " + str(accuracy))
print("Test Error = %g " % (1.0 - accuracy))