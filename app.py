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
from pyspark.sql.functions import *
from pyspark.sql.functions import col, udf, array, when
from pyspark.sql.types import IntegerType, DoubleType
from pyspark.ml import Pipeline
from pyspark.ml.feature import Imputer, OneHotEncoderEstimator, StringIndexer, VectorAssembler
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier, LinearSVC, DecisionTreeClassifier, NaiveBayes


def ComputeAvg(x):
    if x[1] == 0:
        return 0
    else:
        return x[0]/ x[1]

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
    imputer = Imputer(inputCols=["Age"], outputCols=["Age"])   
    cleanData = imputer.setStrategy("median").fit(unCleanData).transform(unCleanData)
    #cleanData.show()
    return cleanData

def cleanCollegeData(postion):
    '''
        * One Flaw to think of if a player graduated in the year 2002 that means we are missing data for the year 1999 since we start at the year 2000
          Need to find a clever way around this so the data won't be messed up --> [SOLUTION] When scraping get a few years before target NFL year that way we see
          full development of a player
        [X] Merge the two Dataframes together

    '''
    if(postion == "RB"):
        unCleanData = spark.read.format("csv").option("header", "true").option("inferSchema","true").load("./data/CollegeStatsData/RushingData.csv")
        # Drop Avgs since we will recompute them once we combine all similar player together
        unCleanData = unCleanData.drop('Rnk', 'Avg (rushing)','Avg (receiving)','Avg (scrimmage)')
    
        # If there are four nulls in Rec (receiving), Yds (receiving), Yds (receiving), TD (receiving) Drop them they are a QB
        unCleanData = unCleanData.filter((unCleanData['Rec (receiving)'] != 'null') & (unCleanData['Yds (receiving)'] != 'null') &  (unCleanData['TD (receiving)'] != 'null'))

        # Cast values to double
        doubleCols = ['Games Played', 'Att (rushing)', 'Yds (rushing)', 'TD (rushing)','Rec (receiving)','Yds (receiving)',
                      'TD (receiving)', 'Plays (scrimmage)', 'Yds (scrimmage)','TD (scrimmage)']
        
        for c in doubleCols:
            unCleanData = unCleanData.withColumn(c, unCleanData[c].cast(DoubleType()))
        
        # Fill nulls with 0's
        unCleanData = unCleanData.na.fill(0)
        
        # Merge rows with the same name and add elements together
        unCleanDataGrouped = unCleanData.groupBy("Player",'School').sum('Games Played', 'Att (rushing)', 'Yds (rushing)', 'TD (rushing)','Rec (receiving)','Yds (receiving)',
                      'TD (receiving)', 'Plays (scrimmage)', 'Yds (scrimmage)','TD (scrimmage)')

        # Compute new Avgs
        averageFunc = udf(lambda array: ComputeAvg(array), DoubleType())
        # unCleanDataGrouped.show(1000)
        # exit()
        unCleanDataGrouped = unCleanDataGrouped.withColumn("Avg (rushing)", averageFunc(array(unCleanDataGrouped['sum(Yds (rushing))'], unCleanDataGrouped['sum(Att (rushing))'])))
        unCleanDataGrouped = unCleanDataGrouped.withColumn("Avg (receiving)", averageFunc(array(unCleanDataGrouped['sum(Yds (receiving))'], unCleanDataGrouped['sum(Rec (receiving))'])))
        unCleanDataGrouped = unCleanDataGrouped.withColumn("Avg (scrimmage)", averageFunc(array(unCleanDataGrouped['sum(Yds (scrimmage))'], unCleanDataGrouped['sum(Plays (scrimmage))'])))
        unCleanDataGrouped = unCleanDataGrouped.na.fill(0)

        # Rename cols 
        unCleanDataGrouped = unCleanDataGrouped.select( col("Player"), col("School"), col("sum(Games Played)").alias("Games Played"), col("sum(Att (rushing))").alias("Att (rushing)"),
        col("sum(Yds (rushing))").alias("Yds (rushing)"), col("Avg (rushing)"), col("sum(TD (rushing))").alias("TD (rushing)"), col("sum(Rec (receiving))").alias("Rec (receiving)"),
        col("sum(Yds (receiving))").alias("Yds (receiving)"), col("Avg (receiving)"), col("sum(TD (receiving))").alias("TD (receiving)"), col("sum(Plays (scrimmage))").alias("Plays (scrimmage)"),
        col("sum(Yds (scrimmage))").alias("Yds (scrimmage)"), col("Avg (scrimmage)"), col("sum(TD (scrimmage))").alias("TD (scrimmage)") )
        #unCleanDataGrouped.show()
        
        # Get the players team and conference of their final year
        unCleanDataTeams = unCleanData.groupBy('Player','School').agg(F.max("Year").alias("Year"))

        # Merge dataframes together
        df1 = unCleanDataGrouped.alias('df1')
        df2 = unCleanDataTeams.alias('df2')
        cleanData = df1.join(df2, (df1.Player == df2.Player) & (df1.School == df2.School)).select('df1.*','df2.Year')
        #cleanData.show()

        return cleanData

    elif(postion == "WR"):
        unCleanData = spark.read.format("csv").option("header", "true").option("inferSchema","true").load("./data/CollegeStatsData/ReceivingData.csv")
    elif(postion == "QB"):
        unCleanData = spark.read.format("csv").option("header", "true").option("inferSchema","true").load("./data/CollegeStatsData/PassingData.csv")

    

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
    #cleanData.show()
    return cleanData

def prepDataForML(df):
    # https://towardsdatascience.com/machine-learning-with-pyspark-and-mllib-solving-a-binary-classification-problem-96396065d2aa
    cols = df.columns

    categoricalColumns = ['School']
    stages = []
    for categoricalCol in categoricalColumns:
        # indexes each categorical column using the StringIndexer
        stringIndexer = StringIndexer(inputCol = categoricalCol, outputCol = categoricalCol + 'Index')
        #  converts the indexed categories into one-hot encoded variables
        encoder = OneHotEncoderEstimator(inputCols=[stringIndexer.getOutputCol()], outputCols=[categoricalCol + "classVec"])
        stages += [stringIndexer, encoder]
    
    # StringIndexer again to encode our labels to label indices
    label_stringIdx = StringIndexer(inputCol = 'Drafted', outputCol = 'label')
    stages += [label_stringIdx]

    # VectorAssembler to combine all the feature columns into a single vector column
    numericCols = ['Games Played', 'Att (rushing)', 'Yds (rushing)', 'Avg (rushing)', 'TD (rushing)', 'Rec (receiving)', 'Yds (receiving)',
                    'Avg (receiving)', 'TD (receiving)', 'Plays (scrimmage)', 'Yds (scrimmage)', 'Avg (scrimmage)', 'TD (scrimmage)', 'Year']
    assemblerInputs = [c + "classVec" for c in categoricalColumns] + numericCols
    assembler = VectorAssembler(inputCols=assemblerInputs, outputCol="features")
    stages += [assembler]
    
    # PipeLine for the ML data to follow
    pipeline = Pipeline(stages = stages)
    pipelineModel = pipeline.fit(df)
    df = pipelineModel.transform(df)
    selectedCols = ['label', 'features'] + cols
    df = df.select(selectedCols)
    #df.printSchema()
    #df.show()

    # Randomly split data into train and test sets, and set seed for reproducibility.
    train, test = df.randomSplit([0.7, 0.3], seed = 2018)
    train.cache()
    test.cache()
    # print("Training Dataset Count: " + str(train.count()))
    # print("Test Dataset Count: " + str(test.count()))

    # Apply machine learing to it 

    # Logistic Regression --> ROC: 85%, Accuracy: 88.73239436619719%
    # lr = LogisticRegression(featuresCol = 'features', labelCol = 'label', maxIter=10)
    # lrModel = lr.fit(train)
    # predictions = lrModel.transform(test)
    # evaluator = BinaryClassificationEvaluator()
    # print('Test Area Under ROC', evaluator.evaluate(predictions) * 100)
    # evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
    # accuracy = evaluator.evaluate(predictions)
    # print("Test set accuracy = " + str(accuracy))

    # Random Forest --> ROC: 83.90126725368875%, Accuracy: 90.02347417840375%
    # rf = RandomForestClassifier(labelCol="label", featuresCol="features", numTrees=10)
    # rfModel = rf.fit(train)
    # predictions = rfModel.transform(test)
    # # predictions.show(1000)
    # evaluator = BinaryClassificationEvaluator()
    # print("Test Area Under ROC: " + str(evaluator.evaluate(predictions, {evaluator.metricName: "areaUnderROC"})))
    # evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
    # accuracy = evaluator.evaluate(predictions)
    # print("Test set accuracy = " + str(accuracy))

    # Linear SVM --> ROC: 77.35899571632558, Accuracy: 89.67136150234741%
    # lsvc = LinearSVC(maxIter=10, regParam=0.1)
    # lsvcModel = lsvc.fit(train)
    # predictions = lsvcModel.transform(test)
    # evaluator = BinaryClassificationEvaluator()
    # print('Test Area Under ROC', evaluator.evaluate(predictions) * 100)
    # evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
    # accuracy = evaluator.evaluate(predictions)
    # print("Test set accuracy = " + str(accuracy))

    # Decision Tree --> Area Under ROC: 83.31151832460733, Accuracy: 86.97183098591549%
    dt = DecisionTreeClassifier(labelCol="label", featuresCol="features")
    dtModel = dt.fit(train)
    predictions = dtModel.transform(test)
    evaluator = BinaryClassificationEvaluator()
    print('Test Area Under ROC', evaluator.evaluate(predictions) * 100)
    evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
    accuracy = evaluator.evaluate(predictions)
    print("Test set accuracy = " + str(accuracy))
    
    # Naive Bayes --> Does not work right now b/c we have some negative numbers in our data
    # nb = NaiveBayes(smoothing=1.0, modelType="multinomial")
    # nbModle = nb.fit(train)
    # predictions = nbModle.transform(test)
    # evaluator = BinaryClassificationEvaluator()
    # print('Test Area Under ROC', evaluator.evaluate(predictions) * 100)
    # evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
    # accuracy = evaluator.evaluate(predictions)
    # print("Test set accuracy = " + str(accuracy))

def main():
    getData()

main()


#Config
conf = SparkConf()
sc = SparkContext(conf=conf)
spark = SparkSession.builder.appName("NflDraftApp").config("spark.some.config.option", "some-value").getOrCreate()
nflDF =  cleanDraftData("RB") # takes in either RB, QB, WR since these are the main postions we can analytics on 
                            # Note from 2000 - 2018 we have 371 RBs, 223 QBs, 585 WRs
collegeDF = cleanCollegeData("RB")
#combineDF = cleanCombineData("RB")

# Merged data from the NFL and College Data 1 if they got drafted 0 otherwise NOTE (Yes = 1, No = 0)
draftedDataFrame = collegeDF.withColumn("Player", col("Player")).alias("College")\
    .join(nflDF.withColumn("Player", col("Player Name")).alias("NFL"), on="Player", how="left")\
    .select("College.*", when(col("NFL.Player Name").isNotNull(), "Yes").otherwise("No").alias("Drafted"))
draftedDataFrame.cache()
#draftedDataFrame.show(1000)
prepDataForML(draftedDataFrame)