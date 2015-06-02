package com.xebia.spark.randomForestClassification


import com.xebia.spark.randomForestClassification.features.Engineering
import org.apache.spark.mllib.tree.RandomForest
import com.xebia.spark.randomForestClassification.tools.Utilities._
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.{SparkContext, SparkConf}


object RandomForestClassificationGridSolution {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("RandomForest").setMaster("local[4]").set("spark.executor.memory", "6g")
    val sc = new SparkContext(conf)

    // Loading data
    val rawData = sc.textFile("./src/main/resources/data_titanic.csv")

    // Feature Engineering
    val cleanData = Engineering.featureEngineering(rawData)

    // Splitting data
    val Array(trainSet, valSet, testSet) = cleanData.randomSplit(Array(0.6, 0.15, 0.25), seed = 54)

    // Modelling
    // -------- Tuning parameters
    val categoricalFeaturesInfo = Map(1 -> 2, 6 -> 4)
    val featuresSubsetStrategy = "auto"
    val bestParams = gridSearchRFClassifier(trainSet, valSet,
      categoricalFeaturesInfo = categoricalFeaturesInfo, numTreesGrid = Array(50, 100),
      impurityGrid = Array("entropy", "gini"), maxDepthGrid = Array(5, 10), maxBinsGrid = Array(50, 100))

    // -------- Training the model
    val dataTrain = sc.union(trainSet, valSet)
    val (numTrees, impurity, maxDepth, maxBins) = bestParams
    val model: RandomForestModel = RandomForest.trainClassifier(dataTrain, 2, categoricalFeaturesInfo, numTrees, featuresSubsetStrategy, impurity, maxDepth, maxBins)

    // Prediction & Evaluation
    val accuracyTest = accuracyRandomForest(model, testSet)

    // Print results
    println(s"Best parameters found : \n $bestParams \n")

    println(s"Results for the test set")
    println(s"\t Accuracy: $accuracyTest %")

  }

}
