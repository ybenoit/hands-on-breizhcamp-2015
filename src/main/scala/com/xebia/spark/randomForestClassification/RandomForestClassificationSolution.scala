package com.xebia.spark.randomForestClassification


import com.xebia.spark.randomForestClassification.features.Engineering
import com.xebia.spark.randomForestClassification.tools.Utilities._
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.{SparkContext, SparkConf}


object RandomForestClassificationSolution {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("RandomForest").setMaster("local[4]").set("spark.executor.memory", "6g")
    val sc = new SparkContext(conf)

    // Loading data
    val rawData = sc.textFile("./src/main/resources/data_titanic.csv")

    // Feature Engineering
    val cleanData = Engineering.featureEngineering(rawData)

    // Splitting data
    val Array(trainSet, testSet) = cleanData.randomSplit(Array(0.75, 0.25), seed=54)

    // Modelling
    // -------- Tuning parameters
    val numClass = 2
    val categoricalFeaturesInfo = Map(1 -> 2, 6 -> 4)
    val numTrees = 50
    val featureSubsetStrategy = "auto"
    val impurity = "entropy"
    val maxDepth = 2
    val maxBins = 12

    // -------- Training the model
    val model: RandomForestModel = RandomForest.trainClassifier(trainSet, numClass, categoricalFeaturesInfo, numTrees,
      featureSubsetStrategy, impurity, maxDepth, maxBins)

    // Prediction & Evaluation
    val accuracyTest = accuracyRandomForest(model, testSet)

    // Print results
    println(s"Results for the test set")
    println(s"\t Accuracy: $accuracyTest %")


  }




}
