package com.xebia.spark.decisionTreeClassification


import com.xebia.spark.decisionTreeClassification.features.Engineering
import com.xebia.spark.decisionTreeClassification.tools.Utilities._
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.{SparkContext, SparkConf}


object DecisionTreeClassificationSolution {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("RandomForest").setMaster("local[4]").set("spark.executor.memory", "6g")
    val sc = new SparkContext(conf)

    // Loading data
    val rawData = sc.textFile("./src/main/resources/data_titanic.csv")

    // Feature Engineering
    val cleanData = Engineering.featureEngineering(rawData)

    // Splitting data
    val Array(trainSet, testSet) = cleanData.randomSplit(Array(0.75, 0.25), seed = 54)

    // Modelling
    // -------- Tuning parameters
    val numClass = 2
    val categoricalFeaturesInfo = Map(1 -> 2, 6 -> 4)
    val impurity: String = "entropy"
    val maxDepth: Int = 2
    val maxBins: Int = 12

    // -------- Training the model
    val model = DecisionTree.trainClassifier(trainSet, numClass, categoricalFeaturesInfo, impurity, maxDepth, maxBins)

    // Prediction & Evaluation
    val accuracyTest = accuracyDecisionTree(model, testSet)

    // Print results
    println(s"Results for the test set")
    println(s"\t Accuracy: $accuracyTest %")


  }




}
