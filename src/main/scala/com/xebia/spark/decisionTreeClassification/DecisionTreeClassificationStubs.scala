package com.xebia.spark.decisionTreeClassification


import com.xebia.spark.decisionTreeClassification.features.Engineering
import com.xebia.spark.decisionTreeClassification.tools.Utilities._
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.{SparkContext, SparkConf}


object DecisionTreeClassificationStubs {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("RandomForest").setMaster("local[4]").set("spark.executor.memory", "6g")
    val sc = new SparkContext(conf)

    // Loading data
    // TODO : read file ./src/main/resources/data_titanic.csv

    // Feature Engineering
    // TODO : use the featureEngineering method in features/Engineering to get the cleaned data
    // Be carefull, you will get a RDD[LabeledPoint]

    // Splitting data
    // TODO : split the cleaned data into a train and test set (proportions 0.75, 0.25) using the 'randomSplit' method on the initial RDD

    // Modelling
    // -------- Tuning parameters
    val numClass = 2
    val categoricalFeaturesInfo = Map(1 -> 2, 6 -> 4)
    val impurity: String = "entropy"
    val maxDepth: Int = 2
    val maxBins: Int = 12

    // -------- Training the model
    // TODO : Train a DecisionTree model on the training set (Use the parameters of your choice)

    // Prediction & Evaluation
    // TODO : get the precision for the prediction on the test set (You can use the accuracyDecisionTree method in tools/Utilities)

    // Print results
    println(s"Results for the test set")
    // TODO : print the results obtained for the test set


  }




}
