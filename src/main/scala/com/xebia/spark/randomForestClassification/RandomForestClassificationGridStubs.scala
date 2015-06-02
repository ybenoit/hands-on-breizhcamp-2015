package com.xebia.spark.randomForestClassification


import com.xebia.spark.randomForestClassification.features.Engineering
import com.xebia.spark.randomForestClassification.tools.Utilities._
import org.apache.spark.{SparkContext, SparkConf}


object RandomForestClassificationGridStubs {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("RandomForest").setMaster("local[4]").set("spark.executor.memory", "6g")
    val sc = new SparkContext(conf)

    // Loading data
    // TODO : read file ./src/main/resources/data_titanic.csv

    // Feature Engineering
    // TODO : use the featureEngineering method in features/Engineering to get the cleaned data

    // Splitting data
    // TODO : split the cleaned data in a train, validation and test set (proportions 0.60, 0.15, 0.25) using the 'randomSplit' method

    // Modelling
    // -------- Tuning parameters
    val categoricalFeaturesInfo = Map(1 -> 2, 6 -> 4)
    val featuresSubsetStrategy = "auto"
    // TODO: Implement a grid search to search for the best parameters, using the train and validation sets
    // (You can use the given gridSearchRandomForestClassifier method in tools/Utilities)

    // -------- Training the model
    // TODO : Train a RandomForest model on the training set (Use the best parameters found)

    // Prediction & Evaluation
    // TODO : get the precision for the prediction on the test set (You can use the accuracyRandomForest method in tools/Utilities)

    // Print results
    // TODO: Print the best parameters found during grid search

    println(s"Results for the test set")
    // TODO : print the results obtained for the test set

  }

}
