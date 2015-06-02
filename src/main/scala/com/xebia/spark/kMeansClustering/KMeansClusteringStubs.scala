package com.xebia.spark.kMeansClustering

import com.xebia.spark.kMeansClustering.features.Engineering.featureEngineering
import com.xebia.spark.kMeansClustering.tools.Utilities.clustersInfo
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering.{KMeansModel, KMeans}

object KMeansClusteringStubs {

   def main(args: Array[String]) {

     val conf = new SparkConf().setAppName("KMeans").setMaster("local[4]").set("spark.executor.memory", "6g")
     val sc = new SparkContext(conf)

     // Loading data
     // TODO : read file ./src/main/resources/data_titanic.csv

     // Feature Engineering
     // TODO : use the featureEngineering method in features/Engineering to get the cleaned data.

     // Modelling
     // TODO : Train a KMeans model on the data set

     // Inspect centroid of each cluster
     println("Clusters description")
     // TODO : For each cluster, print the centroid information. You can use the clustersInfo method in tools/Utilities


   }

 }
