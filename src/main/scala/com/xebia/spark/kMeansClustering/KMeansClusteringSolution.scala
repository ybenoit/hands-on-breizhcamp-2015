package com.xebia.spark.kMeansClustering

import com.xebia.spark.kMeansClustering.features.Engineering.featureEngineering
import com.xebia.spark.kMeansClustering.tools.Utilities
import com.xebia.spark.kMeansClustering.tools.Utilities._
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering.{KMeansModel, KMeans}

object KMeansClusteringSolution {

  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("KMeans").setMaster("local[4]").set("spark.executor.memory", "6g")
    val sc = new SparkContext(conf)

    // Loading data
    val rawData: RDD[String] = sc.textFile("./src/main/resources/data_titanic.csv")

    // Feature Engineering
    val cleanData: RDD[Vector] = featureEngineering(rawData)

    // Modelling
    val model: KMeansModel = KMeans.train(cleanData, 2, 50)

    // Inspect centroid of each cluster
    println("Clusters description")
    Utilities.clustersInfo(model)

  }

}
