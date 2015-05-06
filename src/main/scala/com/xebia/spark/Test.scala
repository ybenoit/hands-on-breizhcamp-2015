package com.xebia.spark

import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.clustering.{KMeansModel, KMeans}
import org.apache.spark.mllib.linalg.{Vector, Vectors}

/**
 * Created by Yoann on 05/05/15.
 */
object Test {

  val conf = new SparkConf().setAppName("RandomForest").setMaster("local[4]").set("spark.executor.memory", "6g")
  val sc = new SparkContext(conf)

  // Load and parse the data
  val data: RDD[String] = sc.textFile("data/mllib/kmeans_data.txt")
  val parsedData: RDD[Vector] = data.map(s => Vectors.dense(s.split(' ').map(_.toDouble))).cache()

  // Cluster the data into two classes using KMeans
  val clusters: KMeansModel = KMeans.train(parsedData, k = 2, maxIterations = 20)

  // Evaluate clustering by computing Within Set Sum of Squared Errors
  val WSSSE: Double = clusters.computeCost(parsedData)
  println("Within Set Sum of Squared Errors = " + WSSSE)


}
