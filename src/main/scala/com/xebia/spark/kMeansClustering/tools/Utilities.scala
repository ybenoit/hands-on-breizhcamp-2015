package com.xebia.spark.kMeansClustering.tools

import org.apache.spark.mllib.clustering.KMeansModel

object Utilities {

  /**
   * Gives the centroids information of a KMeansModel
   * @param model A KMeansModel from the method KMeans.train()
   * @return The centroids and the proportion of survivors
   */
  def clustersInfo(model: KMeansModel) = {

    val centroids = model.clusterCenters

    centroids.foreach(l => println(s"Class: ${l(0).toInt}, Age: ${l(1).toInt}, Fair: ${l(2).toInt}"))
  }

}
