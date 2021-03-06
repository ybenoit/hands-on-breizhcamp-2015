package com.xebia.spark.decisionTreeClassification.features

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD


object Engineering {

  def featureEngineering(data : RDD[String]): RDD[LabeledPoint] = {

    data.map(line => {

      val values = line.split('§')

      val label = values(1).toDouble

      val pClass = values(0).toDouble
      val sex = values(3) match {
        case "\"male\"" => 0d
        case "\"female\"" => 1d
      }
      val age = values(4) match {
        case "NA" => 28d
        case l => l.toDouble
      }
      val sibsp = values(5).toDouble
      val parch = values(6).toDouble
      val fair = values(8) match {
        case "NA" => 14.45
        case l => l.toDouble
      }
      val embarked = values(10) match {
        case "\"\"" => 0d
        case "\"C\"" => 1d
        case "\"Q\"" => 2d
        case "\"S\"" => 3d
      }

      val cleanedData = Array(pClass, sex, age, sibsp, parch, fair, embarked)

      LabeledPoint(label, Vectors.dense(cleanedData))
    })

  }





}
