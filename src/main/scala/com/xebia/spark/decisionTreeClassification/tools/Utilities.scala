package com.xebia.spark.decisionTreeClassification.tools

import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.rdd.RDD


object Utilities {

  /**
   *
   * @param model A DecisionTreeModel from the method DecisionTree.trainClassifier()
   * @param data the data (a RDD[LabeledPoint])
   * @return A tuple giving the accuracy and the confusion matrix
   */
  def accuracyDecisionTree(model: DecisionTreeModel, data: RDD[LabeledPoint]): Double = {

    val predictionsAndLabels = data.map(l => (model.predict(l.features), l.label))

    val metrics: MulticlassMetrics = new MulticlassMetrics(predictionsAndLabels)

    val accuracy = 100d * metrics.precision

    accuracy
  }

}
