package com.h2ospark

import org.apache.spark.sql.SparkSession

import hex.genmodel.easy.{EasyPredictModelWrapper, RowData}
import hex.genmodel.MojoModel
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode

object BinaryModels {

  def main(args: Array[String])
  {

    // args(0) : path ruta modelo h2o de regresion
    // args(1) : path completo tablon a predecir
    // args(2) : path completo tablon_predict

    val modelPath = args(0)
    val tableIn = args(1)
    val tableOut = args(2)


    val spark = SparkSession.builder
    //  .master("local[*]")
      .appName("binomial_predict")
      .getOrCreate()
    import spark.implicits._


    val tabla_origin = spark.table(tableIn)

    // cast all columns to string for h2o compatibility
    val tabla = tabla_origin.select(tabla_origin.columns.map(c => col(c).cast(StringType)) : _*)

    // Import MOJO model
    val mojo = MojoModel.load(modelPath)

    val easyModel = new EasyPredictModelWrapper(
      new EasyPredictModelWrapper.Config().
        setModel(mojo).
        setConvertUnknownCategoricalLevelsToNa(true).
        setConvertInvalidNumbersToNa(true))

    val header = tabla.columns

    // Predict and save as dataframe
    val dfScore = tabla.map {
      x =>
        val r = new RowData
        header.indices.foreach(idx => r.put(header(idx), x.getAs[String](idx) ))
        val score = easyModel.predictBinomial(r).classProbabilities
        (x.getAs[String](0), score(1))
    }.toDF("columna1","predict")

    // Print Schema and show datos
    //dfScore.printSchema
    //dfScore.show()

    // Save in a specific table
    dfScore.write.mode(SaveMode.Overwrite).saveAsTable(tableOut)
  }
}