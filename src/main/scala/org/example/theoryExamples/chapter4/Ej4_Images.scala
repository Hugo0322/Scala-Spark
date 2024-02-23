package org.example.theoryExamples.chapter4

import org.example.fuf.sparkSessionFactory.SparkSessionFactory

object Ej4_Images {

  def run(fileInjected: String): Unit = {

    val spark = SparkSessionFactory.createSparkSession("Images")

    /*
        val imagesDF = spark.read.format("image").load(fileInjected)

        imagesDF.printSchema

        imagesDF.select("image.height", "image.width", "image.nChannels", "image.mode", "label").show(5, false)
     */

    val binaryFileDF = spark.read.format("binaryFile").option("pathGlobFilter", "*.jpg").load(fileInjected)

    binaryFileDF.show(5)

  }

}
