package org.example.theoryExamples.chapter6

import org.example.caseClasses.Bloggers
import org.example.fuf.sparkSessionFactory.SparkSessionFactory


object Ej1_ScalaCaseClase {

  def run(fileInjected: String): Unit = {

    val spark = SparkSessionFactory.createSparkSession("Scala Case Clases for Datasets")

    import spark.implicits._

    val bloggersDS = spark.read.json(fileInjected).as[Bloggers]

    bloggersDS.show()

  }

}
