package org.example.chapter6

import org.example.commonlyUsedSharedFolder.caseClasses.Bloggers
import org.example.commonlyUsedSharedFolder.sparkSessionFactory.SparkSessionFactory


object Ej1_ScalaCaseClase {

  def run(fileInjected: String): Unit = {

    val spark = SparkSessionFactory.createSparkSession("Scala Case Clases for Datasets")

    import spark.implicits._

    val bloggersDS = spark.read.json(fileInjected).as[Bloggers]

    bloggersDS.show()

  }

}
