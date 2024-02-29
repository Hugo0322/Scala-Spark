package org.example.theoryExamples.chapter6

import org.apache.spark.sql.functions.desc
import org.example.caseClasses.{Usage, UsageCost}
import org.example.fuf.sparkSessionFactory.SparkSessionFactory

import scala.util.Random

object Ej2_CreatingSampleData {

  def run(): Unit = {

    val spark = SparkSessionFactory.createSparkSession("Creating Sample Data")

    import spark.implicits._

    // Our case class for the DataSet
    val r = new Random(42)
    // Creating 1000 instances of scala Usage class
    // This generates data on the fly
    val data = for (i <- 0 to 1000)
      yield (Usage(i, "user-" + r.alphanumeric.take(5).mkString(""),
        r.nextInt(1000)))
    // Creating a Dataset of Usage typed data
    val usageDS = spark.createDataset(data)
    usageDS.show()


    usageDS.filter(d => d.usage > 900)
      .orderBy(desc("usage"))
      .show()

    usageDS.map(u => {if (u.usage > 750) u.usage * 0.15 else u.usage * 0.5}).show()
    // Define a function to compute the usage
    def computeCostUsage(usage: Int): Double = {
      if (usage > 750) usage * 0.15 else usage * 0.5
    }
    // Use the function as the argument to map()
    usageDS.map(u => {computeCostUsage(u.usage)}).show(5, false)


    // Compute the usage cost with Usage as a parameter
    // Return a new object, UsageCost
    def computeUserCostUsage(u: Usage): UsageCost = {
      val v = if (u.usage > 750) u.usage * 0.15 else u.usage * 0.5
      UsageCost(u.uid, u.uname, u.usage, v)
    }
    // Use map() on our original Dataset
    usageDS.map(u => {computeUserCostUsage(u)}).show(5)




  }

}
