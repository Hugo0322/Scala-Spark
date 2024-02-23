package org.example.chapter7

import org.apache.spark.sql.SparkSession

object Ej1_SparkConfigModifications {

  def printConfigs(session: SparkSession) = {
    // Get conf
    val mconf = session.conf.getAll
    // Print conf
    for (k <- mconf.keySet) { println(s"${k} -> ${mconf(k)}\n")}
  }

  def run(sessionName: String): Unit = {

    // Creating SparkSession
    val spark = SparkSession.builder()
      .config("spark.sql.shuffle.partitions", 5)
      .config("spark.executor.memory", "2g")
      .master("local[*]")
      .appName(sessionName)
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    printConfigs(spark)
    spark.conf.set("spark.sql.shuffle.partitions", spark.sparkContext.defaultParallelism)
    println(" ****** Setting Shuffle Partitions to Default Parallelism")
    printConfigs(spark)

    spark.sql("SET -v").select("key", "value").show(5, false)

  }


}
