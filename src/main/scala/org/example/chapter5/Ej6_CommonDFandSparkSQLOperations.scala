package org.example.chapter5

import org.apache.spark.sql.catalyst.dsl.expressions.{DslAttr, StringToAttributeConversionHelper}
import org.apache.spark.sql.functions.expr
import org.example.commonlyUsedSharedFolder.sparkSessionFactory.SparkSessionFactory


object Ej6_CommonDFandSparkSQLOperations {

  def run(delaysFile: String, airportsFile: String): Unit = {

    val spark = SparkSessionFactory.createSparkSession("Common DataFrame and SparkSQL Operations")

    // Obtain airports data set
    val airports = spark.read
      .option("header", "true")
      .option("inferschema", "true")
      .option("delimeter", " \t")
      .csv(airportsFile)

    airports.createOrReplaceTempView("airports_na")

    // Obtain departure Delays data set
    val delays = spark.read
      .option("header", true)
      .csv(delaysFile)
      .withColumn("delay", expr("CAST(delay AS INT) AS delay"))
      .withColumn("distance", expr("CAST(distance AS INT) AS distance"))

    delays.createOrReplaceTempView("departureDelays")

    // Create temp small table
    val foo = delays.filter(
      expr("""origin == 'SEA' AND destination == 'SFO' AND date like '01010%' AND delay > 0""")
    )
    foo.createOrReplaceTempView("foo")

    spark.sql("SELECT * FROM airports_na LIMIT 10").show()
    spark.sql("SELECT * FROM departureDelays LIMIT 10").show()
    spark.sql("SELECT * FROM foo LIMIT 10").show()

    // Unions
    val bar = delays.union(foo)
    bar.createOrReplaceTempView("bar")
    bar.filter("""origin == 'SEA' AND destination == 'SFO' AND date LIKE '01010%' AND delay > 0""").show()
    spark.sql("""SELECT * FROM bar WHERE origin = 'SEA' AND destination = 'SFO' AND date LIKE '01010%' and delay > 0""").show()

    import org.apache.spark.sql.functions._
    // Joins
    // foo.join(
    //  airports.as('air), col("air.IATA") === col("origin")
    //).select("City", "State", "date", "delay", "distance", "destination").show()
    // Does not work // No reconoce la columna air.IATA y no he conseguido hacer que reconozca ninguna columna de la tabla airports renamed as air

    // Windowing
    spark.sql("DROP TABLE IF EXISTS departureDelaysWindow")
    spark.sql("CREATE TABLE departureDelaysWindow AS SELECT origin, destination, SUM(delay) as TotalDelays FROM departureDelays WHERE origin IN ('SEA', 'SFP', 'JFK) AND destination IN ('SEA', 'SFO', 'JFK', 'DEN', 'ORD', 'LAX', 'ALT') GROUP BY origin, destination")

    spark.sql("SELECT * FROM departureDelaysWindow")

  }

}
