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

    // Windowing // Done on VIEWS becase theres no Hive tu support TABLE creation
    spark.sql("DROP VIEW IF EXISTS departureDelaysWindow")
    spark.sql("CREATE TEMPORARY VIEW departureDelaysWindow AS SELECT origin, destination, SUM(delay) as TotalDelays FROM departureDelays WHERE origin IN ('SEA', 'SFO', 'JFK') AND destination IN ('SEA', 'SFO', 'JFK', 'DEN', 'ORD', 'LAX', 'ALT') GROUP BY origin, destination")

    spark.sql("SELECT * FROM departureDelaysWindow").show()

    spark.sql("SELECT origin, destination, SUM(TotalDelays) FROM departureDelaysWindow WHERE origin = 'JFK' GROUP BY origin, destination ORDER BY SUM(TotalDelays) DESC LIMIT 3").show()

    spark.sql("SELECT origin, destination, TotalDelays, rank FROM (SELECT origin, destination, TotalDelays, dense_rank() OVER (PARTITION BY origin ORDER BY TotalDelays DESC) AS rank FROM departureDelaysWindow) t WHERE rank <= 3").show()

    foo.show()

    // Adding columns
    val foo2 = foo.withColumn(
      "status", expr("CASE WHEN delay <= 10 THEN 'On-Time' ELSE 'Delayed' END")
    )
    foo2.show()

    // Dropping columns
    val foo3 = foo.drop("delay")
    foo3.show()

    // Renaming columns
    val foo4 = foo3.withColumnRenamed("status", "flgiht_status")
    foo4.show()

    // Pivoting
    spark.sql("SELECT destination, CAST(SUBSTRING(date, 0, 2) AS int) AS month, delay FROM departureDelays WHERE origin = 'SEA'").show()
    // Pivoting allows you to place names in the month column instead of numbers as well as perform aggregate calculations on the delays by destination and month
    spark.sql("SELECT * FROM (SELECT destination, CAST(SUBSTRING(date, 0, 2) AS int) AS month, delay FROM departureDelays WHERE origin = 'SEA') " +
      "PIVOT (CAST(AVG(delay) AS DECIMAL(4, 2)) AS AvgDelay, MAX(delay) AS MaxDelay FOR month IN (1 JAN, 2 FEB)) ORDER BY destination").show()


  }

}
