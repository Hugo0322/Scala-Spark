package org.example.theoryExamples.chapter4

import org.example.fuf.sparkSessionFactory.SparkSessionFactory

object Ej1_BasicSQLQueryExamples {

  def run(fileInyected: String): Unit = {

    val spark = SparkSessionFactory.createSparkSession("Basic SQL Query Examples")

    // Sin un esquema definido no reconoce el nombre de las columnas
    val schema = "date STRING, delay INT, distance INT, origin STRING, destination STRING"
    /*
       val schema = StructType(Seq(
        StructField("date", StringType, nullable = true),
        StructField("delay", IntegerType, nullable = true),
        StructField("distance", IntegerType, nullable = true),
        StructField("origin", StringType, nullable = true),
        StructField("destination", StringType, nullable = true)
    ))
     */

    // Read and create a temporary view
    // Infer schema (note that for larger files toy may want to specify the schema)
    val dataFrame = spark.read.schema(schema).csv(fileInyected)

    // Create temporary view
    dataFrame.createOrReplaceTempView("US_delay_flights_tbl")

    // Find all flights whose distance is greater than 1k miles
    spark.sql("""SELECT distance, origin, destination FROM US_delay_flights_tbl WHERE distance > 1000 ORDER BY distance DESC""").show(10)
    // Estamos filtrando de manera descendente, lo que significa que aparecerán los 10 vuelos más largos

    // Find flights between SanFrancisco(SFO) and Chicago(ORD) with at least a 2 hour delay
    spark.sql(
      """SELECT date_format(to_timestamp(date, 'MMddHHmm'), 'MM-dd HH:mm') AS flight_date, date, delay, distance, origin, destination
        | FROM US_delay_flights_tbl WHERE delay > 120 AND origin = 'SFO' AND destination = 'ORD' ORDER BY delay DESC""".stripMargin).show(10)
    // Mostrará los que más retraso tuvieron. Parece que todos los retrasos son entre enero y marzo

    // lable al US flights with and indication of this delays
    spark.sql(
      """SELECT delay, origin, destination,
          CASE
              WHEN delay > 360 THEN 'Very Long Delay'
              WHEN delay > 120 AND delay < 360 THEN 'Long Delay'
              WHEN delay > 60 AND delay < 120 THEN 'Short Delay'
              WHEN delay > 0 AND delay < 60 THEN 'Tolerable Delay'
              WHEN delay = 0 THEN 'No Delay at All'
              ELSE 'Early Flight'
          END AS Flight_Delays
          FROM US_delay_flights_tbl
          ORDER BY origin, delay DESC""".stripMargin).show(10)


  }

}
