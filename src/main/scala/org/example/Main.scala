package org.example

import org.example.exercices.Ej1_CreatingTablesOnTextFormat
import org.example.theoryExamples.chapter2.Ej1_MnMs
import org.example.theoryExamples.chapter3.{Ej1_DefiningSchema, Ej2_FireCalls_CommonDataFrameOperations, Ej3_DataSet_API}
import org.example.theoryExamples.chapter4.{Ej1_BasicSQLQueryExamples, Ej2_CreatingSQLDatabasesAndTables, Ej3_DataFrameReader, Ej4_Images}
import org.example.theoryExamples.chapter5.{Ej1_UserDefinedFunctions, Ej2_PostgreSQL, Ej3_MySQL, Ej5_MySQLServer, Ej6_CommonDFandSparkSQLOperations}
import org.example.theoryExamples.chapter6.{Ej1_ScalaCaseClase, Ej2_CreatingSampleData}
import org.example.theoryExamples.chapter7.Ej1_SparkConfigModifications

object Main {
  def main(args:Array[String]): Unit = {

    //if (args.length != 1) {
    //  println("Usage: Main <mnm_dataset.csv>")
    //  System.exit(-1)
    //}

    /*
      // Chapter2 Ej1_MnMs execution code
      val mnmDataSet_File = "src/main/resources/in/mnm_dataset.csv"
      Ej1_MnMs.run(mnmDataSet_File)
     */

    /*
      // Chapter3 Ej1_DefiningSchema execution code
      val blogs_File = "src/main/resources/in/blogs.json"
      DefiningSchema.run(blogs_File)
     */

    /*
      // Chapter3 Ej2_FireCallsCommonDataFrameOperations execution code
      val sfFireCalls_File = "src/main/resources/in/sf-fire-calls.csv"
      Ej2_FireCalls_CommonDataFrameOperations.run(sfFireCalls_File)
     */

    /*
      // Chapter3 Ej3_DataSet_API execution code
      val scalaCaseClasses_File = "src/main/resources/in/iot_devices.json"
      Ej3_DataSet_API.run(scalaCaseClasses_File)
     */

    /*
      // Chapter4 Basic SQL Exmaples execution code
      val basicSQLQueryExamples = "src/main/resources/in/departuredelays.csv"
      Ej1_BasicSQLQueryExamples.run(basicSQLQueryExamples)
     */

    /*
      // Chapter4 Creating SQL Databases and Tables execution code
      val creatingSQLDatabasesAndTables = "src/main/resources/in/departuredelays.csv"
      Ej2_CreatingSQLDatabasesAndTables.run(creatingSQLDatabasesAndTables)
     */

    /*
      // Chapter4 DataFrame Readers execution code
      val dataFrameReader = "src/main/resources/in/sumaryData/"
      Ej3_DataFrameReader.run(dataFrameReader)
     */

    /*
      // Chapter4 Images execution code
      val imagesFile = "src/main/resources/in/images/"
      Ej4_Images.run(imagesFile)
     */

    /*
      // Chapter5 User-Defined Functions execution code
      Ej1_UserDefinedFunctions.run()
      // Chapter5 PostgreSQL execution code
      Ej2_PostgreSQL.run()
      // Chapter5 MySQL execution conde
      Ej3_MySQL.run()
      // Chapter5 Azure Cosmos DB execution code
      Ej4_AzureCosmosDB.run()
      // Chapter5 My SQL Server execution code
      Ej5_MySQLServer.run()
     */

    /*
      // Chapter5 Common DataFrame and SparkSQL Operations execution code
      val departureDelaysCSVfile = "src/main/resources/in/departuredelays.csv"
      val airportCodesNA = "src/main/resources/in/airport-codes-na.txt"
      Ej6_CommonDFandSparkSQLOperations.run(departureDelaysCSVfile, airportCodesNA)
     */

    /*
      // Chapter6 Scala Case Classes for DataSets
      val bloggers = "src/main/resources/in/blogs.json"
      Ej1_ScalaCaseClase.run(bloggers)
     */

    /*
      // Chapter6 Creating Sample Data execution code
      Ej2_CreatingSampleData.run()
     */

    /*
      // Chapter7 Spark Configuration Modification execution code
      Ej1_SparkConfigModifications.run("SparkConfig")
     */

    val exercicesEj1_path = "src/main/resources/in/estadisticas202402.csv"
    Ej1_CreatingTablesOnTextFormat.run(exercicesEj1_path)

  }
}
