package org.example


import org.example.chapter3.{Ej1_DefiningSchema, Ej2_FireCalls_CommonDataFrameOperations, Ej3_DataSet_API}
import org.example.chapter2.Ej1_MnMs
import org.example.chapter4.{Ej1_BasicSQLQueryExamples, Ej2_CreatingSQLDatabasesAndTables, Ej3_DataFrameReader, Ej4_Images}

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

    // Chapter4 Images execution code
    val imagesFile = "src/main/resources/in/images/"
    Ej4_Images.run(imagesFile)

  }
}
