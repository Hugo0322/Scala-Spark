package org.example


import org.example.chapter3.{Ej1_DefiningSchema, Ej2_FireCalls_CommonDataFrameOperations, Ej3_DataSet_API}
import org.example.chapter2.Ej1_MnMs

object Main {
  def main(args:Array[String]): Unit = {

    //if (args.length != 1) {
    //  println("Usage: Main <mnm_dataset.csv>")
    //  System.exit(-1)
    //}

    /*
      // Chapter2 Ej1_MnMs execution code
      val mnmDataSet_File = "src/main/resources/mnm_dataset.csv"
      Ej1_MnMs.run(mnmDataSet_File)
     */

    /*
      // Chapter3 Ej1_DefiningSchema execution code
      val blogs_File = "src/main/resources/blogs.json"
      DefiningSchema.run(blogs_File)
     */

    /*
      // Chapter3 Ej2_FireCallsCommonDataFrameOperations execution code
      val sfFireCalls_File = "src/main/resources/sf-fire-calls.csv"
      Ej2_FireCalls_CommonDataFrameOperations.run(sfFireCalls_File)
     */

    // Chapter3 Ej3_DataSet_API execution code
    val scalaCaseClasses_File = "src/main/resources/iot_devices.json"
    Ej3_DataSet_API.run(scalaCaseClasses_File)
  }
}
