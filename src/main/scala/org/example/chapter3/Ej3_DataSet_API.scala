package org.example.chapter3

import org.apache.spark.sql.Row
import org.example.commonlyUsedSharedFolder.sparkSessionFactory.SparkSessionFactory
import org.example.commonlyUsedSharedFolder.sparkSessionFactory.caseClasses.{DeviceIoTData, DeviceTempByCountry}

object Ej3_DataSet_API {

  def run (fileInjected: String): Unit = {

    val spark = SparkSessionFactory.createSparkSession("DataSet API")

    import spark.implicits._

    // Creating a Row object
    val row = Row(350, true, "Learning Spark 2E", null)

    // Using an index into the row object you can acces individual fields with its public getter methods
    row.getInt(0)
    row.getBoolean(1)
    row.getString(2)

    // Doesn't show anything not sure why

    // Once defined, we can use it to read out the file and convert the raturned Dataset[Row] into Dataset[DeviceIoTData]
    val ds = spark.read.json(fileInjected).as[DeviceIoTData]

    ds.show(5, false)

    // DataSet operations
    val filterTempDS = ds
      .filter({d => {d.temp > 30 && d.humidity > 70}})
    filterTempDS.show(5, false)

    val dsTemp = ds
      .filter(d => {d.temp > 25})
      .map(d => (d.temp, d.device_name, d.device_id, d.cca3))
      .toDF("temp", "device_name", "device_id", "cca3")
      .as[DeviceTempByCountry]
    dsTemp.show(5, false)
    // Inspect only the first row of the dataset
    val device = dsTemp.first()
    println(device)

    // Express the same query as above using column names and then cast a Dataset[DeviceTempByCountry]
    val dsTemp2 = ds
      .select($"temp", $"device_name", $"device_id", $"cca3")
      .where("temp > 25")
      .as[DeviceTempByCountry]


  /*
          import org.apache.spark.sql.Encoders
        // Creating the specific Encoder for DeviceIoTData
        val deviceIoTDataEncoder = Encoders.product[DeviceIoTData]

        val df = spark.read.json(fileInjected)

        df.show(10, false)


        val dataSet = spark.read.json(fileInjected).as(deviceIoTDataEncoder)

        dataSet.show(5, false)


       // DataSet Operations
       val filterTempDataSet = dataSet.filter({d => {d.temp > 30 && d.humidity > 70})

       filterTempDataSet.show(5, false)

     */


  }
}
