
/*

package org.example.chapter5

import org.example.commonlyUsedSharedFolder.sparkSessionFactory.SparkSessionFactory
import com.microsoft.azure.cosmosdb.spark.schema._
import com.microsoft.azure.cosmosdb.spark._
import com.microsoft.azure.cosmosdb.spark.config.Congif

object Ej4_AzureCosmosDB {

  def run(): Unit = {

    val spark = SparkSessionFactory.createSparkSession("Azure Cosmos DB")

    // Loading data from Azure Cosmos DB
    // Configure connection to your collection
    val query = "SELECT c.colA, c.coln FROM c WHERE c.origin = 'SEA'"
    val readConfig = Config(Map(
      "Endpoing" -> "https://[ACCOUNT].documents.azure.com:443/",
      "Masterkey" -> "[MASTER KEY]",
      "Database" -> "[DATABASE]",
      "PreferredRegions" -> "Central US; East US2",
      "Collection" -> "[COLLECTION]",
      "SamplingRatio" -> "1.0",
      "query_custom" -> query
    ))

    // Connect via azure-cosmosdb-spark to create Spark DataFrame
    val df =  spark.read.cosmosDM(readConfig)
    df.count()

    // Saving data to Azure Cosmos DB
    // Configure connection
    val writeConfig =  Config(Map(
      "Endpoing" -> "https://[ACCOUNT].documents.azure.com:443/",
      "Masterkey" -> "[MASTER KEY]",
      "Database" -> "[DATABASE]",
      "PreferredRegions" -> "Central US; East US2",
      "Collection" -> "[COLLECTION]",
      "WritingBatchSize" -> "100"
    ))

    // Upsert the DataFrame to Azure Cosmos DB
    import org.apache.spark.sql.SaveMode
    df.write.mode(SaveMode.Overwrite).cosmosDB(writeConfig)
  }

}

 */