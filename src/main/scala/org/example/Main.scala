package org.example


import org.example.ej1_mnms.Ej1_MnMs

object Main {
  def main(args:Array[String]): Unit = {

    //if (args.length != 1) {
    //  println("Usage: Main <mnm_dataset.csv>")
    //  System.exit(-1)
    //}

    // val mnmFile = args(0) // Esta línea sirve para pasarle el archivo directamente por consola
    val mnmFileManuallyInjecetd = "src/main/resources/mnm_dataset.csv"
    Ej1_MnMs.run(mnmFileManuallyInjecetd)


  }
}
