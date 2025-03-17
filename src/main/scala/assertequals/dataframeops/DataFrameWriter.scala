package assertequals.dataframeops

import org.apache.spark.sql.DataFrame

object DataFrameWriter {

  def writeToGCS(df:DataFrame,path:String): Unit = {

    df.write.format("csv")
      .option("delimiter", ",")
      .option("header", true)
      .mode("overwrite")
      .save(path)
    println(s"CSV DataFrame written to: $path")
  }

}
