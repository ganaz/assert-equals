package assertequals.dataframeops

import org.apache.spark.sql.{DataFrame, SparkSession}


class DataFrameReader (val expectedTable:String, val actualTable:String){
  override def toString: String = s"Reading data from tables: $expectedTable  and $actualTable"

}

object DataFrameReader {
 def apply(expectedTable:String, actualTable:String): DataFrameReader = {
  new DataFrameReader(expectedTable,actualTable)

 }

  def readFromBQ(spark:SparkSession, expectedTable:String, actualTable:String):(DataFrame,DataFrame) = {

    val expectedTableDF= spark.read.table(expectedTable)
    val actualTableDF= spark.read.table(actualTable)

    (expectedTableDF, actualTableDF)


  }


}
