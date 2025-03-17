package com.beamsuntory.assertequals


import org.apache.spark.SparkConf
import org.apache.spark.sql.{SQLImplicits, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.Dataset
import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import javax.jdo.annotations.Columns


trait SampleTest extends AnyFunSuite with BeforeAndAfterAll {
  self =>
  var ss: SparkSession = null

  protected def sparkSession: SparkSession = ss

  protected lazy val sqlImplicits: SQLImplicits = self.sparkSession.implicits

  override def beforeAll(): Unit = {
    super.beforeAll()
    val sparkConfig = new SparkConf()
    sparkConfig.set("spark.master", "local")

    ss = SparkSession.builder().config(sparkConfig).getOrCreate()


  }

  override def afterAll(): Unit = {
    if (ss != null) {
      ss.stop()
    }
    super.afterAll()

  }

}

class test extends SampleTest with DataFrameSuiteBase {

  test("filter even numbers from a dataset") {
    import sqlImplicits._

    val data = Seq(1, 2, 3, 4, 5).toDS()
    val result: Dataset[Int] = data.filter(_ % 2 == 0)

    assert(result.collect().sorted === Array(2, 4))
  }

  test("compare two DataFrames") {
    import sqlImplicits._ // Provided by DataFrameSuiteBase

    // Create your expected DataFrame
    val expectedData = Seq((1, "a"), (2, "b"), (3, "c"))
    val expectedDF = ss.createDataFrame(expectedData).toDF("id", "value")

    // Create the actual DataFrame (e.g., from a transformation)
    val actualData = Seq((1, "a"),(3, "c")) // Example: data might come from a function
    val actualDF = ss.createDataFrame(actualData).toDF("id", "value")
    val result = actualDF.except(expectedDF)
    println("Result -----> "+ result.show())



     //Use assertDataFrameEqual (from spark-testing-base)
//    assertDataFrameEquals(expectedDF, actualDF)
//    assertDataFrameApproximateEquals()
//    assertDataFrameDataEquals()
//     assertSchemasEqual()
//    assertDataFrameNoOrderEquals()
  }

  test("Compare two with approx values"){

    def compareLargeDataFramesWithTolerance(df1: DataFrame, df2: DataFrame, tolerance: Double, numericCols: Seq[Column], bucketCols:Seq[Column]): DataFrame = {

      // 1. Bucketing: Create buckets for efficient joining
      val df1Bucketed = df1.withColumn("bucket_id1", hash(bucketCols: _*))
      val df2Bucketed = df2.withColumn("bucket_id2", hash(bucketCols: _*))

      // 2. Joining on Buckets: Join only within the same bucket
      val joinedDF = df1Bucketed.as("df1").join(df2Bucketed.as("df2"), col("bucket_id1") === col("bucket_id2"), "full_outer")
        .drop("bucket_id1","bucket_id2") // Drop the bucket ID after joining

      // 3. Filtering and Comparison with Tolerance (after join)
      val comparisonExprs: Seq[Column] = numericCols.map { colName =>
        abs(joinedDF(s"df1.$colName") - joinedDF(s"df2.$colName")) <= tolerance as s"${colName}_equal"
      }

      val allCols1 = df1.columns.map(c=> col(s"df1.$c"))
      val allCols2 = df2.columns.map(c=> col(s"df2.$c"))

      val combinmedCols = allCols1 ++ allCols2 ++ comparisonExprs

      joinedDF.select(combinmedCols: _*)


    }

    val df1 = ss.range(1000).withColumn("value", rand() * 100).withColumn("category", when(col("id") % 2 === 0, "A").otherwise("B"))
    val df2 = ss.range(1000).withColumn("value", rand() * 100).withColumn("category", when(col("id") % 2 === 0, "A").otherwise("B"))

    df1.show(100)
    df2.show(100)
    val tolerance = 0.01
    val numericCols = Seq(col("value"))
    val bucketCols = Seq(col("category")) // Choose appropriate bucketing columns

    val comparisonDF = compareLargeDataFramesWithTolerance(df1, df2, tolerance, numericCols, bucketCols)

    comparisonDF.cache() // Cache the result if you'll be using it multiple times
    comparisonDF.count()  // Trigger the computation and evaluate the performance

    //Filter for approximately equal rows:
    //val approximatelyEqualRows = comparisonDF.filter(numericCols.map(colName => col(s"${colName}_equal")).reduce(_ && _))
    //val approximatelyEqualRows = comparisonDF.filter(!numericCols.map(colName => col(s"${colName}_equal")).reduce(_ && _))
    val approximatelyEqualRows = comparisonDF.filter(col(s"value_equal").equalTo(false))
    approximatelyEqualRows.show()


  }
}

