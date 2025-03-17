package assertequals.dataframeops

import org.apache.spark.sql.functions.{abs, col, hash}
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.types.{DataType, NumericType, StructField, StructType}
import org.json4s.jackson.JsonMethods.parse
import org.json4s.jackson.Serialization
import org.apache.log4j.{Level, Logger}

object AssertsDataFrame {
  val log = Logger.getLogger(getClass.getName)

  def assertEqualsBasicCheck(expectedDF: DataFrame, actualDF: DataFrame): Unit = {

    try {


      val expectedCount = expectedDF.count()
      val actualCount = actualDF.count()

      log.info("Expected table count: " + expectedCount)
      log.info("Actual table count: " + actualCount)


      assert(expectedCount == actualCount, " Count matching")

      assertSchemasEqual(expectedDF.schema, actualDF.schema)


    } catch {
      case e: Exception => e.printStackTrace()
    }

  }

  def getNumericColumns(df: DataFrame): Seq[String] = {
    df.schema.fields
      .filter(_.dataType.isInstanceOf[NumericType]) // Filter for NumericType
      .map(_.name)
  }

  def isNumericColumn(dataType: DataType): Boolean = {
    if (dataType.isInstanceOf[NumericType]) {
      true
    } else {
      false
    }

  }

  def compareLargeDataFramesWithTolerance(df1: DataFrame, df2: DataFrame, tolerance: Double, numericCols: Seq[Column], bucketCols: Seq[Column]): DataFrame = {

    // 1. Bucketing: Create buckets for efficient joining
    val df1Bucketed = df1.withColumn("bucket_id1", hash(bucketCols: _*))
    val df2Bucketed = df2.withColumn("bucket_id2", hash(bucketCols: _*))

    // 2. Joining on Buckets: Join only within the same bucket
    val joinedDF = df1Bucketed.as("df1").join(df2Bucketed.as("df2"), col("bucket_id1") === col("bucket_id2"), "full_outer")
      .drop("bucket_id1", "bucket_id2") // Drop the bucket ID after joining

    // 3. Filtering and Comparison with Tolerance (after join)
    val comparisonExprs: Seq[Column] = numericCols.map { colName =>
      abs(joinedDF(s"df1.$colName") - joinedDF(s"df2.$colName")) <= tolerance as s"${colName}_equal"
    }

    val allCols = df1.columns.map(c => col(s"df1.$c"))

    val combinmedCols = allCols ++ comparisonExprs

    joinedDF.select(combinmedCols: _*)

  }

  /**
   *  This method is design to perform the comparison of all the data, matching its numeric and non-numeric rows as it is.
   * @param expectedDF
   * @param actualDF
   * @param path: GCS path
   */
  def assertDataFrameDataEquals(expectedDF: DataFrame, actualDF: DataFrame, path: String): Unit = {

    try {
      expectedDF.cache()
      actualDF.cache()

      // Compare dataframe based on all data.
      val outDF1 = expectedDF.exceptAll(actualDF)
      val outDF2 = actualDF.exceptAll(expectedDF)


      if (outDF1.count() > 0 || outDF2.count() > 0) {
        outDF1.show(10)
        outDF2.show(10)

        log.error(" Mismatch found in data")

        DataFrameWriter.writeToGCS(outDF1,path+"/expected")
        DataFrameWriter.writeToGCS(outDF2,path+"/actual")
      }

    } catch {
      case e: Exception => {
        e.printStackTrace()

      }
    } finally {
      expectedDF.unpersist()
      actualDF.unpersist()
    }


  }

  /**
   * This method is designed to perform the comparison of two dataframes which approximation of numerical values
   * @param expectedDF
   * @param actualDF
   * @param tolerance : this is used for allowing the tolerance for the difference between numerical values.
   * @param path : GCS path
   */
  def assertDataFrameDataApproxEquals(expectedDF: DataFrame, actualDF: DataFrame, tolerance: Double, path: String): Unit = {

    try {
      expectedDF.cache()
      actualDF.cache()

      assertSchemasEqual(expectedDF.schema, actualDF.schema)

      val nonNumericalColumns = expectedDF.schema.fields.filter(field => !isNumericColumn(field.dataType)).map(x => col(x.name))
      val numericalColumns = expectedDF.schema.fields.filter(field => isNumericColumn(field.dataType)).map(x => col(x.name))

      val comparisonDF = compareLargeDataFramesWithTolerance(expectedDF, actualDF, tolerance, numericalColumns, nonNumericalColumns)

      val approximatelyEqualRows = comparisonDF.filter(!numericalColumns.map(colName => col(s"${colName}_equal")).reduce(_ && _))

      approximatelyEqualRows.cache() // Cache the result if you'll be using it multiple times
      val cnt = approximatelyEqualRows.count() // Trigger the computation and evaluate the performance

      if (cnt >= 1) {
        log.error("Mismatch found in data")
        approximatelyEqualRows.show(10)
        DataFrameWriter.writeToGCS(approximatelyEqualRows, path)
      } else {
        log.info("Both expected and actual dataframes are equal")
      }

    } catch {
      case e: Exception => {
        e.printStackTrace()

      }
    } finally {
      expectedDF.unpersist()
      actualDF.unpersist()
    }


  }


  /**
   * This method is used for comparing the schema of two dataframes
   * @param expected
   * @param result
   */
  def assertSchemasEqual(expected: StructType, result: StructType): Unit = {
    import scala.collection.JavaConverters._
    def dropInternal(s: StructField) = {
      // No metadata no need to filter
      val metadata = s.metadata
      if (!metadata.contains("__autoGeneratedAlias")) {
        s
      } else {
        val jsonString = metadata.json
        val metadataMap = (parse(jsonString).values.asInstanceOf[Map[String, Any]] - "__autoGeneratedAlias")
        implicit val formats = org.json4s.DefaultFormats
        val cleanedJson = Serialization.write(metadataMap)
        val cleanedMetadata = org.apache.spark.sql.types.Metadata.fromJson(cleanedJson)
        new StructField(s.name, s.dataType, s.nullable, cleanedMetadata)
      }
    }

    val cleanExpected = expected.iterator.map(dropInternal).toList
    val cleanResult = result.iterator.map(dropInternal).toList

    log.info("Expected clean schema : " + cleanExpected)
    log.info("Actual clean schema : " + cleanResult)
    assert(cleanExpected == cleanResult, "Comparing schema")
  }

}
