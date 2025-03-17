import assertequals.dataframeops.DataFrameWriter
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.log4j.Logger
import org.json4s.jackson.JsonMethods.parse
import org.json4s.jackson.Serialization
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StructField

/**
 * Object `AssertsDataFrame` provides a suite of methods for comparing and asserting
 * the equality of Spark DataFrames. It includes checks for schema compatibility,
 * exact data equality, and approximate data equality with a specified tolerance.
 *
 * This object is designed for use in data validation and testing scenarios,
 * particularly when comparing data from sources like BigQuery.
 */
object AssertsDataFrame {

  val log = Logger.getLogger(getClass.getName)

  /**
   * Performs basic checks on two DataFrames, including row count and schema comparison.
   *
   * @param expectedDF The expected DataFrame.
   * @param actualDF   The actual DataFrame.
   */
  def assertEqualsBasicCheck(expectedDF: DataFrame, actualDF: DataFrame): Unit = {
    try {
      val expectedCount = expectedDF.count()
      val actualCount = actualDF.count()

      log.info("Expected table count: " + expectedCount)
      log.info("Actual table count: " + actualCount)

      assert(expectedCount == actualCount, "Count matching")
      assertSchemasEqual(expectedDF.schema, actualDF.schema)
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  /**
   * Retrieves a sequence of column names from a DataFrame that are of a numeric data type.
   *
   * @param df The DataFrame to inspect.
   * @return A sequence of column names.
   */
  def getNumericColumns(df: DataFrame): Seq[String] = {
    df.schema.fields
      .filter(_.dataType.isInstanceOf[NumericType])
      .map(_.name)
  }

  /**
   * Checks if a given data type is a numeric type.
   *
   * @param dataType The data type to check.
   * @return `true` if the data type is numeric, `false` otherwise.
   */
  def isNumericColumn(dataType: DataType): Boolean = {
    dataType.isInstanceOf[NumericType]
  }

  /**
   * Compares two large DataFrames with a specified tolerance for numeric columns.
   *
   * This method uses bucketing to efficiently join and compare DataFrames,
   * allowing for a tolerance in numeric column comparisons.
   *
   * @param df1         The first DataFrame.
   * @param df2         The second DataFrame.
   * @param tolerance   The tolerance for numeric column comparisons.
   * @param numericCols The sequence of numeric columns to compare.
   * @param bucketCols  The sequence of columns used for bucketing.
   * @return A DataFrame containing the comparison results.
   */
  def compareLargeDataFramesWithTolerance(df1: DataFrame, df2: DataFrame, tolerance: Double, numericCols: Seq[Column], bucketCols: Seq[Column]): DataFrame = {
    val df1Bucketed = df1.withColumn("bucket_id1", hash(bucketCols: _*))
    val df2Bucketed = df2.withColumn("bucket_id2", hash(bucketCols: _*))

    val joinedDF = df1Bucketed.as("df1").join(df2Bucketed.as("df2"), col("bucket_id1") === col("bucket_id2"), "full_outer")
      .drop("bucket_id1", "bucket_id2")

    val comparisonExprs: Seq[Column] = numericCols.map { colName =>
      abs(joinedDF(s"df1.$colName") - joinedDF(s"df2.$colName")) <= tolerance as s"${colName}_equal"
    }

    val allCols = df1.columns.map(c => col(s"df1.$c"))
    val combinmedCols = allCols ++ comparisonExprs

    joinedDF.select(combinmedCols: _*)
  }

  /**
   * Asserts that two DataFrames have exactly the same data.
   *
   * Any differences are logged as errors, and the differing rows are written to GCS.
   *
   * @param expectedDF The expected DataFrame.
   * @param actualDF   The actual DataFrame.
   * @param path       The GCS path to write differing rows.
   */
  def assertDataFrameDataEquals(expectedDF: DataFrame, actualDF: DataFrame, path: String): Unit = {
    try {
      expectedDF.cache()
      actualDF.cache()

      val outDF1 = expectedDF.exceptAll(actualDF)
      val outDF2 = actualDF.exceptAll(expectedDF)

      if (outDF1.count() > 0 || outDF2.count() > 0) {
        outDF1.show(10)
        outDF2.show(10)

        log.error("Mismatch found in data")
        DataFrameWriter.writeToGCS(outDF1, path + "/expected")
        DataFrameWriter.writeToGCS(outDF2, path + "/actual")
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      expectedDF.unpersist()
      actualDF.unpersist()
    }
  }

  /**
   * Asserts that two DataFrames have approximately the same data, allowing for a tolerance
   * in numeric column comparisons.
   *
   * Any differences are logged as errors, and the differing rows are written to GCS.
   *
   * @param expectedDF The expected DataFrame.
   * @param actualDF   The actual DataFrame.
   * @param tolerance  The tolerance for numeric column comparisons.
   * @param path       The GCS path to write differing rows.
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

      approximatelyEqualRows.cache()
      val cnt = approximatelyEqualRows.count()

      if (cnt >= 1) {
        log.error("Mismatch found in data")
        approximatelyEqualRows.show(10)
        DataFrameWriter.writeToGCS(approximatelyEqualRows, path)
      } else {
        log.info("Both expected and actual dataframes are equal")
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      expectedDF.unpersist()
      actualDF.unpersist()
    }
  }

  /**
   * Asserts that two DataFrame schemas are equal, ignoring metadata differences.
   *
   * @param expected The expected schema.
   * @param result   The actual schema.
   */
  def assertSchemasEqual(expected: StructType, result: StructType): Unit = {
    import scala.collection.JavaConverters._
    def dropInternal(s: StructField) = {
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