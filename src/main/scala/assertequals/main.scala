package assertequals

import assertequals.dataframeops.{AssertsDataFrame, DataFrameReader}
import org.apache.spark.sql.SparkSession
import org.kohsuke.args4j.{CmdLineException, CmdLineParser, Option}

import scala.collection.JavaConverters
import scala.collection.JavaConverters._
import scala.sys.exit


object Args {
  @Option(name = "-expected_table", required = false, usage = "Legacy table name with dataset and project")
  var expectedTable: String = ""

  @Option(name = "-actual_table", required = false, usage = "VPC table name with dataset and project")
  var actualTable: String = ""

  @Option(name = "-process_name", required = false, usage = "Chain or dag name, it will be used to read config from application.conf to get the affected tables")
  var processName: String = ""

  @Option(name = "-output_path", required = false, usage = "Path to store unmatched data")
  var outputPath: String = ""

  @Option(name = "-threshold_value", required = false, usage = "Threshold value for approximation check")
  var threshold: Double = 0.01


}


object AssertEquals {


  /**
   * Main application entry point for data validation between BigQuery tables.
   *
   * This application compares data between an expected table and an actual table,
   * performing basic schema checks, exact data equality checks, and approximate
   * data equality checks based on a specified threshold.
   *
   * The application can operate in two modes:
   * 1. Process a list of tables specified in the configuration file.
   * 2. Process a single pair of expected and actual tables provided as command-line arguments.
   *
   * Usage:
   * --processName <process_name> [--outputPath <output_path>] [--threshold <threshold>]
   * Processes a list of tables defined under the specified `process_name` in the configuration file.
   * --outputPath specifies where to save the results.
   * --threshold specifies the tolerance for approximate equality checks.
   *
   * --expectedTable <expected_table> --actualTable <actual_table> [--outputPath <output_path>] [--threshold <threshold>]
   * Processes a single pair of expected and actual tables.
   * --outputPath specifies where to save the results.
   * --threshold specifies the tolerance for approximate equality checks.
   *
   * Arguments:
   * args: An array of command-line arguments.
   *
   * Configuration:
   * The application relies on a configuration file (loaded via `ConfigReader`) that
   * specifies the source and target BigQuery projects, as well as the list of tables
   * to process when using the `--processName` option.
   *
   * Processing Steps:
   * 1. Parses command-line arguments using `CmdLineParser`.
   * 2. Initializes a SparkSession using `getSparkSession`.
   * 3. Reads source and target BigQuery project names from the configuration.
   * 4. Reads table names from the configuration (if `--processName` is provided) or
   * from command-line arguments (`--expectedTable` and `--actualTable`).
   * 5. Reads DataFrames from BigQuery for each table pair using `DataFrameReader.readFromBQ`.
   * 6. Performs the following assertions using `AssertsDataFrame`:
   * - `assertEqualsBasicCheck`: Checks if the schemas of the DataFrames are compatible.
   * - `assertDataFrameDataEquals`: Checks if the DataFrames have exactly the same data.
   * - `assertDataFrameDataApproxEquals`: Checks if the DataFrames have approximately the same data,
   * allowing for a specified threshold of differences.
   * 7. Prints the results of the assertions to the console and potentially to a specified output path.
   *
   * Error Handling:
   * If command-line argument parsing fails, the application prints an error message
   * and usage instructions, then exits with a non-zero exit code.
   * Exceptions during BigQuery data reading or assertion checks are not explicitly
   * handled in this main method.
   *
   * Dependencies:
   * - SparkSession (`getSparkSession`)
   * - CmdLineParser (for argument parsing)
   * - ConfigReader (for configuration loading)
   * - DataFrameReader (for reading DataFrames from BigQuery)
   * - AssertsDataFrame (for DataFrame assertions)
   * - JavaConverters (for Java-Scala collection conversions)
   *
   * Example Usage:
   * # Process tables from the 'my_process' configuration:
   * ./myapp.sh --processName my_process --outputPath /path/to/results --threshold 0.01
   *
   * # Process a single table pair:
   * ./myapp.sh --expectedTable target_dataset.expected_table --actualTable source_dataset.actual_table --outputPath /path/to/results
   */

  def main(args: Array[String]): Unit = {

    val parser = new CmdLineParser(Args)
    try {
      parser.parseArgument(JavaConverters.asJavaCollection(args))
    } catch {
      case e: CmdLineException =>
        print(s"Error:${e.getMessage}\n Usage:\n")
        parser.printUsage(System.out)
        exit(1)
    }
    println(Args.expectedTable)
    println(Args.actualTable)

    val spark: SparkSession = getSparkSession

    val sourceProject = ConfigReader.applicationConf.getConfig("app").getString("source_project")
    val targetProject = ConfigReader.applicationConf.getConfig("app").getString("target_project")



    println(ConfigReader.applicationConf.getConfig("app").getString("source_project"))
    println(ConfigReader.applicationConf.getConfig("app").getString("target_project"))
    println(ConfigReader.applicationConf.getConfig("process").getConfig("process_name").getList("tables").unwrapped())

    if (Args.processName != null) {

      val tableList = ConfigReader.applicationConf.getConfig("process").getConfig("process_name")
        .getList("tables").unwrapped().asInstanceOf[java.util.ArrayList[String]]
        .asScala
        .toList


      for (table <- tableList) {
        println("Processing table " + table)
        println("Source table :"+ sourceProject+"."+table)
        println("Target table :"+ targetProject+"."+table)
        val (expectedDF, actualDF) = DataFrameReader
          .readFromBQ(spark, targetProject+"."+table+"_vpc", sourceProject+"."+table)


        AssertsDataFrame.assertEqualsBasicCheck(expectedDF, actualDF)
        AssertsDataFrame.assertDataFrameDataEquals(expectedDF,actualDF, Args.outputPath)
        AssertsDataFrame.assertDataFrameDataApproxEquals(expectedDF,actualDF, Args.threshold, Args.outputPath)

      }


    }else if (Args.expectedTable !=null && Args.actualTable !=null){

      val (expectedDF, actualDF) = DataFrameReader
        .readFromBQ(spark, targetProject+"."+Args.expectedTable , sourceProject+"."+Args.actualTable)


      AssertsDataFrame.assertEqualsBasicCheck(expectedDF, actualDF)
      AssertsDataFrame.assertDataFrameDataEquals(expectedDF,actualDF, Args.outputPath)
      AssertsDataFrame.assertDataFrameDataApproxEquals(expectedDF,actualDF, Args.threshold, Args.outputPath)
    }
  }

  private def getSparkSession = {
    val spark = SparkSession
      .builder()
      .master("local[4]")
      .appName("Spark Dataframe comparison")
      .getOrCreate()
    spark
  }
}

