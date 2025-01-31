package com.beamsuntory.assertequals

import org.apache.spark.{SparkConf}
import org.apache.spark.sql.{SparkSession, SQLImplicits}
import org.scalatest.{BeforeAndAfterAll}
import org.scalatest.funsuite.AnyFunSuite
import org.apache.spark.sql.Dataset

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

class test extends SampleTest{

  test("filter even numbers from a dataset") {
    import sqlImplicits._

    val data = Seq(1, 2, 3, 4, 5).toDS()
    val result: Dataset[Int] = data.filter(_ % 2 == 0)

    assert(result.collect().sorted === Array(2, 4))
  }
}

