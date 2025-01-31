name := "assert-equals"
organization:= "com.beamsuntory.assertequals"
version := "1.0.0"
scalaVersion := "2.13.16"

val sparkVersion = "3.5.1"


lazy val root = (project in file("."))
  .settings(
    name := "AssertEquals",
    idePackagePrefix := Some("com.beamsuntory.assertequals")
  )

libraryDependencies ++= Seq(
  ("org.apache.spark" %% "spark-sql" % "3.5.1" % "provided").cross(CrossVersion.for3Use2_13),

  "com.google.cloud.spark" %% "spark-bigquery" % "0.41.1",

  "org.scalatest" %% "scalatest" % "3.2.19" % Test


)

