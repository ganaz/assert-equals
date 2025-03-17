name := "assert-equals"
version := "1.0.0"
scalaVersion := "2.12.20"

val sparkVersion = "3.5.1"



libraryDependencies ++= Seq(
  ("org.apache.spark" %% "spark-sql" % "3.5.1" ).cross(CrossVersion.for3Use2_13),

  "org.apache.spark" %% "spark-core" % "3.5.1" ,

  "com.google.cloud.spark" %% "spark-bigquery" % "0.41.1",

  "org.scalatest" %% "scalatest" % "3.2.19" % "test",

  "com.holdenkarau" %% "spark-testing-base" % "3.5.3_2.0.1" % Test,

  "com.typesafe" % "config" % "1.3.3",

  "args4j" % "args4j" % "2.33",
  "org.apache.logging.log4j" % "log4j-core" % "2.24.2",
  "com.github.luben" % "zstd-jni" % "1.5.6-8"

)
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.15.4"

