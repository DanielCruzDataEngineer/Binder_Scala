name := "MyProject"

version := "0.1"

scalaVersion := "2.13.8"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.0"
libraryDependencies += "mysql" % "mysql-connector-java" % "8.0.23"
libraryDependencies += "org.apache.iceberg" % "iceberg-spark3" % "0.12.0"
