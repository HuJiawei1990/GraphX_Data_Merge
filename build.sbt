name := "GraphX_Data_Merge"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.1.0"
libraryDependencies += "org.apache.spark" % "spark-graphx_2.11" % "2.1.0"
libraryDependencies += "org.apache.spark" % "spark-mllib_2.11" % "2.1.0"
//libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.1.0"
libraryDependencies += "org.apache.spark" % "spark-hive_2.11" % "2.1.0"

libraryDependencies += "com.alibaba" % "fastjson" % "1.2.24"

// for csv files
//libraryDependencies += "com.databricks" % "spark-csv_2.10" % "1.3.0"