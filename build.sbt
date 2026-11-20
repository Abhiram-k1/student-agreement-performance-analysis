ThisBuild / scalaVersion := "2.12.18"
ThisBuild / organization := "edu.bigdata"
ThisBuild / version := "1.0.0"

name := "student-rdd-analysis"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.5.3",
  "org.apache.commons" % "commons-csv" % "1.11.0"
)

Compile / run / fork := true
Test / run / fork := true
Compile / run / javaOptions ++= Seq(
  "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
  "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
  "--add-opens=java.base/java.io=ALL-UNNAMED",
  "--add-opens=java.base/java.net=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
  "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED",
  "--add-opens=java.base/sun.security.action=ALL-UNNAMED",
  "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED",
  "-Dspark.ui.showConsoleProgress=false"
)
Test / run / javaOptions ++= (Compile / run / javaOptions).value

Compile / mainClass := Some("edu.bigdata.StudentAnalysisApp")
