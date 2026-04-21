ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.3.7"

lazy val root = (project in file("."))
  .settings(
    name := "order-discount-system"
  )

libraryDependencies ++= Seq(
  "org.scala-lang.modules" %% "scala-parallel-collections" % "1.0.4",
  "com.oracle.database.jdbc" % "ojdbc8" % "23.3.0.23.09",
  "com.oracle.database.jdbc" % "ucp" % "23.3.0.23.09",  // Connection pool
  "com.typesafe" % "config" % "1.4.3"
)

// JVM options for large data processing
fork := true
javaOptions ++= Seq(
  "-Xmx8g",  // Increase heap size to 8GB
  "-Xms4g",  // Initial heap size
  "-XX:+UseG1GC",  // Use G1 garbage collector
  "-XX:MaxGCPauseMillis=100",
  "-XX:+ParallelRefProcEnabled"
)