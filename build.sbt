ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.3.7"

lazy val root = (project in file("."))
  .settings(
    name := "order-discount-system"
  )

libraryDependencies += "com.typesafe" % "config" % "1.4.3"
libraryDependencies += "com.oracle.database.jdbc" % "ojdbc8" % "23.3.0.23.09"
