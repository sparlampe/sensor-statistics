ThisBuild / scalaVersion     := "2.13.1"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "io.pusteblume"
ThisBuild / organizationName := "sensor-statistics"
val AkkaVersion = "2.6.16"
lazy val root = (project in file("."))
  .settings(
    name := "Sensor Statistics",
    libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-stream" % "2.6.16",
      "com.typesafe.akka" %% "akka-stream-testkit" % "2.6.16" % Test,
      "com.monovore" %% "decline-effect" % "2.1.0",
      "com.monovore" %% "decline" % "2.1.0",
      "org.scalatest" %% "scalatest" % "3.2.9" % Test,
      "com.lightbend.akka" %% "akka-stream-alpakka-file" % "3.0.3"
    )
  )
