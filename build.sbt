name := "Permutive Analytics Exercise"

version := "0.1"

scalaVersion := "2.12.6"

resolvers ++= Seq(
  "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/"
)

libraryDependencies ++= {
  val akkaVersion  = "2.5.9"
  Seq(
    /** Akka **/
    "com.typesafe.akka"      %% "akka-actor"  % akkaVersion,
    "com.typesafe.akka"      %% "akka-stream" % akkaVersion,
    "com.typesafe.akka"      %% "akka-stream-testkit" % akkaVersion % Test,
    /** JSON **/
    "io.spray"               %% "spray-json"  % "1.3.3",

    /** Command line parsing. **/
    "com.github.scopt" %% "scopt" % "3.7.0",

    /** Testing **/
    "org.scalatest" %% "scalatest" % "3.0.5" % "test",

    /** Logging **/
    "ch.qos.logback" % "logback-classic" % "1.2.3",
    "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0"
  )
}
