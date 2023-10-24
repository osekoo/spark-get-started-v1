
name := sys.env.getOrElse("APP_NAME", "Spark Get Started") // the project's name
version := sys.env.getOrElse("APP_VERSION", "0.1") // the application version
scalaVersion := sys.env.getOrElse("SCALA_FULL_VERSION", "2.12.18") // version of Scala we want to use (this should be in line with the version of Spark framework)
organization := "com.osekoo.dev"

val sparkVersion = sys.env.getOrElse("SPARK_VERSION", "3.5.0")

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
  "net.ruippeixotog" %% "scala-scraper" % "3.1.0"
)
