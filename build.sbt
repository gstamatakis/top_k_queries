name := "top_k_queries"
version := "1.0"
scalaVersion := "2.12.11"

val versions = new {
  val spark = "2.4.3"
}

val scopes = new {
  val sparkCore = "provided"
  val sparkSQL = "provided"
}

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % versions.spark % scopes.sparkCore,
  "org.apache.spark" %% "spark-sql" % versions.spark % scopes.sparkSQL
)

//don't run tests in parallel - breaks spark test harness
parallelExecution in Test := false

//don't run sbt test during assembly
test in assembly := {}

// set the main class for 'sbt run'
mainClass in(Compile, run) := Some("topk.TopKDriver")
mainClass in(Compile, packageBin) := Some("topk.TopKDriver")

////which jar to use for sparkSubmit: the assembled fat jar
//sparkSubmitJar := assembly.value.absolutePath
//
////variables set during sparkSubmit
//sparkSubmitSparkArgs := Seq(
//  "--master", sys.env.getOrElse("SPARK_MASTER_URL", "spark://spark-master:7077")
//)

assemblyMergeStrategy in assembly := {
  case PathList("org", "aopalliance", xs@_*) => MergeStrategy.last
  case PathList("javax", "inject", xs@_*) => MergeStrategy.last
  case PathList("javax", "servlet", xs@_*) => MergeStrategy.last
  case PathList("javax", "activation", xs@_*) => MergeStrategy.last
  case PathList("org", "apache", xs@_*) => MergeStrategy.last
  case PathList("com", "google", xs@_*) => MergeStrategy.last
  case PathList("com", "esotericsoftware", xs@_*) => MergeStrategy.last
  case PathList("com", "codahale", xs@_*) => MergeStrategy.last
  case PathList("com", "yammer", xs@_*) => MergeStrategy.last
  case "about.html" => MergeStrategy.rename
  case "META-INF/ECLIPSEF.RSA" => MergeStrategy.last
  case "META-INF/mailcap" => MergeStrategy.last
  case "META-INF/mimetypes.default" => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
