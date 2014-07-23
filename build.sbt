name         := "spark"

version      := "3.0.0"

scalaVersion := "2.10.4"

organization := "com.scalacourses"

description  := "Spark Demo"

scalacOptions in (Compile, doc) <++= baseDirectory.map {
  (bd: File) => Seq[String](
     "-deprecation",
     "-encoding", "UTF-8",
     "-unchecked",
     "-feature",
     "-target:jvm-1.6",
     "-sourcepath", bd.getAbsolutePath,
     "-Ywarn-adapted-args"
  )
}

resolvers ++= Seq(
  "Sonatype Release" at "https://oss.sonatype.org/content/repositories/releases",
  "MVN Repo" at "http://mvnrepository.com/artifact"
)

libraryDependencies ++= Seq(
  "org.apache.spark"  %% "spark-core"      % Version.spark withSources(),
  "org.apache.spark"  %% "spark-streaming" % Version.spark withSources(),
  "org.apache.spark"  %% "spark-sql"       % Version.spark withSources(),
  "org.apache.spark"  %% "spark-hive"      % Version.spark withSources(),
  "org.apache.spark"  %% "spark-repl"      % Version.spark withSources(),
  "org.apache.hadoop"  % "hadoop-client"   % Version.hadoopClient,
  //
  "org.scalatest"     %% "scalatest"       % Version.scalaTest  % "test",
  "org.scalacheck"    %% "scalacheck"      % Version.scalaCheck % "test"
)

// Must run Spark tests sequentially because they compete for port 4040!
parallelExecution in Test := false

logLevel := Level.Error

// Optional settings from https://github.com/harrah/xsbt/wiki/Quick-Configuration-Examples follow
initialCommands := """
"""

// Only show warnings and errors on the screen for compilations.
// This applies to both test:compile and compile and is Info by default
logLevel in compile := Level.Warn
