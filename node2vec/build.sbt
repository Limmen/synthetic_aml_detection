import Dependencies._

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "limmen.gitub.com",
      scalaVersion := "2.11.8",
      version      := "0.1.0-SNAPSHOT"
    )),
    name := "node2vec",
    libraryDependencies ++= Seq(
      scalaTest,
      mockito,
      sparkCore,
      sparkSql,
      sparkMlLib,
      sparkStreaming,
      scalaCsv,
      scallop,
      commonsMath,
      log4jApi,
      log4jCore
    )
  )

assemblyMergeStrategy in assembly := {
 case PathList("META-INF", xs @ _*) => MergeStrategy.discard
 case x => MergeStrategy.first
}
