organization := "demons"
name := "enkidu"

version := "0.3-SNAPSHOT"

val twitterDeps = Seq(
  "com.twitter" %% "util-core" % "18.5.0",
  "com.twitter" %% "util-jvm" % "18.5.0"
)


libraryDependencies ++= Seq(
  "demons" %% "enki-routing" % "0.3-SNAPSHOT",
  "io.netty" % "netty-all" % "4.1.25.Final",
  "org.scalatest" %% "scalatest" % "3.0.5" % "test",
  "demons" %% "enki-common" % "0.3-SNAPSHOT"
)

libraryDependencies ++= twitterDeps
