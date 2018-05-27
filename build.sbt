organization := "demons"
name := "enkidu"

version := "0.3"

libraryDependencies ++= Seq(
  "io.netty" % "netty-all" % "4.1.25.Final", 
  "com.twitter" %% "util-collection" % "18.5.0",
  "com.twitter" %% "util-jvm" % "18.5.0",
  "org.scalatest" %% "scalatest" % "3.0.5" % "test"

)

