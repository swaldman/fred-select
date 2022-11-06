val ScalaVersion = "3.2.0"
val SttpVersion  = "3.8.3"

lazy val root = project
  .in(file("."))
  .settings(
    organization        := "com.mchange",
    name                := "fred-select",
    version             := "0.0.1-SNAPSHOT",
    scalaVersion        := ScalaVersion,
    libraryDependencies += "com.softwaremill.sttp.client3" %% "zio"               % SttpVersion,
    libraryDependencies += "com.softwaremill.sttp.client3" %% "zio-json"          % SttpVersion,
    libraryDependencies += "com.mysql"                      % "mysql-connector-j" % "8.0.31",     
    // libraryDependencies += "dev.zio" %% "zio-json" % "0.3.0",
    resolvers += Resolver.mavenLocal
  )
