val ScalaVersion = "3.3.6"
val SttpVersion  = "3.8.3"

enablePlugins(JavaAppPackaging)

lazy val root = project
  .in(file("."))
  .settings(
    organization        := "com.mchange",
    name                := "fred-select",
    version             := "0.0.2-SNAPSHOT",
    scalaVersion        := ScalaVersion,
    libraryDependencies += "com.softwaremill.sttp.client3" %% "zio"               % SttpVersion,
    libraryDependencies += "com.softwaremill.sttp.client3" %% "zio-json"          % SttpVersion,
    libraryDependencies += "com.mchange"                   %% "sqlutil-scala"     % "0.0.2-SNAPSHOT",     
    libraryDependencies += "com.mysql"                      % "mysql-connector-j" % "8.0.31",
    // libraryDependencies += "dev.zio" %% "zio-json" % "0.3.0",
    resolvers += Resolver.mavenLocal,
    fork := true
  )
