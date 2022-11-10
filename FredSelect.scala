package com.mchange.fredselect

import zio.*

object FredSelect extends ZIOAppDefault:

  def run =
    for {
      args <- getArgs
      _    <- actionFromArgs( args )
    }
    yield ()

  def actionFromArgs( args : Chunk[String] ) : ZIO[Any,Any,Unit] =
    args.length match
      case 0 => Console.printLine(Usage) *> ZIO.fail("A subcommand (init|update) is required.")
      case _ => args(0) match
        case "init"   =>
          val jdbcUrl = args(1)
          if args.length != 2 then 
            Console.printLine(Usage) *> ZIO.fail("Only a JDBC URL should be provided after 'init'")
          else
            for {
              _ <- Console.printLine("Initializing fred-select schema")
              _ <- DoltSchema.createSchemaTransaction( jdbcUrl, "root","")
              _ <- Console.printLine("Successfully initialized fred-select schema")
            }
            yield ()
        case "update" =>
          if args.length != 4 then 
            Console.printLine(Usage) *> ZIO.fail("A JDBC URL, the FRED API KEY, then FRED Series ID is required after init")
          else
            val jdbcUrl    = args(1)
            val apiKey     = args(2)
            val seriesName = args(3)
            for {
              _ <- Console.printLine(s"Inserting series '${seriesName}'")
              seriesMetadata     <- FredApi.seriesMetadataFetch( seriesName, apiKey )
              seriesObservations <- FredApi.seriesObservationsFetch( seriesName, apiKey )
              replaced           <- DoltSchema.updateSeriesTransaction( jdbcUrl, "root", "", seriesMetadata, seriesObservations )
              _                  <- if replaced then Console.printLine(s"Successfully updated series '${seriesName}'") else Console.printLine(s"Successfully inserted series '${seriesName}'")
            }
            yield ()
        case other    => Console.printLine(Usage) *> ZIO.fail (s"Unknown command: ${other}")

  val Usage = 
    """|
       |fredselect init <jdbc-url>
       |           update <jdbc-url> <fred-api-key> <series>
    """.stripMargin 