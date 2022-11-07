package com.mchange.fredselect

import zio.*

object InsertSeries extends ZIOAppDefault:
  def run =
    for {
      args              <- getArgs
      _                 <- ZIO.cond(args.size == 3, (),new Exception("A JDBC URL must be provided."))
      jdbcUrl    = args(0)
      apiKey     = args(1)
      seriesName = args(2)
      _ <- Console.printLine(s"Inserting series '${seriesName}'")
      seriesMetadata     <- FredApi.seriesMetadataFetch( seriesName, apiKey )
      seriesObservations <- FredApi.seriesObservationsFetch( seriesName, apiKey )
      _                  <- DoltSchema.insertSeries( args(0), "root", "", seriesMetadata, seriesObservations )
      _                  <- Console.printLine(s"Successfully inserted series ${seriesName}")
    }
    yield ()