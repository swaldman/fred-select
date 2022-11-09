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
      //_                  <- Console.printLine( seriesMetadata )
      seriesObservations <- FredApi.seriesObservationsFetch( seriesName, apiKey )
      replaced           <- DoltSchema.updateSeriesTransaction( args(0), "root", "", seriesMetadata, seriesObservations )
      _                  <- if replaced then Console.printLine(s"Successfully updated series '${seriesName}'") else Console.printLine(s"Successfully inserted series '${seriesName}'")
    }
    yield ()