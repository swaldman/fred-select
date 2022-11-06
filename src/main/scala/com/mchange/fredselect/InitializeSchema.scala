package com.mchange.fredselect

import zio.*

object InitializeSchema extends ZIOAppDefault:
  def run =
    for {
      args <- getArgs
      _ <- ZIO.cond(args.size == 1, (),new Exception("A JDBC URL must be provided."))
      _ <- Console.printLine("Initializing fred-select schema")
      _ <- DoltSchema.createSchema( args(0), "root","")
      _ <- Console.printLine("Successfully initialized fred-select schema")
    }
    yield ()