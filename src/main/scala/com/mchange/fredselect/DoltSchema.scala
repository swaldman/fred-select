package com.mchange.fredselect

import java.sql.*
import scala.collection.*
import scala.util.Using
import zio.*

// see parameter definitions, https://fred.stlouisfed.org/docs/api/fred/series_observations.html
object DoltSchema:
  final val CreateUnitsKey = 
    """
    |CREATE TABLE units_key (
    |  unit        CHAR(3),
    |  description VARCHAR(128),
    |  PRIMARY KEY ( unit )  
    |)
    """.stripMargin

  final val PopulateUnitsKey =
    """
    |INSERT INTO units_key
    |VALUES
    |  ('lin','Levels'),
    |  ('chg','Change'),
    |  ('ch1','Change from Year Ago'),
    |  ('pch','Percent Change'),
    |  ('pc1','Percent Change from Year Ago'),
    |  ('pca','Compounded Annual Rate of Change'),
    |  ('cch','Continuously Compounded Rate of Change'),
    |  ('cca','Continuously Compounded Annual Rate of Change'),
    |  ('log','Natural Log')
    """.stripMargin
  
  final val CreateFrequencyKey =
    """
    |CREATE TABLE frequency_key (
    |  frequency   VARCHAR(4),
    |  description VARCHAR(128),
    |  PRIMARY KEY ( frequency )  
    |)
    """.stripMargin

  final val FrequenciesToDescriptions = immutable.Map (
    "d"->"Daily",
    "w"->"Weekly",
    "bw"->"Biweekly",
    "m"->"Monthly",
    "q"->"Quarterly",
    "sa"->"Semiannual",
    "a"->"Annual",
    "wef"->"Weekly, Ending Friday",
    "weth"->"Weekly, Ending Thursday",
    "wew"->"Weekly, Ending Wednesday",
    "wetu"->"Weekly, Ending Tuesday",
    "wem"->"Weekly, Ending Monday",
    "wesu"->"Weekly, Ending Sunday",
    "wesa"->"Weekly, Ending Saturday",
    "bwew"->"Biweekly, Ending Wednesday",
    "bwem"->"Biweekly, Ending Monday"
  )

  final val PopulateFrequencyKey =
    s"INSERT INTO frequency_key VALUES " + FrequenciesToDescriptions.map { case (k,v) => s"('$k','$v')" }.mkString(", ")

  // we don't have an exhaustive list for this one, so we'll fill it lazily
  final val CreateSeasonalAdjustmentKey =
    """
    |CREATE TABLE seasonal_adjustment_key (
    |  seasonal_adjustment   VARCHAR(8),
    |  description           VARCHAR(128),
    |  PRIMARY KEY ( seasonal_adjustment )  
    |)
    """.stripMargin


  final val CreateSeriesMetadata = 
    """
    |CREATE TABLE series_metadata (
    |  series              VARCHAR(64),
    |  title               VARCHAR(256),
    |  frequency           VARCHAR(4),
    |  units               CHAR(3),
    |  seasonal_adjustment VARCHAR(8),
    |  notes               TEXT,
    |  timestamp           TIMESTAMP,
    |  PRIMARY KEY (series),
    |  FOREIGN KEY (frequency) REFERENCES frequency_key(frequency),
    |  FOREIGN KEY (units) REFERENCES units_key(unit),
    |  FOREIGN KEY (seasonal_adjustment) REFERENCES seasonal_adjustment_key(seasonal_adjustment)
    |)
    """.stripMargin

  def executeCustomizedUpdate( conn : Connection, queryTemplate : String )( customization : PreparedStatement => Unit) = 
    val doIt = ZIO.attemptBlocking {
      Using.resource(conn.prepareStatement(queryTemplate)){ ps =>
        customization(ps)
        ps.executeUpdate()
      }
    }
    Console.printLine(s"Uncustomized query: ${queryTemplate}").flatMap( _ => doIt )

  def executeUpdate(conn : Connection, query : String) =
    executeCustomizedUpdate(conn, query){ ps => () }

  def createObservationsTable(conn : Connection, frequency : String) =
    executeUpdate(conn, s"CREATE TABLE observations_${frequency} (observation_date DATE)")

  def createObservartionsTables(conn : Connection) =
    FrequenciesToDescriptions.keys.foldLeft( ZIO.attempt(0) ){case (prev, next) => prev *> createObservationsTable(conn, next)}

  def createSchema(conn : Connection) : ZIO[Any,Any,Unit] =
    for {
      _ <- executeUpdate(conn, CreateUnitsKey)
      _ <- executeUpdate(conn, PopulateUnitsKey)
      _ <- executeUpdate(conn, CreateFrequencyKey)
      _ <- executeUpdate(conn, PopulateFrequencyKey)
      _ <- executeUpdate(conn, CreateSeasonalAdjustmentKey)
      _ <- executeUpdate(conn, CreateSeriesMetadata)
      _ <- createObservartionsTables(conn)
    }
    yield ()
  
  def createSchema( jdbcUrl : String, user : String, password : String ) : ZIO[Any,Any,Unit] =
    def acquireConnection() = ZIO.attempt(DriverManager.getConnection(jdbcUrl,user,password))
    def releaseConnection( conn : Connection ) = ZIO.succeed( conn.close() )
    ZIO.acquireReleaseWith(acquireConnection())(releaseConnection)(createSchema)
