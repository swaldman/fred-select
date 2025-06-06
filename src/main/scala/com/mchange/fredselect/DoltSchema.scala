package com.mchange.fredselect

import java.sql.*
import scala.collection.*
import scala.util.{Try,Using}
import scala.util.control.NonFatal
import zio.*

import com.mchange.sc.sqlutil

import FredApi.*

// see parameter definitions, https://fred.stlouisfed.org/docs/api/fred/series_observations.html
object DoltSchema:
  object TableName:
    val FrequencyKey          = "frequency_key"
    val SeasonalAdjustmentKey = "seasonal_adjustment_key"
    val SeriesMetadata        = "series_metadata"
    val TransformationsKey    = "transformations_key"

  val TaskTrue  : Task[Boolean] = ZIO.succeed(true)
  val TaskFalse : Task[Boolean] = ZIO.succeed(false)

  val CreateTransformationsKey = 
    s"""
    |CREATE TABLE ${TableName.TransformationsKey} (
    |  unit        CHAR(3),
    |  description VARCHAR(128),
    |  PRIMARY KEY ( unit )  
    |)
    """.stripMargin

  val PopulateTransformationsKey =
    s"""
    |INSERT INTO ${TableName.TransformationsKey}
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
  
  val CreateFrequencyKey =
    s"""
    |CREATE TABLE ${TableName.FrequencyKey} (
    |  frequency   VARCHAR(4),
    |  description VARCHAR(128),
    |  PRIMARY KEY ( frequency )  
    |)
    """.stripMargin

  val FrequenciesToDescriptions = immutable.Map (
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
  ).map{ case (k,v) => (k.toUpperCase, v) } // series seem to reference these values in upper case, despite lower case in docs

  val PopulateFrequencyKey =
    s"INSERT INTO ${TableName.FrequencyKey} VALUES " + FrequenciesToDescriptions.map { case (k,v) => s"('$k','$v')" }.mkString(", ")

  // we don't have an exhaustive list for this one, so we'll fill it lazily
  val CreateSeasonalAdjustmentKey =
    s"""
    |CREATE TABLE ${TableName.SeasonalAdjustmentKey} (
    |  seasonal_adjustment   VARCHAR(8),
    |  description           VARCHAR(128),
    |  PRIMARY KEY ( seasonal_adjustment )  
    |)
    """.stripMargin

  val CountInSeasonalAdjustmentKeyQuery = s"SELECT COUNT(*) FROM ${TableName.SeasonalAdjustmentKey} WHERE seasonal_adjustment = ?"
  val SeasonalAdjustmentKeyInsertion    = s"INSERT INTO ${TableName.SeasonalAdjustmentKey} VALUES (?, ?)"

  val CreateSeriesMetadata = 
    s"""
    |CREATE TABLE ${TableName.SeriesMetadata} (
    |  series              VARCHAR(64),
    |  title               VARCHAR(256),
    |  frequency           VARCHAR(4),
    |  units               VARCHAR(128),
    |  seasonal_adjustment VARCHAR(8),
    |  notes               TEXT,
    |  timestamp           TIMESTAMP,
    |  PRIMARY KEY (series),
    |  FOREIGN KEY (frequency) REFERENCES frequency_key(frequency),
    |  FOREIGN KEY (seasonal_adjustment) REFERENCES seasonal_adjustment_key(seasonal_adjustment)
    |)
    """.stripMargin

  val SeriesMetadataInsertion = s"INSERT INTO ${TableName.SeriesMetadata} VALUES (?, ?, ?, ?, ?, ?, NOW())"
  val CountInSeriesMetadataQuery = s"SELECT COUNT(*) FROM ${TableName.SeriesMetadata} WHERE series = ?"


  def transact[A]( conn : Connection )( block : Connection => ZIO[Any,Throwable,A] ) : ZIO[Any, Throwable, A] =
    def revertAutoCommit(oldAutoCommit : Boolean) : ZIO[Any,Nothing,Unit] = 
      ZIO.attemptBlocking(conn.setAutoCommit(oldAutoCommit)).catchAll(t => ZIO.succeed(t.printStackTrace()))

    for {
      autoCommit <- ZIO.attemptBlocking( conn.getAutoCommit )
      _   <- ZIO.attemptBlocking( conn.setAutoCommit(false) )
      out <- block(conn).catchSome{ case NonFatal(t) => conn.rollback(); t.printStackTrace(); throw t }
      _   <- ZIO.attemptBlocking( conn.commit() ).ensuring(revertAutoCommit(autoCommit))
    } yield out

  def executeCustomized[A]( conn : Connection, queryTemplate : String, execution : PreparedStatement => A)( customization : PreparedStatement => Unit ) :ZIO[Any,Throwable, A] = 
    val doIt = ZIO.attemptBlocking {
      Using.resource(conn.prepareStatement(queryTemplate)){ ps =>
        customization(ps)
        execution(ps)
      }
    }
    //Console.printLine(s"Uncustomized query: ${queryTemplate}").flatMap( _ => doIt )
    doIt

  def executeCustomizedUpdate( conn : Connection, queryTemplate : String )( customization : PreparedStatement => Unit ) =
    executeCustomized( conn, queryTemplate, _.executeUpdate())(customization)

  def executeCustomizedMaybeSingleIntQuery(conn : Connection, queryTemplate : String, rsIndex : Int = 1)( customization : PreparedStatement => Unit ) =
    val extractor : ResultSet => Int = { rs =>
      rs.getInt(rsIndex)
    }
    val execution : PreparedStatement => Option[Int] = { ps =>
      val rs = ps.executeQuery()
      sqlutil.zeroOrOneResult("customized-maybe-single-int-query", rs)(extractor)
    }
    executeCustomized( conn, queryTemplate, execution )( customization )

  def executeCustomizedSingleIntQuery(conn : Connection, queryTemplate : String, rsIndex : Int = 1)( customization : PreparedStatement => Unit ) =
    val extractor : ResultSet => Int = { rs =>
      rs.getInt(rsIndex)
    }
    val execution : PreparedStatement => Int = { ps =>
      val rs = ps.executeQuery()
      sqlutil.uniqueResult("customised-single-int-query",rs)(extractor)
    }
    executeCustomized( conn, queryTemplate, execution )( customization )

  def executeUpdate(conn : Connection, query : String) =
    executeCustomizedUpdate(conn, query){ ps => () }

  def mbAbsBigInt(value : String) : Option[BigInt] = Try(BigInt(value).abs).toOption

  def valueSqlType(goodObservations : List[SeriesObservation]) : String =
    def maxAbsIntegral : Option[BigInt] = goodObservations.tail.foldLeft(mbAbsBigInt(goodObservations.head.value)){ (prev, next) =>
        prev match
          case None          => None
          case Some(lastNum) => mbAbsBigInt(next.value) match
            case None          => None
            case Some(nextNum) => if (nextNum > lastNum) Some(nextNum) else prev
      }
    def integralType(absNum : BigInt) : String =
      if      absNum <         128 then "TINYINT"
      else if absNum <       32768 then "SMALLINT"
      else if absNum <     8388608 then "MEDIUMINT"
      else if absNum < 2147483648L then "INT"
      else                             "BIGINT"
    def allDouble : Boolean = goodObservations.forall(obs => Try( obs.value.toDouble ).isSuccess)

    maxAbsIntegral match
      case Some(absNum)      => integralType(absNum)
      case None if allDouble => "DOUBLE PRECISION"
      case _                 => throw new Exception(s"Failed to interpret values ${goodObservations.map(_.value).mkString(",")} into an SQL type.")
  end valueSqlType 

  def createObservationsTable( conn : Connection, series : String, valueSqlType : String) =
    executeUpdate(conn, s"CREATE TABLE ${series} (observation_date DATE, value ${valueSqlType}, PRIMARY KEY (observation_date))")

  def populateObservationsTable( conn : Connection, series : String, goodObservations : List[SeriesObservation]) =
    val sb = new StringBuilder(512) // XXX: hard-coded
    sb.append(s"INSERT INTO ${series} VALUES ")
    val insertionTuples = goodObservations.map( obs => s"('${obs.date}', ${obs.value})" ).mkString(", ")
    sb.append(insertionTuples)
    executeUpdate( conn, sb.toString )

  def ensureSeasonalAdjustment( conn : Connection, metadata : SeriesMetadata ) =
    def doInsert = executeCustomizedUpdate(conn, SeasonalAdjustmentKeyInsertion){ ps =>
      ps.setString(1, metadata.seasonal_adjustment_short)
      ps.setString(2, metadata.seasonal_adjustment)
    }
    val checkQuery = executeCustomizedSingleIntQuery(conn, CountInSeasonalAdjustmentKeyQuery)( _.setString(1, metadata.seasonal_adjustment_short) )
    checkQuery.flatMap { numRows =>
      assert( numRows == 0 || numRows == 1, s"There should be precisely zero or one rows in ${TableName.SeasonalAdjustmentKey} for key '${metadata.seasonal_adjustment_short}'")
      if numRows == 0 then doInsert else ZIO.unit
    }

  def insertSeriesMetadata( conn : Connection, metadata : SeriesMetadata) =
    ensureSeasonalAdjustment(conn, metadata).flatMap { _ =>
      executeCustomizedUpdate(conn, SeriesMetadataInsertion){ ps =>
        ps.setString(1, metadata.id)
        ps.setString(2, metadata.title)
        ps.setString(3, metadata.frequency_short)
        ps.setString(4, metadata.units)
        ps.setString(5, metadata.seasonal_adjustment_short)
        ps.setString(6, metadata.notes)
      }
    }

  // def insertSeries( conn : Connection )

  def createSchemaLoose(conn : Connection) : ZIO[Any,Throwable,Unit] =
    for {
      _ <- executeUpdate(conn, CreateTransformationsKey)
      _ <- executeUpdate(conn, PopulateTransformationsKey)
      _ <- executeUpdate(conn, CreateFrequencyKey)
      _ <- executeUpdate(conn, PopulateFrequencyKey)
      _ <- executeUpdate(conn, CreateSeasonalAdjustmentKey)
      _ <- executeUpdate(conn, CreateSeriesMetadata)
    }
    yield ()

  def createSchemaTransaction( conn : Connection ) : ZIO[Any,Throwable,Unit] =
    transact(conn)( createSchemaLoose )
  
  def withConnection[A]( jdbcUrl : String, user : String, password : String )( action : Connection => ZIO[Any,Throwable,A]) : ZIO[Any,Throwable,A] =
    def acquireConnection() = ZIO.attempt(DriverManager.getConnection(jdbcUrl,user,password))
    def releaseConnection( conn : Connection ) = ZIO.succeed( conn.close() )
    ZIO.acquireReleaseWith(acquireConnection())(releaseConnection)(action)

  def createSchemaTransaction( jdbcUrl : String, user : String, password : String ) : ZIO[Any,Throwable,Unit] =
    withConnection(jdbcUrl, user, password)( createSchemaTransaction )

  def insertSeriesTransaction( conn : Connection, seriesMetadata : SeriesMetadata, seriesObservations : SeriesObservations ) : ZIO[Any,Throwable,Unit] =
    transact(conn)( conn => insertSeriesLoose(conn, seriesMetadata, seriesObservations))

  def insertSeriesLoose( conn : Connection, seriesMetadata : SeriesMetadata, seriesObservations : SeriesObservations ) : ZIO[Any,Throwable,Unit] =
    val goodObservations = seriesObservations.observations.filter( _.value.trim != "." ) // null values seem to be represented as '.'
    for {
      _ <- insertSeriesMetadata(conn, seriesMetadata)
      _ <- createObservationsTable(conn, seriesMetadata.id, valueSqlType(goodObservations))
      _ <- populateObservationsTable(conn, seriesMetadata.id, goodObservations)
    }
    yield()

  def insertSeriesTransaction( jdbcUrl : String, user : String, password : String, seriesMetadata : SeriesMetadata, seriesObservations : SeriesObservations ) : ZIO[Any,Throwable,Unit] =
    withConnection(jdbcUrl, user, password)( conn => insertSeriesTransaction(conn, seriesMetadata, seriesObservations) )

  def dropObservationsTable( conn : Connection, series : String ) = executeUpdate(conn, s"DROP TABLE ${series}")

  def deleteSeriesMetadata( conn : Connection, series : String ) = executeCustomizedUpdate(conn, s"DELETE FROM ${TableName.SeriesMetadata} WHERE series = ?")(_.setString(1,series))

  def dropSeriesLoose( conn : Connection, series : String ) =
    for {
      _ <- dropObservationsTable( conn, series )
      _ <- deleteSeriesMetadata( conn, series )
    } yield()

  def dropSeriesIfPresentLoose( conn : Connection, series : String ) : Task[Boolean] =
    val checkQuery = executeCustomizedSingleIntQuery(conn, CountInSeriesMetadataQuery)( _.setString(1, series) )
    checkQuery.flatMap { numRows =>
      assert( numRows == 0 || numRows == 1, s"There should be precisely zero or one rows in ${TableName.SeriesMetadata} for series '${series}'")
      if numRows == 1 then dropSeriesLoose(conn, series).flatMap( _ => TaskTrue) else TaskFalse
    }

  def updateSeriesLoose( conn : Connection, seriesMetadata : SeriesMetadata, seriesObservations : SeriesObservations ) : Task[Boolean] =
    for {
      replaced <- dropSeriesIfPresentLoose(conn, seriesMetadata.id)
      _        <- insertSeriesLoose(conn, seriesMetadata, seriesObservations)
    }
    yield replaced

  def updateSeriesTransaction(conn : Connection, seriesMetadata : SeriesMetadata, seriesObservations : SeriesObservations) : Task[Boolean] =
    transact(conn)(conn => updateSeriesLoose( conn, seriesMetadata, seriesObservations ))

  def updateSeriesTransaction(jdbcUrl : String, user : String, password : String, seriesMetadata : SeriesMetadata, seriesObservations : SeriesObservations ) : Task[Boolean] =
    withConnection(jdbcUrl,user,password)(conn => updateSeriesTransaction(conn, seriesMetadata, seriesObservations))

