package com.mchange.fredselect

import sttp.client3.*
import sttp.client3.httpclient.zio.HttpClientZioBackend
import sttp.client3.ziojson.*

import zio.*
import zio.json.*

object FredApi:

  final case class SeriesMetadata(
    id                        : String,
    realtime_start            : String,
    realtime_end              : String,
    title                     : String,
    observation_start         : String,
    observation_end           : String,
    frequency                 : String,
    frequency_short           : String,
    units                     : String,
    units_short               : String,
    seasonal_adjustment       : String,
    seasonal_adjustment_short : String,
    last_updated              : String,
    popularity                : Int,
    notes                     : String
  )
  final case class SeriessMetadata(realtime_start : String, realtime_end : String, seriess : List[SeriesMetadata])

  final case class SeriesObservation(realtime_start : String, realtime_end : String, date : String, value : String)
  final case class SeriesObservations(
    realtime_start            : String,
    realtime_end              : String,
    observation_start         : String,
    observation_end           : String,
    units                     : String,
    output_type               : Long,
    file_type                 : String,
    order_by                  : String,
    sort_order                : String,
    count                     : Long,
    offset                    : Long,
    limit                     : Long,
    observations              : List[SeriesObservation]
  )

  implicit val SeriesMetadataDecoder  : JsonDecoder[SeriesMetadata]  = DeriveJsonDecoder.gen[SeriesMetadata]
  implicit val SeriessMetadataDecoder : JsonDecoder[SeriessMetadata] = DeriveJsonDecoder.gen[SeriessMetadata]
  
  implicit val SeriesObservationDecoder  : JsonDecoder[SeriesObservation]  = DeriveJsonDecoder.gen[SeriesObservation]
  implicit val SeriesObservationsDecoder : JsonDecoder[SeriesObservations] = DeriveJsonDecoder.gen[SeriesObservations]

  // don't blame me for the seriess thing, it is straight out of the Fred API
  def seriesMetadataFetch( series : String, apiKey : String ) : ZIO[Any, Throwable, FredApi.SeriesMetadata] = 
    HttpClientZioBackend().flatMap { backend =>
      val request = basicRequest.get(uri"https://api.stlouisfed.org/fred/series?series_id=${series}&api_key=${apiKey}&file_type=json")
      //println(request)
      for {
        response        <- request.response(asJson[SeriessMetadata]).send(backend)
        seriessMetadata <- ZIO.fromEither(response.body)
      }
      yield seriessMetadata.seriess.head
    }

  def seriesObservationsFetch( series : String, apiKey : String ) : ZIO[Any, Throwable, FredApi.SeriesObservations] = 
    HttpClientZioBackend().flatMap { backend =>
      val request = basicRequest.get(uri"https://api.stlouisfed.org/fred/series/observations?series_id=${series}&api_key=${apiKey}&file_type=json")
      //println(request)
      for {
        response           <- request.response(asJson[SeriesObservations]).send(backend)
        seriesObservations <- ZIO.fromEither(response.body)
      }
      yield seriesObservations
    }
    

