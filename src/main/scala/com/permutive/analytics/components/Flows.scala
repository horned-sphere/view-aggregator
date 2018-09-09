package com.permutive.analytics.components

import java.time.Duration

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.permutive.analytics.aggregation.{Aggregations, Filters}
import com.permutive.analytics.model.{DocumentAggregate, EventSummary, VisitEvent}
import com.permutive.analytics.windows.WindowAssigner.{SessionWindows, TumblingHourWindows}
import com.permutive.analytics.windows.{SessionStore, WindowEvents}

/**
  * Modular components of the data flow.
  * @param sessionExpiry Time after which a session expires (and emits a summary).
  * @param watermarkLag The watermark lag for tolerating out of order records.
  * @param maxSessions Maximum number of permissible sessions.
  * @param maxTimeWindows Maximum number of time windows being processed at once.
  * @param maxDocumentsPerWindow Maximum number of documents being processed per time window.
  */
class Flows(sessionExpiry : Duration, watermarkLag : Duration,
            maxSessions : Int, maxTimeWindows : Int,
            maxDocumentsPerWindow : Int) {

  /**
    * A flow that summarises [[VisitEvent]]s by combining the last [[com.permutive.analytics.model.VisitUpdate]]
    * in a session with the corresponding [[com.permutive.analytics.model.VisitCreate]].
    * @param in The flow of events.
    * @tparam T The type of the original inputs.
    * @return The summarized flow.
    */
  def createSummaryFlow[T](in : Flow[T, VisitEvent, NotUsed]): Flow[T, EventSummary, NotUsed] = {
    val sessionWindows = new SessionWindows(sessionExpiry)

    val storeFac = Some(() => new SessionStore)
    in.statefulMapConcat(WindowEvents(sessionWindows, watermarkLag, storeFac))
      .scan[Aggregations.SessionState]((Map.empty, Nil))(Aggregations.scanSessions)
      .mapConcat(_._2)
  }

  /**
    * A flow that summarises [[VisitEvent]]s by combining the last [[com.permutive.analytics.model.VisitUpdate]]
    * in a session with the corresponding [[com.permutive.analytics.model.VisitCreate]].
    * @param in The flow of events.
    * @tparam T The type of the original inputs.
    * @return The summarized flow.
    */
  def createSummaryFlowAlt[T](in : Flow[T, VisitEvent, NotUsed]): Flow[T, EventSummary, NotUsed] = {
    val sessionWindows = new SessionWindows(sessionExpiry)

    in.statefulMapConcat(WindowEvents(sessionWindows, watermarkLag))

    .groupBy(maxSessions, _.window)
    .takeWhile(event => !event.isClose)
    .reduce(Aggregations.lastSessionEvent)
    .map(EventSummary(_))
    .mergeSubstreams
  }

  /**
    * Flow component that aggregates the a stream of summaries by document and hour.
    * @param in The flow of summaries.
    * @tparam T The original type of the inputs.
    * @return The flow of aggregates.
    */
  def createAggregateFlow[T](in : Flow[T, EventSummary, NotUsed]): Flow[T, DocumentAggregate, NotUsed] = {
    import com.permutive.analytics.windows.WindowAssigner.TimeWindow._
    in.statefulMapConcat(WindowEvents(TumblingHourWindows, watermarkLag))
      .groupBy(maxTimeWindows, _.window)
      .takeWhile(event => !event.isClose)
      .mapConcat(Filters.filterContribs)
      .groupBy(maxDocumentsPerWindow, _._1.documentId)
      .fold[Option[DocumentAggregate]](None)(Aggregations.foldSummaries)
      .mapConcat(_.toList)
      .mergeSubstreams
      .mergeSubstreams
  }

}
