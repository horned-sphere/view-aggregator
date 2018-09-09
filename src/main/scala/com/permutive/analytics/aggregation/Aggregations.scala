package com.permutive.analytics.aggregation

import java.util.UUID

import com.permutive.analytics.model._
import com.permutive.analytics.windows.WindowAssigner.{SessionWindow, TimeWindow}
import com.permutive.analytics.windows.{CloseWindow, OpenWindow, WindowContrib, WindowEvent}

/**
  * Aggregation operations on the stream of events.
  */
object Aggregations {

  /**
    * Finds the last event from a [[SessionWindow]] by timestamp.
    * @param acc The state which carries forward the previous latest.
    * @param event The event to compare.
    * @return The new latest.
    */
  def lastSessionEvent(acc : WindowEvent[VisitEvent, SessionWindow],
                       event : WindowEvent[VisitEvent, SessionWindow]) : WindowEvent[VisitEvent, SessionWindow] = {
    (acc, event) match {
      case (WindowContrib(prev, _), WindowContrib(current, _)) =>
        if (current.timestamp.compareTo(prev.timestamp) >= 0) event else acc
      case (_ , WindowContrib(_, _)) => event
      case _ => acc
    }
  }


  type SessionState = (Map[UUID, (VisitCreate, Option[VisitUpdate])], List[EventSummary])
  /**
    * Scans through the  window events for [[VisitEvent]] records an emits summaries each time a window
    * closes.
    *
    * @param acc The latest record seen for each session (if any) and the previously emitted summaries.
    * @param event The latest window event, either a window open/close or a [[VisitEvent]] in the window.
    */
  def scanSessions(acc : SessionState,
                   event : WindowEvent[VisitEvent, SessionWindow]) : (Map[UUID, (VisitCreate, Option[VisitUpdate])], List[EventSummary]) = {
    val (sessionMap, _) = acc

    event match {
      //We are opening a new window so insert a record into the map.
      case OpenWindow(SessionWindow(create)) => (sessionMap + (create.id -> (create, None)), Nil)
      case WindowContrib(newUpd @ VisitUpdate(id, _ , _, updatedAt), _) =>
        sessionMap.get(id) match {
          case Some((create, Some(upd))) => if (updatedAt.isAfter(upd.updatedAt)) {
            //If we have a newer record, update the state.
            (sessionMap + (id -> (create, Some(newUpd))), Nil)
          } else {
            (sessionMap, Nil)
          }
          //We didn't have any record before so store this one.
          case Some((create, None)) => (sessionMap + (id -> (create, Some(newUpd))), Nil)
          //Record for a window that never opened, shouldn't occur.
          case _ => (sessionMap, Nil)
        }
      //Ignore events for creations and flushes.
      case WindowContrib(_, _) => (sessionMap, Nil)
      //A window is closing so emit it's state.
      case CloseWindow(SessionWindow(create)) =>
        sessionMap.get(create.id) match {
          case Some((_, optUpd)) =>
            //Remove the pending session.
            val newSessionMap = sessionMap - create.id
            optUpd match {
              //Emit the summary.
              case Some(upd) => (newSessionMap, List(EventSummary(create.id, create.userId,
                create.documentId, create.createdAt, upd.updatedAt, upd.engagedTime, upd.completion)))
              //No updates were ever received, create a 0 record.
              case _ => (newSessionMap, List(EventSummary(create.id, create.userId,
                create.documentId, create.createdAt, create.createdAt, 0, 0.0)))
            }
          //This window was never opened, shouldn't happen.
          case _ => (sessionMap, Nil)
        }

    }
  }

  /**
    * Fold summaries together for a document and hour to produce the aggregate record.
    * @param acc Accumulates the aggregate.
    * @param summaryWithWindow The summary to add to the aggregate.
    * @return The updated aggregate.
    */
  def foldSummaries(acc : Option[DocumentAggregate],
                    summaryWithWindow : (EventSummary, TimeWindow[EventSummary])) : Option[DocumentAggregate] = {
    val (summary, window) = summaryWithWindow
    val newAgg = acc match {
      case Some(agg @ DocumentAggregate(_, _, oldTotal, oldUsers, oldComp, oldTime)) =>
        agg.copy(totalVisits = oldTotal + 1,
          users = oldUsers + summary.userId,
          accumulatedCompletions = oldComp + summary.completion,
          totalTime = oldTime + summary.engagedTime)
      //Set up an empty aggregate to start.
      case _ =>
        DocumentAggregate(summary.documentId,
          window.start,
          1,
          Set(summary.userId),
          summary.completion,
          summary.engagedTime)
    }
    Some(newAgg)
  }


}
