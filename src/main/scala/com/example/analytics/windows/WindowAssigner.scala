package com.example.analytics.windows

import java.time.temporal.ChronoUnit
import java.time.{Duration, Instant, LocalDateTime, ZoneOffset}

import com.example.analytics.model._

/**
  * A window over which data can be aggregated.
  * @tparam T The type of the data.
  */
trait Window[T] {

  /**
    * Whether this window contains a piece of data.
    * @param event The data.
    * @return Whether it is in the window.
    */
  def contains(event : T) : Boolean

}

/**
  * Trait for classes that can assign windows to records in a stream.
  * @tparam T The type of the records.
  * @tparam W The type of the windows.
  */
trait WindowAssigner[T, W <: Window[T]] {

  /**
    * Get all windows associated with a record.
    * @param event The record.
    * @return The windows.
    */
  def assignWindows(event : T) : Set[W]

  /**
    * Determine when a window should be removed.
    * @param watermark The current watermark.
    * @param window The window.
    * @return Whether the window is expired.
    */
  def isExpired(watermark : Instant)(window : W) : Boolean

}

object WindowAssigner {

  /**
    * A window determined by a session.
    * @param create The session creation event.
    */
  case class SessionWindow(create : VisitCreate) extends Window[VisitEvent] {
    override def contains(event: VisitEvent): Boolean = event.id == create.id
  }

  object SessionWindow {

    implicit object SessionWindowOrdering extends Ordering[SessionWindow] {
      override def compare(x: SessionWindow, y: SessionWindow): Int = x.create.createdAt.compareTo(y.create.createdAt)
    }
  }

  /**
    * A window defined by a time range.
    * @param start The start of the range.
    * @param end The end of the range.
    * @tparam T The type of the data.
    */
  case class TimeWindow[T <: Timestamped](start : LocalDateTime, end : LocalDateTime) extends Window[T] {
    override def contains(event: T): Boolean = {
      val t = LocalDateTime.ofInstant(event.timestamp, ZoneOffset.UTC)
      start.compareTo(t) <= 0 && t.compareTo(end) < 0
    }
  }

  object TimeWindow {

    implicit object TimeWindowOrdering extends Ordering[TimeWindow[EventSummary]] {
      override def compare(x: TimeWindow[EventSummary], y: TimeWindow[EventSummary]): Int = x.start.compareTo(y.start)
    }

  }

  /**
    * Assignes windows based on the session of an event.
    * @param expiryTime The expiry time of sessions.
    */
  class SessionWindows(expiryTime : Duration) extends WindowAssigner[VisitEvent, SessionWindow] {

    override def assignWindows(event: VisitEvent): Set[SessionWindow] = event match {
      case create : VisitCreate => Set(SessionWindow(create))
      case _ => Set.empty
    }

    //Windows expire when the expiry time has elapsed from the session's creation.
    override def isExpired(watermark: Instant)(window: SessionWindow): Boolean =
      isBeforeOrEqual(window.create.createdAt.plus(expiryTime), watermark)

  }

  /**
    * Tumbling, aligned windows of 1h length.
    */
  object TumblingHourWindows extends WindowAssigner[EventSummary, TimeWindow[EventSummary]]{

    override def assignWindows(summary : EventSummary): Set[TimeWindow[EventSummary]] = {
      val t = LocalDateTime.ofInstant(summary.createdAt, ZoneOffset.UTC)
      val start = t.truncatedTo(ChronoUnit.HOURS)
      val end = start.plus(1, ChronoUnit.HOURS)
      Set(TimeWindow(start, end))
    }

    override def isExpired(watermark: Instant)(window: TimeWindow[EventSummary]): Boolean =
      isBeforeOrEqual(window.end.toInstant(ZoneOffset.UTC), watermark)
  }

  def isBeforeOrEqual(t1 : Instant, t2 : Instant): Boolean = {
    t1 == t2 || t1.isBefore(t2)
  }
}
