package com.example.analytics.windows

import java.time.{Duration, Instant}

import com.example.analytics.model.Timestamped
import com.typesafe.scalalogging.LazyLogging

/**
  * Generates a stream of [[WindowEvent]]s from an underlying stream of data.
  * @param assigner The window assigner.
  * @param watermarkLag The lag for setting the watermark to tolerate slightly out of order records.
  * @tparam T The type of the data.
  * @tparam W The type of the windows.
  */
class WindowEvents[T <: Timestamped, W <: Window[T] : Ordering](assigner : WindowAssigner[T, W],
                                                   watermarkLag : Duration,
                                                   storeFac : Option[() => WindowStore[T, W]] = None) extends LazyLogging {

  /**
    * Current level of the watermark.
    */
  private[windows] var watermark : Instant = Instant.MIN

  /**
    * Currently open windows.
    */
  private[windows] val windowStore : WindowStore[T, W] = storeFac match {
    case Some(fac) => fac()
    case _ => new DefaultStore[T, W]
  }

  /**
    * Get the events for a record.
    * @param record The record.
    * @return All generate events.
    */
  def windowEventsFor(record : T) : List[WindowEvent[T, W]] = {

    val ts = record.timestamp

    if (ts.compareTo(watermark) >= 0) {
      val newWatermark = record.timestamp.minus(watermarkLag)
      if (newWatermark.compareTo(watermark) >= 0) {
        watermark = newWatermark
      }

      val windowsFor = assigner.assignWindows(record)

      //Sort the open events so they happen in creation order.
      val newWindows = windowsFor -- windowStore.windows

      windowStore.addNewWindows(newWindows)

      val contribs = windowStore.windowsFor(record)

      if (contribs.isEmpty) {
        logger.warn(s"Record $record does not belong to any window so has been dropped.")
      }

      //Sort the close events so they happen in creation order.
      val closedWindows = windowStore.remove(assigner.isExpired(watermark))

      newWindows.map(OpenWindow[T, W]).toList ::: contribs.map(WindowContrib(record, _)) :::
        closedWindows.map(CloseWindow[T, W])

    } else {
      //Out of order records are dropped.
      logger.warn(s"Record $record with $ts was before the watermark $watermark. Record dropped.")
      Nil
    }


  }

}

object WindowEvents {

  def apply[T <: Timestamped, W <: Window[T] : Ordering](assigner : WindowAssigner[T, W],
                         watermarkLag : Duration,
                         storeFac : Option[() => WindowStore[T, W]] = None) : () => T => List[WindowEvent[T, W]] = () => {
    val windowEvents = new WindowEvents[T, W](assigner, watermarkLag, storeFac)
    event => windowEvents.windowEventsFor(event)
  }

}
