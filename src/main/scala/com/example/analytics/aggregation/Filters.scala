package com.example.analytics.aggregation

import com.example.analytics.windows.{Window, WindowContrib, WindowEvent}

object Filters {

  /**
    * Filter out window open and close events.
    * @param event The event.
    * @tparam T The type of the event.
    * @tparam W The type of the window.
    * @return A list that is either empty or a singelton event.
    */
  def filterContribs[T, W <: Window[T]](event : WindowEvent[T, W]) : List[(T, W)] = event match {
    case WindowContrib(record, window) => List((record, window))
    case _ => Nil
  }

}
