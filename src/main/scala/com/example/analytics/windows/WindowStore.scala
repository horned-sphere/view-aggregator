package com.example.analytics.windows

import java.util.UUID

import com.example.analytics.model.VisitEvent
import com.example.analytics.windows.WindowAssigner.SessionWindow

import scala.collection.{SortedSet, mutable}

/**
  * Store for window objects held in the state of a [[WindowEvents]] instance.
  * @tparam T The type of the messages.
  * @tparam W The type of the windows.
  */
trait WindowStore[T, W <: Window[T]] {

  /**
    * Add new windows the store.
    * @param windows The windows.
    */
  def addNewWindows(windows : Set[W]) : Unit

  /**
    * Get the windows containing a given messages.
    * @param record The message.
    * @return The windows, in order.
    */
  def windowsFor(record : T) : List[W]

  /**
    * Get the windows that should expire and remove them.
    * @param predicate The expiry predicate.
    * @return The expired windows, in order.
    */
  def remove(predicate : W => Boolean) : List[W]

  /**
    * Views of all windows in the store.
    * @return The windows, in order.
    */
  def windows : SortedSet[W]

}

/**
  * Default implementation that stores windows in an ordered set.
  * @tparam T The type of the messages.
  * @tparam W The type of the windows.
  */
class DefaultStore[T, W <: Window[T] : Ordering] extends WindowStore[T, W] {

  private val windowSet : mutable.TreeSet[W] = mutable.TreeSet()

  override def addNewWindows(windows: Set[W]): Unit = windowSet ++= windows

  override def windowsFor(record: T): List[W] = windowSet.filter(_.contains(record)).toList

  override def remove(predicate: W => Boolean): List[W] = {
    val toRemove = windowSet.filter(predicate)
    windowSet --= toRemove
    toRemove.toList
  }

  override def windows: SortedSet[W] = windowSet
}

/**
  * Optimised store for session windows that maintains a map from session ID to windows
  * for constant time retrieval.
  */
class SessionStore extends WindowStore[VisitEvent, SessionWindow] {

  private val windowsMap : mutable.HashMap[UUID, SessionWindow] = mutable.HashMap()
  private val windowSet : mutable.TreeSet[SessionWindow] = mutable.TreeSet()

  override def addNewWindows(windows: Set[SessionWindow]): Unit = {
    for (window <- windows) {
      windowsMap += window.create.id -> window
      windowSet += window
    }
  }

  override def windowsFor(record: VisitEvent): List[SessionWindow] =
    windowsMap.get(record.id).toList

  override def remove(predicate: SessionWindow => Boolean): List[SessionWindow] = {
    val toRemove = windowSet.filter(predicate)
    windowSet --= toRemove
    windowsMap --= toRemove.map(_.create.id)
    toRemove.toList
  }

  override def windows: SortedSet[SessionWindow] = windowSet
}
