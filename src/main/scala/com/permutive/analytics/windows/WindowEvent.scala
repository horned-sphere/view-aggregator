package com.permutive.analytics.windows

/**
  * An event in a windowed stream.
  * @tparam T The type of the underlying stream.
  * @tparam W The type of the window.
  */
sealed trait WindowEvent[T, W <: Window[T]]{
  /**
    * @return The window to which the event corresponds.
    */
  def window : W

  /**
    * @return Whether this is a close window event.
    */
  def isClose : Boolean
}

/**
  * Open a new window.
  * @param window The window.
  * @tparam T The type of the underlying stream.
  * @tparam W The type of the window.
  */
case class OpenWindow[T, W <: Window[T]](window : W) extends WindowEvent[T, W] {
  override def isClose: Boolean = false
}

/**
  * Contribute a record to a window.
  * @param event The record.
  * @param window The window.
  * @tparam T The type of the underlying stream.
  * @tparam W The type of the window.
  */
case class WindowContrib[T, W <: Window[T]](event : T, window : W) extends WindowEvent[T, W] {
  override def isClose: Boolean = false
}

/**
  * Close a window.
  * @param window The window.
  * @tparam T The type of the underlying stream.
  * @tparam W The type of the window.
  */
case class CloseWindow[T, W <: Window[T]](window : W) extends WindowEvent[T, W] {
  override def isClose: Boolean = true
}
