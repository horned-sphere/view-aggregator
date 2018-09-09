package com.example.analytics.model

import java.time.Instant
import java.util.UUID

import com.example.analytics.windows.WindowAssigner.SessionWindow
import com.example.analytics.windows.{WindowContrib, WindowEvent}

/**
  * A summary for a session of [[VisitEvent]]s with the same id. This combines information
  * from the [[VisitCreate]] event and the last [[VisitUpdate]] event of a session.
  * @param id The ID of the session.
  * @param userId The ID of the user for the session.
  * @param documentId The ID of the document.
  * @param createdAt The time the session was created.
  * @param lastUpdated The time of the last update.
  * @param engagedTime The final record of the engaged time.
  * @param completion The final document completion.
  */
case class EventSummary(id : UUID,
                        userId : UUID,
                        documentId : UUID,
                        createdAt : Instant,
                        lastUpdated : Instant,
                        engagedTime : Int,
                        completion : Double) extends Timestamped {
  override def timestamp: Instant = createdAt

}
object EventSummary {

  /**
    * Create a summary from the last event of a window.
    * @param windowEvent The last event.
    * @return The summary.
    */
  def apply(windowEvent : WindowEvent[VisitEvent, SessionWindow]): EventSummary = windowEvent match {
    case WindowContrib(VisitUpdate(id, engagedTime, completion, updatedAt), SessionWindow(createEvent)) => EventSummary(
      id, createEvent.userId, createEvent.documentId, createEvent.createdAt, updatedAt,
      engagedTime, completion)
    case _ =>
      val createEvent = windowEvent.window.create
      EventSummary(createEvent.id, createEvent.userId, createEvent.documentId, createEvent.createdAt, createEvent.createdAt,
      0, 0.0)
  }

}