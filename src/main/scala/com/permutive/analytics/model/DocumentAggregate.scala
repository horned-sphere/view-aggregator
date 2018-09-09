package com.permutive.analytics.model

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoUnit
import java.util.UUID

/**
  * An aggregate of [[EventSummary]] records for a clock hour and single document.
  * @param documentId The ID of the document.
  * @param epoch The epoch of the aggregate.
  * @param totalVisits Total number of visits to the document in the time.
  * @param users Set if users who visited the document in that time.
  * @param accumulatedCompletions Accumulated complete document reads equivalent.
  * @param totalTime The total time spend on the document by all users.
  */
case class DocumentAggregate(documentId : UUID,
                             epoch : LocalDateTime,
                             totalVisits : Int,
                             users : Set[UUID],
                             accumulatedCompletions : Double,
                             totalTime : Int) {
  import DocumentAggregate._

  private def startTime = Format.format(epoch)
  private def endTime = Format.format(epoch.plus(1, ChronoUnit.HOURS))

  /**
    * Format the aggregate as a line.
    * @return The line.
    */
  def toLine: String = s"$documentId, $startTime, $endTime, $totalVisits, ${users.size}, $totalTime, $accumulatedCompletions"

}

object DocumentAggregate {

  val Format: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:00")

}
