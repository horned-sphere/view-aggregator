package com.example.analytics.windows

import java.time.temporal.ChronoUnit
import java.time.{Duration, Instant, LocalDateTime, ZoneOffset}
import java.util.UUID

import com.example.analytics.model.{EventSummary, VisitCreate, VisitUpdate}
import com.example.analytics.windows.WindowAssigner.{SessionWindow, SessionWindows, TimeWindow, TumblingHourWindows}
import org.scalatest.{FunSpec, Matchers}

/**
  * Unit tests for the window assigner strategies.
  */
class WindowAssignerTest extends FunSpec with Matchers {


  describe("A time window") {

    it("should correctly state whether a time is within the window") {

      val t = Instant.now()
      val tmin = t.minus(1, ChronoUnit.MINUTES)
      val start = LocalDateTime.ofInstant(tmin, ZoneOffset.UTC)
      val tmax = t.plus(1, ChronoUnit.MINUTES)
      val end = LocalDateTime.ofInstant(tmax, ZoneOffset.UTC)

      val window = TimeWindow[TimeRecord](start, end)

      val tMid = TimeRecord(t)
      val tStart = TimeRecord(tmin)
      val tEnd = TimeRecord(tmax)
      val tVeryLow = TimeRecord(t.minus(1, ChronoUnit.HOURS))
      val tVeryHigh = TimeRecord(t.plus(1, ChronoUnit.HOURS))

      window.contains(tMid) should be (true)
      window.contains(tStart) should be (true)
      window.contains(tEnd) should be (false)
      window.contains(tVeryLow) should be (false)
      window.contains(tVeryHigh) should be (false)

    }

  }

  describe("The session windows assigner") {

    it("should assign windows only to create events") {

      val sessionWindows = new SessionWindows(Duration.ofHours(1))

      val create = VisitCreate(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), Instant.now())

      sessionWindows.assignWindows(create) shouldEqual Set(SessionWindow(create))

      val upd = VisitUpdate(UUID.randomUUID(), 0, 0.0, Instant.now())

      sessionWindows.assignWindows(upd) shouldEqual Set.empty

    }

    it("should close sessions when they expire") {
      val sessionWindows = new SessionWindows(Duration.ofHours(1))

      val t = Instant.now()

      val create = VisitCreate(UUID.randomUUID(), UUID.randomUUID(), UUID.randomUUID(), t)

      val window = SessionWindow(create)

      sessionWindows.isExpired(t)(window) should be (false)
      sessionWindows.isExpired(t.plus(59, ChronoUnit.MINUTES))(window) should be (false)

      sessionWindows.isExpired(t.plus(60, ChronoUnit.MINUTES))(window) should be (true)

      sessionWindows.isExpired(t.plus(61, ChronoUnit.MINUTES))(window) should be (true)

    }

  }

  describe("The tumbling hour window assigner") {

    it("should assign summaries to the correct hour long bucket") {
      val t = Instant.parse("2015-05-18T23:55:49.254Z")
      val expectedStart = LocalDateTime.ofInstant(Instant.parse("2015-05-18T23:00:00.000Z"), ZoneOffset.UTC)
      val expectedEnd = expectedStart.plus(1, ChronoUnit.HOURS)

      val summary = new EventSummary(UUID.randomUUID(), UUID.randomUUID(),
        UUID.randomUUID(), t, t.plus(15, ChronoUnit.MINUTES), 0, 0.0)

      TumblingHourWindows.assignWindows(summary) shouldEqual Set(TimeWindow[EventSummary](expectedStart, expectedEnd))

    }

    it("should close windows when the watermark passes their end time") {

      val start = LocalDateTime.ofInstant(Instant.parse("2015-05-18T23:00:00.000Z"), ZoneOffset.UTC)
      val end = start.plus(1, ChronoUnit.HOURS)

      val window = TimeWindow[EventSummary](start, end)

      TumblingHourWindows.isExpired(start.toInstant(ZoneOffset.UTC))(window) should be (false)
      TumblingHourWindows.isExpired(start.toInstant(ZoneOffset.UTC)
        .plus(59, ChronoUnit.MINUTES))(window) should be (false)
      TumblingHourWindows.isExpired(end.toInstant(ZoneOffset.UTC))(window) should be (true)
    }

  }

}

