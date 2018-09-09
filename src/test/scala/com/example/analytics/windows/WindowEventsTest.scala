package com.example.analytics.windows

import java.time.{Duration, Instant}
import java.time.temporal.ChronoUnit

import org.scalatest.{FunSpec, Matchers}

object WindowEventsTest {

  case class FakeWindow(ts : Instant, isEven : Boolean) extends Window[TimeRecord] {
    override def contains(event: TimeRecord): Boolean = if (isEven) event.n % 2 == 0 else event.n % 2 == 1
  }

  object FakeWindow {

    implicit object FakeWindowOrdering extends Ordering[FakeWindow] {
      override def compare(x: FakeWindow, y: FakeWindow): Int = x.ts.compareTo(y.ts)
    }
  }

  /**
    * Dummy assigner.
    */
  object Assigner extends WindowAssigner[TimeRecord, FakeWindow] {
    override def assignWindows(event: TimeRecord): Set[FakeWindow] =
      if (event.n % 2 == 0) Set(FakeWindow(event.timestamp, isEven = true)) else
        Set(FakeWindow(event.timestamp, isEven = false))

    override def isExpired(watermark: Instant)(window: FakeWindow): Boolean =
      window.ts.plus(1, ChronoUnit.MINUTES).isBefore(watermark)
  }

}

/**
  * Unit tests for [[WindowEvents]].
  */
class WindowEventsTest extends FunSpec with Matchers {

  import WindowEventsTest._

  describe("The window events generator") {

    it("should open and close windows correctly") {

      val generator = new WindowEvents[TimeRecord, FakeWindow](Assigner, Duration.ofMinutes(1))

      val t0 = Instant.now()

      val rec1 = TimeRecord(t0, 2)

      val expectedWindow1 = FakeWindow(t0, isEven = true)

      generator.windowEventsFor(rec1) shouldEqual
        List(OpenWindow[TimeRecord, FakeWindow](expectedWindow1), WindowContrib(rec1, expectedWindow1))

      generator.windowStore.windows.toSet shouldEqual Set(expectedWindow1)
      generator.watermark shouldEqual t0.minus(1, ChronoUnit.MINUTES)

      val t1 = t0.plus(30, ChronoUnit.SECONDS)
      val rec2 = TimeRecord(t1, 1)

      val expectedWindow2 = FakeWindow(t1, isEven = false)

      generator.windowEventsFor(rec2) shouldEqual
        List(OpenWindow[TimeRecord, FakeWindow](expectedWindow2), WindowContrib(rec2, expectedWindow2))

      generator.windowStore.windows.toSet shouldEqual Set(expectedWindow1, expectedWindow2)
      generator.watermark shouldEqual t1.minus(1, ChronoUnit.MINUTES)

      val t2 = t1.plus(91, ChronoUnit.SECONDS)
      val rec3 = TimeRecord(t2, 1)

      val expectedWindow3 = FakeWindow(t2, isEven = false)

      val events = generator.windowEventsFor(rec3)

      events.size shouldEqual 4

      events.head shouldEqual OpenWindow[TimeRecord, FakeWindow](expectedWindow3)

      events.last shouldEqual CloseWindow[TimeRecord, FakeWindow](expectedWindow1)

      events.slice(1, 3).toSet shouldEqual Set(WindowContrib(rec3, expectedWindow2), WindowContrib(rec3, expectedWindow3))

      generator.windowStore.windows.toSet shouldEqual Set(expectedWindow2, expectedWindow3)
      generator.watermark shouldEqual t2.minus(1, ChronoUnit.MINUTES)
    }
  }

}


