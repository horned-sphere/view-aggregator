package com.example.analytics.model

import java.time.Instant
import java.util.UUID

import org.scalatest.{FunSpec, Inside, Matchers}

/**
  * Unit tests for [[VisitEvent]].
  */
class VisitEventDeserializationTest extends FunSpec with Matchers with Inside {

  describe("The JSON protocol for visit messages") {

    it("should deserialize a visit create message correctly") {

      val message =
        """{"messageType":"VisitCreate","visit":{"id":"3a98e4f3-49cf-48a1-b63a-3eeaa2443f26","userId":"630adcc1-e302-40a6-8af2-d2cd84e71720","documentId":"b61d8914-560f-4985-8a5f-aa974ad0c7ab","createdAt":"2015-05-18T23:55:49.254Z"}}"""

      inside(VisitEvent.parse(message)) {

        case Left(VisitCreate(id, userId, docId, createdAt)) =>
          id shouldEqual UUID.fromString("3a98e4f3-49cf-48a1-b63a-3eeaa2443f26")
          userId shouldEqual UUID.fromString("630adcc1-e302-40a6-8af2-d2cd84e71720")
          docId shouldEqual UUID.fromString("b61d8914-560f-4985-8a5f-aa974ad0c7ab")
          createdAt shouldEqual Instant.parse("2015-05-18T23:55:49.254Z")

      }

    }

    it("should deserialize a visit update message correctly") {
      val message = """{"messageType":"VisitUpdate","visit":{"id":"9ee0db5b-ff3a-444a-9730-1aa57213cd62","engagedTime":25,"completion":0.5555555555555556,"updatedAt":"2015-05-18T23:55:49.210Z"}}"""

      inside(VisitEvent.parse(message)) {
        case Left(VisitUpdate(id, engagedTime, completion, updatedAt)) =>
          id shouldEqual UUID.fromString("9ee0db5b-ff3a-444a-9730-1aa57213cd62")
          engagedTime shouldEqual 25
          completion shouldEqual 0.5555555555555556
          updatedAt shouldEqual Instant.parse("2015-05-18T23:55:49.210Z")
      }
    }

    it("should produce error messages for invalid records") {
      val message1 =
        """{"messageType":"VisitHello","visit":{"id":"3a98e4f3-49cf-48a1-b63a-3eeaa2443f26","userId":"630adcc1-e302-40a6-8af2-d2cd84e71720","documentId":"b61d8914-560f-4985-8a5f-aa974ad0c7ab","createdAt":"2015-05-18T23:55:49.254Z"}}"""

      inside(VisitEvent.parse(message1)) {
        case Right(errs) => errs.nonEmpty should be (true)
      }

      val message2 = """{"messageType":"VisitUpdate","visit":{"id":"9ee0db5b-ff3a-444a-9730-1aa57213cd62h","engagedTime":25,"completion":0.5555555555555556,"updatedAt":"2015-05-18T23:55:49.210Z"}}"""

      inside(VisitEvent.parse(message2)) {
        case Right(errs) => errs.nonEmpty should be (true)
      }

      val message3 = "nonsense"

      inside(VisitEvent.parse(message3)) {
        case Right(errs) => errs.nonEmpty should be (true)
      }
    }

  }

}
