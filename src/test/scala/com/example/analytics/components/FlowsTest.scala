package com.example.analytics.components

import java.time.temporal.ChronoUnit
import java.time.{Duration, Instant, LocalDateTime, ZoneOffset}
import java.util.UUID

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Sink, Source}
import akka.testkit.TestKit
import com.example.analytics.model._
import org.scalatest._

import scala.collection.immutable
import scala.concurrent.Await
import scala.concurrent.duration._

/**
  * Unit and integration tests for the streaming graph components in [[Flows]].
  */
class FlowsTest extends TestKit(ActorSystem("TestSystem")) with FunSpecLike with Matchers with BeforeAndAfterAll {

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
  }

  implicit val materializer : ActorMaterializer = ActorMaterializer()

  import FlowsTest._

  describe("The summary flow") {

    it("should compute the correct summary for a session") {

      val flows = new Flows(Duration.ofHours(1),
        Duration.ofMinutes(5),
        2,
        2,
        2)

      val flow = flows.createSummaryFlow(Flow[VisitEvent])
      val future = Source(Messages1 ::: FlushTail).via(flow).runWith(Sink.seq)

      val result = Await.result(future, 5.seconds)

      result.size shouldEqual 1

      result.head shouldEqual EventSummary(SessionId1,
        Create1.userId,
        Create1.documentId,
        Create1.createdAt,
        Messages1.last.timestamp,
        3,
        3.0)
    }

    it("should tolerate out of order updates") {
      val flows = new Flows(Duration.ofHours(1),
        Duration.ofMinutes(5),
        2,
        2,
        2)

      val flow = flows.createSummaryFlow(Flow[VisitEvent])

      val OutOfOrderUpdates = Create1 :: Updates1.head :: Updates1(2) :: Updates1(1) :: FlushTail

      val future = Source(OutOfOrderUpdates).via(flow).runWith(Sink.seq)

      val result = Await.result(future, 5.seconds)

      result.size shouldEqual 1

      result.head shouldEqual EventSummary(SessionId1,
        Create1.userId,
        Create1.documentId,
        Create1.createdAt,
        Updates1.last.timestamp,
        3,
        3.0)
    }

    it("should compute correct summaries for interleaved sessions") {
      val flows = new Flows(Duration.ofHours(1),
        Duration.ofMinutes(5),
        2,
        2,
        2)

      val flow = flows.createSummaryFlow(Flow[VisitEvent])
      val future = Source(Interleaved ::: FlushTail).via(flow).runWith(Sink.seq)

      val result = Await.result(future, 5.seconds)

      result.size shouldEqual 2

      result.head shouldEqual EventSummary(SessionId1,
        Create1.userId,
        Create1.documentId,
        Create1.createdAt,
        Updates1.last.timestamp,
        3,
        3.0)

      result(1) shouldEqual EventSummary(SessionId2,
        Create2.userId,
        Create2.documentId,
        Create2.createdAt,
        Updates2.last.timestamp,
        6,
        6.0)
    }

  }

  describe("The aggregation flow") {

    it("should aggregate records in the same hour, for the same document") {
      val flows = new Flows(Duration.ofHours(1),
        Duration.ofMinutes(5),
        2,
        2,
        2)

      val flow = flows.createAggregateFlow(Flow[EventSummary])
      val future = Source(Summaries1).via(flow).runWith(Sink.seq)

      val result = Await.result(future, 5.seconds)

      result.size shouldEqual 1

      result.head shouldEqual DocumentAggregate(DocId,
        LocalDateTime.ofInstant(Hour, ZoneOffset.UTC),
        10,
        UserIds.toSet,
        45.0,
        45)
    }

    it("should aggregate records in the same hour, for multiple documents") {
      val flows = new Flows(Duration.ofHours(1),
        Duration.ofMinutes(5),
        2,
        2,
        2)

      val CombinedSummaries = (Summaries1 ++ Summaries2).sortBy(_.createdAt)

      val flow = flows.createAggregateFlow(Flow[EventSummary])
      val future = Source(CombinedSummaries).via(flow).runWith(Sink.seq)

      val result = Await.result(future, 5.seconds).map(agg => agg.documentId -> agg).toMap

      result.size shouldEqual 2

      result(DocId) shouldEqual DocumentAggregate(DocId,
        LocalDateTime.ofInstant(Hour, ZoneOffset.UTC),
        10,
        UserIds.toSet,
        45.0,
        45)

      result(DocId2) shouldEqual DocumentAggregate(DocId2,
        LocalDateTime.ofInstant(Hour, ZoneOffset.UTC),
        10,
        UserIds.toSet,
        90.0,
        90)
    }

    it("should aggregate records in multiple hours, for the same document") {
      val flows = new Flows(Duration.ofHours(1),
        Duration.ofMinutes(5),
        2,
        2,
        2)

      val CombinedSummaries = (Summaries1 ++ Summaries3).sortBy(_.createdAt)

      val flow = flows.createAggregateFlow(Flow[EventSummary])
      val future = Source(CombinedSummaries).via(flow).runWith(Sink.seq)

      val result = Await.result(future, 5.seconds).sortBy(_.epoch.toInstant(ZoneOffset.UTC))

      result.size shouldEqual 2

      result.head shouldEqual DocumentAggregate(DocId,
        LocalDateTime.ofInstant(Hour, ZoneOffset.UTC),
        10,
        UserIds.toSet,
        45.0,
        45)

      result(1) shouldEqual DocumentAggregate(DocId,
        LocalDateTime.ofInstant(Hour.plus(1, ChronoUnit.HOURS), ZoneOffset.UTC),
        10,
        UserIds.toSet,
        90.0,
        90)

    }


  }

  describe("The combined flow") {

    it("should aggregate interleaved sessions") {

      val tOrigin = T0.truncatedTo(ChronoUnit.HOURS)

      val doc1 = UUID.randomUUID()
      val doc2 = UUID.randomUUID()

      val session1 = createSession(doc1, tOrigin, 1)
      val session2 = createSession(doc1, tOrigin.plus(40, ChronoUnit.MINUTES), 2)
      val session3 = createSession(doc2, tOrigin.plus(65, ChronoUnit.MINUTES), 3)
      val session4 = createSession(doc1, tOrigin.plus(70, ChronoUnit.MINUTES), 4)

      val all = (session1 ::: session2 ::: session3 ::: session4 ::: FlushTail).sortBy(_.timestamp)

      val flows = new Flows(Duration.ofHours(1),
        Duration.ofMinutes(5),
        8,
        8,
        8)

      val sumFlow = flows.createSummaryFlow(Flow[VisitEvent])

      val flow = flows.createAggregateFlow(sumFlow)

      val future = Source(all).via(flow).runWith(Sink.seq)

      val result = Await.result(future, 5.seconds)

      result.size shouldEqual 3

    }

  }

}

object FlowsTest {

  val FlushTail: List[Flush.type] = Flush :: Nil

  val SessionId1: UUID = UUID.randomUUID()

  val SessionId2 : UUID = UUID.randomUUID()

  val T0: Instant = Instant.now()

  val Create1 = VisitCreate(SessionId1, UUID.randomUUID(), UUID.randomUUID(), T0)

  val Updates1: List[VisitUpdate] = for (i <- (1 to 3).toList)
    yield VisitUpdate(SessionId1, i, i.toDouble, T0.plus(i, ChronoUnit.SECONDS))

  val Messages1: List[VisitEvent] = Create1 :: Updates1

  val Create2 = VisitCreate(SessionId2, UUID.randomUUID(), UUID.randomUUID(),
    T0.plus(500, ChronoUnit.MILLIS))

  val Updates2: List[VisitUpdate] = for (i <- (1 to 3).toList)
    yield VisitUpdate(SessionId2, 2 * i, 2 * i.toDouble, T0.plus(i * 1000 + 500, ChronoUnit.MILLIS))

  val Interleaved: List[VisitEvent] = (Create1 :: Create2 :: (Updates1 ::: Updates2)).sortBy(_.timestamp)

  val UserIds: IndexedSeq[UUID] = IndexedSeq.tabulate(4)(_ => UUID.randomUUID())

  val Hour: Instant = Instant.now().truncatedTo(ChronoUnit.HOURS)
  val DocId: UUID = UUID.randomUUID()
  val DocId2: UUID = UUID.randomUUID()

  val Summaries1: immutable.IndexedSeq[EventSummary] = {

    for (i <- 0 until 10) yield {
      EventSummary(UUID.randomUUID(),
        UserIds(i % UserIds.length),
        DocId,
        Hour.plus(i * 5, ChronoUnit.MINUTES),
        Hour.plus(i * 15, ChronoUnit.MINUTES),
        i, i.toDouble)
    }

  }

  val Summaries2: immutable.IndexedSeq[EventSummary] = {

    for (i <- 0 until 10) yield {
      EventSummary(UUID.randomUUID(),
        UserIds(i % UserIds.length),
        DocId2,
        Hour.plus(i * 5 + 2, ChronoUnit.MINUTES),
        Hour.plus(i * 15 + 2, ChronoUnit.MINUTES),
        2 * i, 2 * i.toDouble)
    }

  }

  val Summaries3: immutable.IndexedSeq[EventSummary] = {

    for (i <- 0 until 10) yield {
      EventSummary(UUID.randomUUID(),
        UserIds(i % UserIds.length),
        DocId,
        Hour.plus(1, ChronoUnit.HOURS).plus(i * 5, ChronoUnit.MINUTES),
        Hour.plus(1, ChronoUnit.HOURS).plus(i * 15, ChronoUnit.MINUTES),
        2 * i,  2 * i.toDouble)
    }

  }

  /**
    * Create a fake session.
    * @param docId The ID of the document.
    * @param createdAt The initial creation time of the session.
    * @param n Integer to generate the fields.
    * @return The session records.
    */
  def createSession(docId : UUID, createdAt : Instant, n : Int) : List[VisitEvent] = {
    val id = UUID.randomUUID()
    val create = VisitCreate(id, UUID.randomUUID(), docId, createdAt)
    val updates = for (i <- (1 to 4).toList)
      yield VisitUpdate(id, n * i, n * i.toDouble, createdAt.plus(10 * i, ChronoUnit.MINUTES))
    create :: updates
  }
}
