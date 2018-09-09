package com.permutive.analytics

import java.io.File
import java.time.Duration

import akka.Done
import akka.actor.ActorSystem
import akka.event.Logging
import akka.stream.scaladsl.{FileIO, Flow, Framing, Keep, Sink, Source}
import akka.stream.{ActorMaterializer, Attributes}
import akka.util.ByteString
import com.permutive.analytics.components.{Connectors, Flows}
import com.permutive.analytics.model.{Flush, VisitEvent}
import scopt.OptionParser

import scala.concurrent.{ExecutionContextExecutor, Future}

/**
  * Application to aggregate [[VisitEvent]]s, stored as Json documents in a file, and output
  * the results to the console.
  * @param conf The application configuration.
  */
class Main(conf : Main.Config) extends Runnable {

  import Main._

  override def run(): Unit = {

    implicit val system : ActorSystem = ActorSystem(SysName)
    implicit val materializer : ActorMaterializer = ActorMaterializer()

    //Read the lines from the provided file.
    val fileSource = FileIO.fromPath(conf.inputFile.toPath)
        .via(Framing.delimiter(ByteString("\n"), MaxMessageSize, allowTruncation = true)
        .map(_.utf8String))

    println(Header)
    println(Bar)

    //Attempt to parse them and append a flush record.
    val messages = fileSource.map(VisitEvent.parse).concat(Source(List(Left(Flush))))

    val flows = new Flows(Duration.ofMinutes(conf.expiryMinutes),
      Duration.ofMinutes(conf.watermarkLagMinutes),
      conf.maxSessions,
      conf.maxTimeWindows,
      conf.maxDocsPerWindow)

    val aggregationFlow = flows.createAggregateFlow(flows.createSummaryFlow(Flow[VisitEvent]))
      .toMat(Sink.foreach {
        aggregate =>
          println(aggregate.toLine)
      })(Keep.right)

    val logFlow = Flow[List[String]].withAttributes(Attributes.logLevels(onElement = Logging.ErrorLevel))
      .log("Parse Errors")
      .toMat(Sink.ignore)(Keep.right)

    val combine = (a : Future[Done], b : Future[Done]) => List(a, b)

    //Assemble the flow graph and execute it.
    val result = Connectors.partitionAndRun(messages, aggregationFlow, logFlow, combine)

    implicit val ec: ExecutionContextExecutor = system.dispatcher
    Future.sequence(result).onComplete(_ => system.terminate())

  }
}

object Main {

  /**
    * Column headers for the output.
    */
  val Header = "documentId, startTime, endTime, numVisits, uniqueVisits, totalEngaged, totalCompleted"
  val Bar: String = "-" * Header.length

  /**
    * Name of the actor system.
    */
  final val SysName = "CodingChallengeSystem"

  /**
    * Maximum size of a single document.
    */
  final val MaxMessageSize = 1024

  def main(args: Array[String]): Unit = {
    CmdLineParser.parse(args, Config()) match {
      case Some(config) => new Main(config).run()
      case _ =>
    }

  }

  /**
    * Default maximum size for states.
    */
  final val DefaultMax = 1024 * 1024

  /**
    * Configuration for the application.
    * @param inputFile The file to read.
    * @param maxSessions Maximum number of sessions to process at one time.
    * @param maxTimeWindows Maximum number of time windows to process at one time.
    * @param expiryMinutes Expiry time for sessions in minutes.
    * @param watermarkLagMinutes Watermark lag for late arrival in minutes.
    * @param maxDocsPerWindow Maximum number of simultaneous documents per time window.
    */
  case class Config(inputFile : File = new File("."),
                    maxSessions : Int = DefaultMax,
                    maxTimeWindows : Int = 4,
                    expiryMinutes : Int = 60,
                    watermarkLagMinutes : Int = 5,
                    maxDocsPerWindow : Int = DefaultMax)

  val CmdLineParser: OptionParser[Config] = new OptionParser[Config]("coding-challenge") {
    head("coding-challenge", "1.0")

    opt[File]('i', "input").action( (f, c) =>
      c.copy(inputFile = f) ).text("File from which to read the messages.")
      .required()

    opt[Int]("maxSessions").action( (n, c) =>
      c.copy(maxSessions = n) ).text("Maximum number of simultaneously open sessions.")

    opt[Int]("maxTimeWindows").action( (n, c) =>
      c.copy(maxTimeWindows = n) ).text("Maximum number of simultaneously open time windows.")

    opt[Int]("maxDocs").action( (n, c) =>
      c.copy(maxDocsPerWindow = n) ).text("Maximum number of documents per time window simultaneously open.")

    opt[Int]("sessionExpiry").action( (n, c) =>
      c.copy(expiryMinutes = n) ).text("Time for a session to expire in minutes.")

    opt[Int]("watermarkLag").action( (n, c) =>
      c.copy(watermarkLagMinutes= n) ).text("Lag for the watermark for closing windows in minutes.")
  }
}
