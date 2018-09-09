package com.permutive.analytics.components

import akka.stream.scaladsl.{Flow, GraphDSL, Partition, RunnableGraph, Sink, Source}
import akka.stream.{ClosedShape, Materializer}

import scala.collection.immutable

object Connectors {

  /**
    * Partitions a stream of [[Either]] records and delegates each type to its own sink.
    * @param source The source of either records.
    * @param sink1 The sink for the first type.
    * @param sink2 The sink for the second type.
    * @param combine Combine the results of the sinks.
    * @param materializer Stream materializer.
    * @tparam S The first type.
    * @tparam T The second type.
    * @return The combined result.
    */
  def partitionAndRun[S, T, M1, M2, MOut](source : Source[Either[S, T], _],
                            sink1 : Sink[S, M1],
                            sink2 : Sink[T, M2], combine : (M1, M2) => MOut)(implicit materializer: Materializer) : MOut = {
    RunnableGraph.fromGraph(GraphDSL.create(sink1, sink2)(combine){ implicit builder => (out1, out2) =>
      import GraphDSL.Implicits._

      //Filter out only the left records.
      val left = builder.add(Flow[Either[S, T]].mapConcat {
        case Left(l) => immutable.Iterable(l)
        case _ => immutable.Iterable.empty
      })
      //Filter out only the right records.
      val right = builder.add(Flow[Either[S, T]].mapConcat{

        case Right(r) => immutable.Iterable(r)
        case _ => immutable.Iterable.empty
      })
      //Partition the input stream.
      val partition = builder.add(Partition[Either[S, T]](2, {
        case Left(_) => 0
        case Right(_) => 1
      }))

      //Feed the source in the partitioner.
      source ~> partition.in

      //Feed the partitions into their sinks.
      partition.out(0) ~> left ~> out1
      partition.out(1) ~> right ~> out2

      ClosedShape
    }).run()
  }

}
