package org.apache.openwhisk.core.scheduler.test

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.testkit.TestPublisher
import akka.stream.testkit.scaladsl.TestSource

object Utils {
  def feedAndSource[T](implicit mat: Materializer, sys: ActorSystem): (TestPublisher.Probe[T], Source[T, NotUsed]) = {
    val (source, sink) = Source
      .asSubscriber[T]
      .toMat(Sink.asPublisher(false))(Keep.both)
      .mapMaterializedValue {
        case (sub, pub) => (Source.fromPublisher(pub), Sink.fromSubscriber(sub))
      }
      .run()
    val feed = TestSource
      .probe[T]
      .toMat(sink)(Keep.left)
      .run()
    (feed, source)
  }
}
