package org.apache.openwhisk.core.invoker.test

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl.{Flow, Source}
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestKit
import org.apache.openwhisk.core.containerpool._
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}
import scala.language.postfixOps

@RunWith(classOf[JUnitRunner])
class SideChannelBackpressureTests
    extends TestKit(ActorSystem("SideChannelBackpressureTests"))
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll {

  implicit val ex: ExecutionContext = system.dispatcher
  implicit val mat: Materializer = ActorMaterializer()

  override def afterAll(): Unit = system.terminate()

  "Side channel backpressure source" should {
    "work like a normal identity map" in {
      // map each tick to an integer 1
      val (senderFut, data) = Source
        .fromGraph(new ConflatedTickerStage)
        .throttle(1, 50 millis)
        .mapConcat(List.fill(_)(1))
        .preMaterialize()

      val sideChannel = Await.result(senderFut map { s =>
        Flow.fromGraph(new SideChannelBackpressureStage[Int](s))
      }, 1 second)

      // inject an async boundary
      val recv = data.via(sideChannel).async.runWith(TestSink.probe)

      val totalIter = 10
      (1 to totalIter) foreach {i =>
        recv.request(i)
        recv.expectNextN(List.fill(i)(1))
        recv.expectNoMessage(100 millis)
      }
    }
  }
}
