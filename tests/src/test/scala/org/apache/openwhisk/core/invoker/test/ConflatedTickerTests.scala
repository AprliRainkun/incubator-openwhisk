package org.apache.openwhisk.core.invoker.test

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.testkit.scaladsl.TestSink
import akka.testkit.TestKit
import org.apache.openwhisk.core.containerpool.ConflatedTickerStage
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}

@RunWith(classOf[JUnitRunner])
class ConflatedTickerTests
    extends TestKit(ActorSystem("ConflatedTickerTests"))
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll {
  implicit val ex: ExecutionContext = system.dispatcher
  implicit val mat: Materializer = ActorMaterializer()

  override def afterAll(): Unit = system.terminate()

  "Conflated ticker" should {
    "produce correct aggregated tick number with a slow sender" in {
      val totalRounds = 500

      val (senderFut, acc) = Source
        .fromGraph(new ConflatedTickerStage)
        .throttle(1, 200.millis)
        .toMat(Sink.fold(0)(_ + _))(Keep.both)
        .run()

      val done = for {
        sender <- senderFut
        _ <- Source(1 to totalRounds)
          .throttle(1, 20.millis)
          .runForeach { x =>
            sender.send(x)
          }
      } yield sender.complete()
      Await.result(done, 20.seconds)

      val observed = Await.result(acc, 1.second)
      observed should be((1 to totalRounds).sum)
    }

    "produce correct aggregated tick number with a fast sender" in {
      val totalTicks = 100000

      val (senderFut, acc) = Source
        .fromGraph(new ConflatedTickerStage)
        .throttle(5, 1.second)
        .toMat(Sink.fold(0)(_ + _))(Keep.both)
        .run()

      val done = for {
        sender <- senderFut
        _ <- Source(1 to totalTicks)
          .throttle(50000, 1.second)
          .runForeach { _ =>
            sender.send(1)
          }
      } yield sender.complete()
      Await.result(done, 5.seconds)

      val observed = Await.result(acc, 1.second)
      observed should be(totalTicks)
    }

    "not loss ticks" in {
      val (senderFut, accRecv) = Source
        .fromGraph(new ConflatedTickerStage)
        .toMat(TestSink.probe)(Keep.both)
        .run()

      // first, send 10 ticks
      val sender = Await.result(senderFut, 1.second)
      (1 to 10) foreach { _ =>
        sender.send(1)
      }

      accRecv.request(1)
      val a1 = accRecv.expectNext(20.millis)
      a1 should be(10)

      // then, send another 10 ticks and complete
      (1 to 10) foreach { _ =>
        sender.send(1)
      }
      sender.complete()

      // receiver should still be able to get the number
      // after the sender completes
      accRecv.request(1)
      val a2 = accRecv.expectNext(20.millis)
      a2 should be(10)

      accRecv.expectComplete()
    }
  }
}
