package org.apache.openwhisk.core.invoker.test

import akka.actor.ActorRef
import akka.event.Logging
import akka.testkit.TestProbe
import org.apache.openwhisk.common.{AkkaLogging, Logging}
import org.apache.openwhisk.core.containerpool.MessageBroker
import org.apache.openwhisk.core.scheduler.test.{LocalScheduler, TestBase}
import org.apache.openwhisk.grpc._
import org.scalatest.OptionValues._

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.language.postfixOps

class MessageBrokerTests extends TestBase("MessageBrokerTests") with LocalScheduler {

  lazy val local = "127.0.0.1"

  override def etcdHost: String = local
  override def etcdPort = 2379
  override def schedulerPort: Int = 8956

  implicit val log: Logging = new AkkaLogging(Logging.getLogger(sys, this))

  "Message broker" should {
    "not produce more message than demanded" in {
      val actionName = "messageBroker/test/action000"
      val broker = system.actorOf(MessageBroker.props(schedulerClient, actionName, 5))

      runBatchFetch(broker, actionName, 500 millis)
      succeed
    }

    "behave correctly when the buffer is disabled" in {
      val actionName = "messageBroker/test/action111"
      val broker = system.actorOf(MessageBroker.props(schedulerClient, actionName, 0))

      runBatchFetch(broker, actionName, 500 millis)
      succeed
    }

    "receive ordered messages" in {
      val actionName = "messageBroker/test/action222"
      val broker = system.actorOf(MessageBroker.props(schedulerClient, actionName, 5))
      val putNum = (1 to 10).sum

      val putsFut = for {
        _ <- createQueue(actionName)
        puts <- (1 to putNum).foldLeft(Future.successful(List.empty[String])) {
          case (prevF, i) =>
            for {
              p <- prevF
              body = s"msg-$i"
              _ <- schedulerClient.put(Activation(actionName, body))
            } yield body :: p
        }
      } yield puts.reverse

      putsFut map { puts =>
        val probe = TestProbe()
        val recv = (1 to 10) flatMap { n =>
          probe.send(broker, MessageBroker.NeedMessage(n))
          (1 to n) map { _ =>
            val m = probe.expectMsgType[MessageBroker.NewMessage]
            m.msg
          }
        } toList

        recv should equal(puts)
      }
    }
  }

  private def runBatchFetch(broker: ActorRef, actionName: String, noMsgWait: FiniteDuration): FiniteDuration = {
    val putNum = (1 to 10).sum

    val putFut = for {
      resp <- createQueue(actionName)
      _ = resp.status.value.statusCode should be(200)
      _ <- seedNPuts(actionName, putNum)
    } yield ()
    Await.result(putFut, 5 seconds)

    val probe = TestProbe()
    val start = System.currentTimeMillis()

    (1 to 10) foreach { n =>
      probe.send(broker, MessageBroker.NeedMessage(n))
      (1 to n) foreach { _ =>
        probe.expectMsgType[MessageBroker.NewMessage]
      }
      probe.expectNoMessage(noMsgWait)
    }

    (System.currentTimeMillis() - start) millis
  }

  private def createQueue(actionName: String): Future[CreateQueueResponse] = {
    val tid = TransactionId("tid_000")
    val req = CreateQueueRequest(Some(tid), actionName)
    schedulerClient.create(req)
  }

  private def seedNPuts(actionName: String, num: Int): Future[Unit] = {
    val seed = (1 to num).toList
    Future.traverse(seed) { i =>
      schedulerClient.put(Activation(actionName)) map { resp =>
        resp.status.value.statusCode should be(200)
      }
    } map (_ => ())
  }
}
