package org.apache.openwhisk.core.invoker.test

import akka.actor.ActorRef
import akka.event.Logging
import akka.testkit.TestProbe
import org.apache.openwhisk.common.{AkkaLogging, Logging}
import org.apache.openwhisk.core.containerpool.MessageBroker
import org.apache.openwhisk.core.scheduler.test.{LocalScheduler, TestBase}
import org.apache.openwhisk.grpc._
import org.scalatest.OptionValues._

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.language.postfixOps

class MessageBrokerTests extends TestBase("MessageBrokerTests") with LocalScheduler {

  lazy val local = "127.0.0.1"

  override def etcdHost: String = local
  override def etcdPort = 2379
  override def schedulerPort: Int = 8993

  implicit val log: Logging = new AkkaLogging(Logging.getLogger(sys, this))

  "Message broker" should {
    "not produce more message than demanded" in {
      val actionName = "messageBroker/test/action000"
      val broker = system.actorOf(MessageBroker.props(schedulerClient, actionName, 5))

      runBatchFetch(broker, actionName, 500 millis) map { _ =>
        succeed
      }
    }

    "behave correctly when the buffer is disabled" in {
      val actionName = "messageBroker/test/action111"
      val broker = system.actorOf(MessageBroker.props(schedulerClient, actionName, 0))

      runBatchFetch(broker, actionName, 500 millis) map { _ =>
        succeed
      }
    }
  }

  private def runBatchFetch(broker: ActorRef, actionName: String, noMsgWait: FiniteDuration): Future[FiniteDuration] = {
    val start = System.currentTimeMillis()

    val putNum = (1 to 10).sum

    for {
      resp <- createQueue(actionName)
      _ = resp.status.value.statusCode should be(200)
      _ <- seedNPuts(actionName, putNum)
    } yield {
      val probe = TestProbe()

      (1 to 10) foreach { n =>
        probe.send(broker, MessageBroker.NeedMessage(n))
        (1 to n) foreach { _ =>
          probe.expectMsgType[MessageBroker.NewMessage]
        }
        println(s"$n messages asserted")
        probe.expectNoMessage(noMsgWait)
      }

      (System.currentTimeMillis() - start) millis
    }
  }

  private def createQueue(actionName: String): Future[CreateQueueResponse] = {
    val tid = TransactionId("tid_000")
    val req = CreateQueueRequest(Some(tid), actionName)
    schedulerClient.create(req)
  }

  private def seedNPuts(actionName: String, num: Int): Future[Unit] = {
    val seed = (1 to num).toList
    Future.traverse(seed) { _ =>
      schedulerClient.put(Activation(actionName))
    } map (_ => ())
  }
}
