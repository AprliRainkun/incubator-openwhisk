package org.apache.openwhisk.core.invoker.test

import akka.NotUsed
import akka.actor.{ActorRef, Props}
import akka.event.Logging
import akka.stream.DelayOverflowStrategy
import akka.stream.scaladsl.{Sink, Source}
import akka.testkit.TestProbe
import org.apache.openwhisk.common.{AkkaLogging, Logging}
import org.apache.openwhisk.core.containerpool._
import org.apache.openwhisk.core.scheduler.test.{LocalScheduler, TestBase}
import org.apache.openwhisk.grpc._
import org.scalatest.OptionValues._

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.language.postfixOps

object SlowNetworkMessageBroker {
  def props(client: QueueServiceClient, action: String, bufferLimit: Int)(implicit logging: Logging) =
    Props(new SlowNetworkMessageBroker(client, action, bufferLimit))
}

// simulate 40 millis RTT latency
class SlowNetworkMessageBroker(client: QueueServiceClient, action: String, bufferLimit: Int)(implicit logging: Logging)
    extends MessageBroker(client, action, bufferLimit) {

  override def establishFetchFlow(): Future[(TickerSendEnd, Source[String, NotUsed])] = {
    import org.apache.openwhisk.grpc.WindowAdvertisement.Message
    import org.apache.openwhisk.grpc._

    val (sendFuture, batchSizes) = Source
      .fromGraph(new ConflatedTickerStage)
      .throttle(1, 20 millis)
      .map(b => WindowAdvertisement(Message.WindowsSize(b)))
      .preMaterialize()

    val windows = Source(List(WindowAdvertisement(Message.ActionName(action))))
      .concat(batchSizes)
      .delay(20 millis, DelayOverflowStrategy.backpressure)

    val activations = client
      .fetch(windows)
      .delay(20 millis, DelayOverflowStrategy.backpressure)
      .map(w => w.activation.head.body)

    sendFuture map { send =>
      (send, activations)
    }
  }
}

class MessageBrokerTests extends TestBase("MessageBrokerTests") with LocalScheduler {

  lazy val local = "127.0.0.1"

  override def etcdHost: String = local
  override def etcdPort = 2379
  override def schedulerPort: Int = 8956

  implicit val log: Logging = new AkkaLogging(Logging.getLogger(sys, this))

  "Message broker" should {
    "not produce more message than demanded" in {
      val actionName = "messageBroker/test/action000"
      createQueueAndAssert(actionName)

      val broker = system.actorOf(MessageBroker.props(schedulerClient, actionName, 5))

      runBatchFetch(broker, actionName, 500 millis)
      succeed
    }

    "behave correctly when the buffer is disabled" in {
      val actionName = "messageBroker/test/action111"
      createQueueAndAssert(actionName)

      val broker = system.actorOf(MessageBroker.props(schedulerClient, actionName, 0))

      runBatchFetch(broker, actionName, 500 millis)
      succeed
    }

    "receive ordered messages" in {
      val actionName = "messageBroker/test/action222"
      createQueueAndAssert(actionName)

      val broker = system.actorOf(MessageBroker.props(schedulerClient, actionName, 5))
      val putNum = (1 to 10).sum

      val putsFut = (1 to putNum)
        .foldLeft(Future.successful(List.empty[String])) {
          case (prevF, i) =>
            for {
              p <- prevF
              body = s"msg-$i"
              _ <- schedulerClient.put(Activation(actionName, body))
            } yield body :: p
        }
        .map(_.reverse)

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

    "show improved throughput with buffer in a slow network" in {
      val putNum = 500

      val nameA = "messageBroker/test/action3331"
      createQueueAndAssert(nameA)
      val brokerWithBuffer =
        system.actorOf(SlowNetworkMessageBroker.props(schedulerClient, nameA, 5))
      val bufferFut = timingForMessages(brokerWithBuffer, nameA, putNum)

      val nameB = "messageBroker/test/action3332"
      createQueueAndAssert(nameB)
      val brokerWOBuffer = system.actorOf(SlowNetworkMessageBroker.props(schedulerClient, nameB, 0))
      val noBufferFut = timingForMessages(brokerWOBuffer, nameB, putNum)

      for {
        bufferTime <- bufferFut
        noBufferTime <- noBufferFut
      } yield {
        val ratio = noBufferTime / bufferTime
        println(s"time with buffer: ${bufferTime.toMillis}, time w/o buffer: ${noBufferTime.toMillis}, ratio: $ratio")
        ratio should be >= 2.0
      }
    }
  }

  private def timingForMessages(broker: ActorRef, actionName: String, total: Int): Future[FiniteDuration] = {
    val probe = TestProbe()

    for {
      resp <- createQueue(actionName)
      _ = resp.status.value.statusCode should be(200)
      _ <- seedNPuts(actionName, total)
      start = System.nanoTime()
      _ <- Source(1 to total).mapAsyncUnordered(2) { _ =>
        Future {
          probe.send(broker, MessageBroker.NeedMessage(1))
          probe.expectMsgType[MessageBroker.NewMessage]
          Thread.sleep(20)
        }
      } runWith Sink.ignore
    } yield (System.nanoTime() - start).nanos
  }

  private def runBatchFetch(broker: ActorRef, actionName: String, noMsgWait: FiniteDuration): FiniteDuration = {
    val putNum = (1 to 10).sum

    val putFut = for {
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

  private def createQueueAndAssert(action: String): Unit = {
    val fut = createQueue(action)
    val resp = Await.result(fut, 3 seconds)
    resp.status.value.statusCode should be(200)
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
