package org.apache.openwhisk.core.invoker.test

import akka.NotUsed
import akka.actor.{ActorRef, ActorSystem, Props}
import akka.event.Logging
import akka.stream.DelayOverflowStrategy
import akka.stream.scaladsl.{Sink, Source}
import akka.testkit.TestProbe
import org.apache.openwhisk.common.{AkkaLogging, Logging, TransactionId => WhiskTid}
import org.apache.openwhisk.core.containerpool._
import org.apache.openwhisk.core.database.etcd.QueueMetadataStore
import org.apache.openwhisk.core.entity.{DocInfo, QueueRegistration}
import org.apache.openwhisk.core.scheduler.test.{LocalScheduler, TestBase}
import org.apache.openwhisk.grpc._
import org.scalatest.OptionValues._
import spray.json._

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.language.postfixOps

object SlowNetworkMessageBroker {
  def props(action: DocInfo, bufferLimit: Int, queueMetadataStore: QueueMetadataStore)(implicit sys: ActorSystem,
                                                                                       logging: Logging) =
    Props(new SlowNetworkMessageBroker(action, bufferLimit, queueMetadataStore))
}

// simulate 40 millis RTT latency
class SlowNetworkMessageBroker(action: DocInfo, bufferLimit: Int, queueMetadataStore: QueueMetadataStore)(
  implicit sys: ActorSystem,
  logging: Logging)
    extends MessageBroker(action, bufferLimit, queueMetadataStore) {

  override def establishFetchFlow(): Future[(TickerSendEnd, Source[String, NotUsed])] = {
    import org.apache.openwhisk.grpc.WindowAdvertisement.Message
    import org.apache.openwhisk.grpc._

    queueMetadataStore.getEndPoint(action) flatMap {
      case QueueRegistration(host, port) =>
        val (sendFuture, sizes) = Source
          .fromGraph(new ConflatedTickerStage)
          .throttle(1, 20 millis)
          .map(b => WindowAdvertisement(Message.WindowsSize(b)))
          .preMaterialize()
        val actionId = ActionIdentifier(action.id.asString, action.rev.asString)
        val source = Source(List(WindowAdvertisement(Message.Action(actionId))))
          .concat(sizes)
          .delay(20 millis, DelayOverflowStrategy.backpressure)

        val client = schedulerClientPool.getClient(host, port)
        val activations = client
          .fetch(source)
          .delay(20 millis, DelayOverflowStrategy.backpressure)
          .map(_.activation.head.body)

        sendFuture map { send =>
          (send, activations)
        }
    }
  }
}

class MessageBrokerTests extends TestBase("MessageBrokerTests") with LocalScheduler {

  lazy val local = "127.0.0.1"

  override def etcdHost: String = local
  override def etcdPort = 2379
  override def schedulerPort: Int = 8956

  implicit val log: Logging = new AkkaLogging(Logging.getLogger(sys, this))
  val rpcTid = TransactionId(WhiskTid.invokerNanny.toJson.compactPrint)

  "Message broker" should {
    "not produce more message than demanded" in {
      val action = createActionInfo("messageBroker/test/action000")
      createQueueAndAssert(action)

      val broker = system.actorOf(MessageBroker.props(action, 5, queueMetadataStore))

      runBatchFetch(broker, action, 500 millis)
      succeed
    }

    "behave correctly when the buffer is disabled" in {
      val action = createActionInfo("messageBroker/test/action111")
      createQueueAndAssert(action)

      val broker = system.actorOf(MessageBroker.props(action, 5, queueMetadataStore))

      runBatchFetch(broker, action, 500 millis)
      succeed
    }

    "receive ordered messages" in {
      val action = createActionInfo("messageBroker/test/action222")
      createQueueAndAssert(action)

      val broker = system.actorOf(MessageBroker.props(action, 5, queueMetadataStore))
      val putNum = (1 to 10).sum

      val putsFut = (1 to putNum)
        .foldLeft(Future.successful(List.empty[String])) {
          case (prevF, i) =>
            for {
              p <- prevF
              body = s"msg-$i"
              actionId = ActionIdentifier(action.id.asString, action.rev.asString)
              _ <- schedulerClient.put(Activation(Some(rpcTid), Some(actionId), body))
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

      val actionA = createActionInfo("messageBroker/test/action3331")
      createQueueAndAssert(actionA)
      val brokerWithBuffer =
        system.actorOf(SlowNetworkMessageBroker.props(actionA, 5, queueMetadataStore))
      val bufferFut = timingForMessages(brokerWithBuffer, actionA, putNum)

      val actionB = createActionInfo("messageBroker/test/action3332")
      createQueueAndAssert(actionB)
      val brokerWOBuffer = system.actorOf(SlowNetworkMessageBroker.props(actionB, 0, queueMetadataStore))
      val noBufferFut = timingForMessages(brokerWOBuffer, actionB, putNum)

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

  private def createActionInfo(name: String): DocInfo = DocInfo ! (name, "1")

  private def timingForMessages(broker: ActorRef, action: DocInfo, total: Int): Future[FiniteDuration] = {
    val probe = TestProbe()

    for {
      resp <- createQueue(action)
      _ = resp.status.value.statusCode should be(200)
      _ <- seedNPuts(action, total)
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

  private def runBatchFetch(broker: ActorRef, action: DocInfo, noMsgWait: FiniteDuration): FiniteDuration = {
    val putNum = (1 to 10).sum

    val putFut = for {
      _ <- seedNPuts(action, putNum)
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

  private def createQueueAndAssert(action: DocInfo): Unit = {
    val fut = createQueue(action)
    val resp = Await.result(fut, 3 seconds)
    resp.status.value.statusCode should be(200)
  }

  private def createQueue(action: DocInfo): Future[CreateQueueResponse] = {
    val tid = TransactionId("tid_000")
    val actionId = ActionIdentifier(action.id.asString, action.rev.asString)
    val req = CreateQueueRequest(Some(tid), Some(actionId))
    schedulerClient.create(req)
  }

  private def seedNPuts(action: DocInfo, num: Int): Future[Unit] = {
    val actionId = ActionIdentifier(action.id.asString, action.rev.asString)
    val seed = (1 to num).toList
    Future.traverse(seed) { i =>
      schedulerClient.put(Activation(Some(rpcTid), Some(actionId))) map { resp =>
        resp.status.value.statusCode should be(200)
      }
    } map (_ => ())
  }
}
