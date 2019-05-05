package org.apache.openwhisk.core.scheduler.test

import akka.stream.scaladsl.{Sink, Source}
import akka.stream.testkit.scaladsl.TestSink
import com.google.protobuf.ByteString
import org.apache.openwhisk.grpc.WindowAdvertisement.Message
import org.apache.openwhisk.grpc._
import org.apache.openwhisk.grpc.etcd.RangeRequest
import org.scalatest.OptionValues._

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.language.postfixOps

class QueueServiceTests extends TestBase("QueueCreationTests") with LocalScheduler {

  lazy val local = "127.0.0.1"

  override def etcdHost: String = local
  override def etcdPort = 2379
  override def schedulerPort = 8990

  "Queue creation service" should {
    "be reachable" in {
      val tid = TransactionId("#tid_000")
      val req = CreateQueueRequest(Some(tid), "ns/pkg/act")

      schedulerClient.create(req) map { resp =>
        resp.status.value.statusCode should be(200)
        resp.endpoint should be(local)
      }
    }

    "create a queue and write endpoint to etcd" in {
      val actionName = "ns/pkg/act2"
      val tid = TransactionId("#tid_000")
      val req = CreateQueueRequest(Some(tid), actionName)

      schedulerClient.create(req) flatMap { resp =>
        resp.status.value.statusCode should be(200)

        val req = RangeRequest(key = ByteString.copyFromUtf8(storeConfig.endpointKeyTemplate.format(actionName)))
        kvClient.range(req)
      } map { resp =>
        resp.kvs.head.value should be(ByteString.copyFromUtf8(local))
      }
    }
  }

  "Put and fetch" should {
    "work when first put some then fetch all" in {
      val actionName = "ns/pkg/act222"
      val putNumber = 3

      val putFut = for {
        resp <- createQueue(actionName)
        _ = resp.status.value.statusCode should be(200)
        _ <- seedNPuts(actionName, putNumber)
      } yield ()
      Await.result(putFut, 5.seconds)

      val (sender, source) = Utils.feedAndSource[WindowAdvertisement]
      val recv = schedulerClient.fetch(source).runWith(TestSink.probe)

      List(Message.ActionName(actionName), Message.WindowsSize(putNumber))
        .map(WindowAdvertisement(_))
        .foreach(sender.sendNext)

      recv.request(putNumber)
      (1 to putNumber) foreach { _ =>
        val resp = recv.expectNext(2.seconds)
        resp.status.value.statusCode should be(200)
        resp.activation.value.actionName should be(actionName)
      }

      sender.sendComplete()
      recv.expectComplete()

      succeed
    }

    "serve concurrent fetchers" in {
      val actionName = "ns/pkg/act23234"
      val putNumberUnit = 10
      val fetcherNumber = 3

      val createFut = createQueue(actionName) map { resp =>
        resp.status.value.statusCode should be(200)
      }
      Await.result(createFut, 3.seconds)

      val fetchAsserts = (1 to fetcherNumber) map { _ =>
        val windows = (Message.ActionName(actionName) :: (1 to 4).map(Message.WindowsSize).toList) map { m =>
          WindowAdvertisement(m)
        }
        schedulerClient
          .fetch(Source(windows))
          .map(_ => 1)
          .runWith(Sink.fold(0)(_ + _))
          .map(n => n should be(10))
      } toList

      val putFut = seedNPuts(actionName, putNumberUnit * fetcherNumber)

      val combinedFut = for {
        _ <- Future.traverse(fetchAsserts)(identity)
        _ <- putFut
      } yield ()
      Await.result(combinedFut, 20.seconds)

      succeed
    }

    "not reply with any message when the queue is drained" in {
      val actionName = "ns/pkg/act333"
      val putNumber = 3

      val putFut = for {
        resp <- createQueue(actionName)
        _ = resp.status.value.statusCode should be(200)
        _ <- seedNPuts(actionName, putNumber)
      } yield ()
      Await.result(putFut, 3.seconds)

      val (sender, source) = Utils.feedAndSource[WindowAdvertisement]
      val recv = schedulerClient.fetch(source).runWith(TestSink.probe)

      // drain all puts
      List(Message.ActionName(actionName), Message.WindowsSize(putNumber))
        .map(WindowAdvertisement(_))
        .foreach(sender.sendNext)
      recv.request(putNumber)
      recv.expectNextN(putNumber)

      // should not reply because the queue is empty
      sender.sendNext(WindowAdvertisement(Message.WindowsSize(1)))
      recv.request(1)
      recv.expectNoMessage(3.seconds)

      // put more activation
      val additional = 2
      val newPut = seedNPuts(actionName, additional)
      Await.result(newPut, 3.seconds)

      // now we should be able to receive
      sender.sendNext(WindowAdvertisement(Message.WindowsSize(additional)))
      recv.request(additional)
      recv.expectNextN(additional)

      sender.sendComplete()
      recv.expectComplete()

      succeed
    }

    "not send more activations than requested" in {
      val actionName = "ns/pkg/act444"
      val putNum = 10

      val putFut = for {
        resp <- createQueue(actionName)
        _ = resp.status.value.statusCode should be(200)
        _ <- seedNPuts(actionName, putNum)
      } yield ()
      Await.result(putFut, 3.seconds)

      val (sender, source) = Utils.feedAndSource[WindowAdvertisement]
      val recv = schedulerClient.fetch(source).runWith(TestSink.probe)

      sender.sendNext(WindowAdvertisement(Message.ActionName(actionName)))
      recv.request(10)

      (1 to 4) foreach { n =>
        sender.sendNext(WindowAdvertisement(Message.WindowsSize(n)))
        recv.expectNextN(n)
        // the window should be depleted, no activation should be received
        recv.expectNoMessage(3.seconds)
      }

      sender.sendComplete()
      recv.expectComplete()

      succeed
    }

    "not loss activation when fetcher cancelled" in {
      val actionName = "ns/pkg/act555"
      val putNum = 10

      val putFut = for {
        resp <- createQueue(actionName)
        _ = resp.status.value.statusCode should be(200)
        _ <- seedNPuts(actionName, putNum)
      } yield ()
      Await.result(putFut, 3.seconds)

      (1 to 4) foreach { n =>
        val (sender, source) = Utils.feedAndSource[WindowAdvertisement]
        val recv = schedulerClient.fetch(source).runWith(TestSink.probe)

        List(Message.ActionName(actionName), Message.WindowsSize(n))
          .map(WindowAdvertisement(_))
          .foreach(sender.sendNext)

        recv.request(2 * n)
        recv.expectNextN(n)
        recv.expectNoMessage(3.seconds)

        sender.sendComplete()
        recv.expectComplete()
      }

      succeed
    }
  }

  private def createQueue(actionName: String): Future[CreateQueueResponse] = {
    val tid = TransactionId("#tid_000")
    val req = CreateQueueRequest(Some(tid), actionName)
    schedulerClient.create(req)
  }

  private def seedNPuts(actionName: String, num: Int): Future[Unit] = {
    val seed = (1 to num).toList
    Future.traverse(seed)(_ => schedulerClient.put(Activation(actionName))) map (_ => ())
  }
}
