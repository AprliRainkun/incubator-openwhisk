package org.apache.openwhisk.core.scheduler.test

import akka.stream.scaladsl.{Keep, Sink, Source}
import com.google.protobuf.ByteString
import org.apache.openwhisk.grpc.WindowAdvertisement.Message
import org.apache.openwhisk.grpc._
import org.scalatest.OptionValues._

import scala.concurrent.Future

class PutAndFetchTests extends TestBase("PutAndFetchTests") with LocalScheduler {
  override def etcdHost = "127.0.0.1"
  override def etcdPort = 2379
  override def schedulerPort = 8989

  "Put and fetch" should {
    "work when first put some then fetch all" in {
      val actionName = "ns/pkg/act222"
      val tid = TransactionId("#tid_000")
      val req = CreateQueueRequest(Some(tid), actionName)

      val seed = List(0 to 2)
      for {
        resp <- schedulerClient.create(req)
        _ = resp.status.value.statusCode should be(200)
        _ <- Future.traverse(seed)(_ => schedulerClient.put(Activation(Some(tid), actionName, ByteString.EMPTY)))
        source = Source(List(Message.ActionName(actionName), Message.WindowsSize(3))) map (WindowAdvertisement(_))
        acts <- schedulerClient.fetch(source).toMat(Sink.seq)(Keep.right).run()
      } yield {
        acts.size should be(seed.size)
        acts.foreach { a =>
          a.status.value.statusCode should be(200)
        }
        succeed
      }
    }
  }
}
