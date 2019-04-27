package org.apache.openwhisk.core.scheduler.test

import akka.actor.ActorSystem
import com.google.protobuf.ByteString
import org.apache.openwhisk.grpc._
import org.apache.openwhisk.grpc.etcd.RangeRequest
import org.scalatest.OptionValues._

class QueueCreationTests extends TestBase("QueueCreationTests") with LocalScheduler {
  implicit val sys: ActorSystem = system

  lazy val local = "127.0.0.1"

  override def etcdHost: String = local
  override def etcdPort = 2379
  override def schedulerPort = 8989

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
}
