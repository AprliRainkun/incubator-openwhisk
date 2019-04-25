package org.apache.openwhisk.core.scheduler.test

import akka.actor.ActorSystem
import akka.grpc.GrpcClientSettings
import com.google.protobuf.ByteString
import org.apache.openwhisk.core.database.etcd._
import org.apache.openwhisk.core.scheduler._
import org.apache.openwhisk.grpc._
import org.apache.openwhisk.grpc.etcd.RangeRequest
import org.junit.runner.RunWith
import org.scalatest.OptionValues._
import org.scalatest.junit.JUnitRunner

import scala.concurrent.Await
import scala.concurrent.duration._

@RunWith(classOf[JUnitRunner])
class QueueCreationTests extends TestBase("QueueCreationTests") {
  implicit val sys: ActorSystem = system

  val local = "127.0.0.1"
  val etcdPort = 2379
  val schedulerPort = 8989

  override val etcdClientSettings: GrpcClientSettings =
    GrpcClientSettings.connectToServiceAt(local, etcdPort).withTls(false)

  val hostname = "scheduler0.test.localhost"

  "Queue creation service" should {
    "be reachable" in {
      val tid = TransactionId("#tid_000")
      val req = CreateQueueRequest(Some(tid), "ns/pkg/act")

      schedulerClient.create(req) map { resp =>
        resp.status.value.statusCode should be(200)
        resp.endpoint should be(hostname)
      }
    }

    "create a queue and write endpoint to etcd" in {
      val actionName = "ns/pkg/act2"
      val tid = TransactionId("#tid_000")
      val req = CreateQueueRequest(Some(tid), actionName)

      schedulerClient.create(req) flatMap { resp =>
        resp.status.value.statusCode should be(200)

        val req = RangeRequest(key = ByteString.copyFromUtf8(s"queue/$actionName/endpoint"))
        kvClient.range(req)
      } map { resp =>
        resp.kvs.head.value should be(ByteString.copyFromUtf8(hostname))
      }
    }
  }

  override def beforeAll(): Unit = {
    implicit val etcdSettings: GrpcClientSettings =
      GrpcClientSettings.connectToServiceAt(local, etcdPort).withTls(false)
    val storeConfig = QueueMetadataStoreConfig(entityPrefix + "queue/%s/marker", entityPrefix + "queue/%s/endpoint")
    implicit val schedulerConfig: SchedulerConfig = SchedulerConfig(hostname, storeConfig)

    val manager = system.actorOf(QueueManager.props(etcdSettings, schedulerConfig))

    val srv = new QueueServiceServer(new RpcEndpoint(manager)).run(local, schedulerPort)
    Await.result(srv, 5.seconds)
  }

  private def schedulerClient = {
    val config = GrpcClientSettings.connectToServiceAt(local, schedulerPort).withTls(false)
    QueueServiceClient(config)
  }
}
