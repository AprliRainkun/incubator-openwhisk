package org.apache.openwhisk.core.scheduler.test

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.OptionValues._
import org.scalatest.{AsyncWordSpecLike, BeforeAndAfterAll, Matchers}
import akka.testkit.TestKit
import akka.actor.ActorSystem
import akka.grpc.GrpcClientSettings
import akka.stream.{ActorMaterializer, Materializer}
import com.google.protobuf.ByteString
import org.apache.openwhisk.core.scheduler._
import org.apache.openwhisk.grpc._
import org.apache.openwhisk.grpc.etcd.{KVClient, RangeRequest}

import scala.concurrent.Await
import scala.concurrent.duration._

@RunWith(classOf[JUnitRunner])
class QueueCreationTests
    extends TestKit(ActorSystem("QueueCreationTests"))
    with AsyncWordSpecLike
    with Matchers
    with BeforeAndAfterAll {
  implicit val sys: ActorSystem = system
  implicit val mat: Materializer = ActorMaterializer()

  val local = "127.0.0.1"
  val etcdPort = 2379
  val schedulerPort = 8989

  val hostname = "scheduler0.test.localhost"

  "Queue creation service" should {
    "be reachable" in {
      val tid = TransactionId("#tid_000")
      val req = CreateQueueRequest(Option(tid), "ns/pkg/act")

      schedulerClient.create(req) map { resp =>
        resp.status.value.statusCode should be(200)
        resp.endpoint should be(hostname)
      }
    }

    "create a queue and write endpoint to etcd" in {
      val actionName = "ns/pkg/act2"
      val tid = TransactionId("#tid_000")
      val req = CreateQueueRequest(Option(tid), actionName)

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
    implicit val schedulerConfig: SchedulerConfig = SchedulerConfig(hostname)

    val manager = system.actorOf(QueueManager.props(etcdSettings, schedulerConfig))

    val srv = new QueueServiceServer(new RPCEndpoint(manager)).run(local, schedulerPort)
    Await.result(srv, Duration.Inf)
  }

  private def schedulerClient = {
    val config = GrpcClientSettings.connectToServiceAt(local, schedulerPort).withTls(false)
    QueueServiceClient(config)
  }

  private def kvClient = {
    val config = GrpcClientSettings.connectToServiceAt(local, etcdPort).withTls(false)
    KVClient(config)
  }
}
