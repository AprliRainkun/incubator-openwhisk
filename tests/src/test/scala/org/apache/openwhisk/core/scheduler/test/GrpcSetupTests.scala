package org.apache.openwhisk.core.scheduler.test

import com.google.protobuf.ByteString
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpecLike}
import org.scalatest.OptionValues._
import akka.actor.ActorSystem
import akka.grpc.GrpcClientSettings
import akka.stream.{ActorMaterializer, Materializer}
import akka.stream.scaladsl.{Sink, Source}
import akka.testkit.{TestKit, TestProbe}
import org.apache.openwhisk.grpc._
import org.apache.openwhisk.core.scheduler._

import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._

@RunWith(classOf[JUnitRunner])
class GrpcSetupTests
    extends TestKit(ActorSystem("GrpcSetupTest"))
    with WordSpecLike
    with Matchers
    with BeforeAndAfterAll {
  val local = "127.0.0.1"
  val port = 8899

  implicit val sys: ActorSystem = system
  implicit val mat: Materializer = ActorMaterializer()
  implicit val ec: ExecutionContext = system.dispatcher

  override def beforeAll(): Unit = {
    new QueueServiceServer(new QueueServiceDraftImpl()).run(local, port)
  }

  override def afterAll(): Unit = {
    system.terminate()
  }

  "Queue service server" should {
    "expose its fetch method" in {
      val windows = (0 to 3).map(_ => WindowAdvertisement("ns/pkg/act", 1))
      val activations = client.fetch(Source(windows))
      val probe = TestProbe()

      activations.to(Sink.actorRef(probe.ref, "completed")).run()

      0 to 3 foreach { _ =>
        val resp = probe.expectMsgType[FetchResponse](3.seconds)
        resp.status.map(_.statusCode) should be(Option(200))
      }
    }

    "expose its put method" in {
      val tid = TransactionId("#tid_000")
      val act = Activation(Option(tid), "ns/pkg/act", ByteString.EMPTY)

      val resp = Await.result(client.put(act), 3.seconds)

      resp.status.value.statusCode should be(200)
    }

    "expose its create method" in {
      val tid = TransactionId("#tid_000")
      val act = CreateQueueRequest(Option(tid), "ns/pkg/act")

      val resp = Await.result(client.create(act), 3.seconds)

      resp.status.value.statusCode should be(200)
    }
  }

  private def client = {
    val config = GrpcClientSettings.connectToServiceAt(local, port).withTls(false)
    QueueServiceClient(config)
  }
}
